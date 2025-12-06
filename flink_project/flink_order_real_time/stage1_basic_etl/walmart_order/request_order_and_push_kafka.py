import os
import sys
import json
import logging
from datetime import datetime, timedelta

# Add flink_project directory to path to import config and utils
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

from config.config import BaseConfig

# Import Kafka utils directly to avoid __init__.py import issues
# Add utils directory to path and import directly
utils_dir = os.path.join(flink_project_path, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

# Import directly from files to avoid __init__.py issues
import kafka_admin_utils
import kafka_producer_utils
KafkaAdminUtils = kafka_admin_utils.KafkaAdminUtils
KafkaProducerUtils = kafka_producer_utils.KafkaProducerUtils

# Import from same directory level (stage1_basic_etl)
# Add stage1_basic_etl parent directory to path
stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if stage1_parent_path not in sys.path:
    sys.path.insert(0, stage1_parent_path)

from stage1_basic_etl.token_generator.walmart_access_token_generator import WalmartAccessTokenGenerator
from stage1_basic_etl.walmart_order.request_walmart_order import WalmartOrderRequester

# Configure logger
# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, 'request_order_and_push_kafka.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WalmartOrderKafkaPusher:
    """Class to request Walmart orders and push to Kafka"""
    
    def __init__(self, account_tag='eForCity', kafka_topic='walmart_order_raw', 
                 bootstrap_servers=None, config=None):
        """
        Initialize Walmart Order Kafka Pusher
        
        Args:
            account_tag (str): Account name for token generation (default: 'eForCity')
            kafka_topic (str): Kafka topic name (default: 'walmart_order_raw')
            bootstrap_servers: Kafka broker addresses, can be string or list
            config: Configuration dict (optional, will load from BaseConfig if None)
        """
        self.account_tag = account_tag
        self.kafka_topic = kafka_topic
        
        # Load configuration
        if config is None:
            self.config = BaseConfig().cfg
        else:
            self.config = config
        
        # Get Kafka bootstrap servers from config or parameter
        if bootstrap_servers is None:
            kafka_config = self.config.get('kafka', {})
            self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:29092', 'localhost:39092'])
        else:
            self.bootstrap_servers = bootstrap_servers if isinstance(bootstrap_servers, list) else [bootstrap_servers]
        
        # Initialize components
        self.token_generator = WalmartAccessTokenGenerator()
        self.order_requester = WalmartOrderRequester()
        self.kafka_admin = None
        self.kafka_producer = None
        
        # Token cache
        self.access_token = None
        self.token_expires_at = None
    
    def _ensure_kafka_topic(self):
        """Ensure Kafka topic exists, create if not"""
        if self.kafka_admin is None:
            self.kafka_admin = KafkaAdminUtils(bootstrap_servers=self.bootstrap_servers)
        
        # Check if topic exists
        topic_exists = self.kafka_admin.check_topic_exists(self.kafka_topic)
        
        if not topic_exists:
            logger.info(f"Topic '{self.kafka_topic}' does not exist, creating...")
            result = self.kafka_admin.create_topic(
                topic_name=self.kafka_topic,
                num_partitions=3,
                replication_factor=1,
                config={
                    'retention.ms': '604800000',  # 7 days
                    'max.message.bytes': '10485760'  # 10MB (10485760 bytes)
                }
            )
            
            if result.get(self.kafka_topic, {}).get('status') == 'success':
                logger.info(f"Topic '{self.kafka_topic}' created successfully")
            else:
                error_msg = result.get(self.kafka_topic, {}).get('message', 'Unknown error')
                logger.error(f"Failed to create topic '{self.kafka_topic}': {error_msg}")
                raise Exception(f"Failed to create Kafka topic: {error_msg}")
        else:
            logger.info(f"Topic '{self.kafka_topic}' already exists")
    
    def _get_access_token(self, force_refresh=False):
        """Get access token, refresh if expired or forced"""
        current_time = datetime.now()
        
        # Check if token is still valid (with 5 minute buffer)
        if not force_refresh and self.access_token and self.token_expires_at:
            if current_time < self.token_expires_at - timedelta(minutes=5):
                logger.debug("Using cached access token")
                return self.access_token
        
        # Generate new token
        logger.info(f"Generating new access token for account: {self.account_tag}")
        try:
            token_result = self.token_generator.generate_access_token(self.account_tag)
            token_data = token_result.get(self.account_tag, {})
            
            self.access_token = token_data.get('access_token')
            expires_in = token_data.get('expires_in', 900)  # Default 15 minutes
            
            # Calculate expiration time
            self.token_expires_at = current_time + timedelta(seconds=expires_in)
            
            logger.info(f"Access token generated successfully, expires in {expires_in} seconds")
            return self.access_token
            
        except Exception as e:
            logger.error(f"Failed to generate access token: {e}")
            raise
    
    def _push_orders_to_kafka(self, orders, batch_size=20):
        """Push orders to Kafka topic in batches
        
        Args:
            orders: List or dict of orders to push
            batch_size: Number of orders to send in each batch (default: 20)
        """
        if not orders:
            logger.warning("No orders to push to Kafka")
            return 0
        
        if self.kafka_producer is None:
            self.kafka_producer = KafkaProducerUtils(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.kafka_topic
            )
            # Configure producer with larger max_request_size to handle large messages
            self.kafka_producer.connect_kafka(
                max_request_size=10485760,  # 10MB
                request_timeout_ms=60000  # 60 seconds
            )
        
        # Convert orders to list of dicts if needed
        if isinstance(orders, dict):
            # If it's a dict of order_id -> order_detail, convert to list
            orders = list(orders.values())
        
        if not isinstance(orders, list):
            orders = [orders]
        
        # Push orders to Kafka in batches
        logger.info(f"Pushing {len(orders)} orders to Kafka topic '{self.kafka_topic}' in batches of {batch_size}...")
        
        total_pushed = 0
        total_failed = 0
        num_batches = (len(orders) + batch_size - 1) // batch_size
        
        try:
            # Process orders in batches
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(orders))
                batch = orders[start_idx:end_idx]
                
                logger.info(f"Pushing batch {batch_idx + 1}/{num_batches} ({len(batch)} orders)...")
                
                try:
                    # Use key_field to use purchaseOrderId as Kafka message key
                    self.kafka_producer.send_messages_sync_wait(
                        messages=batch,
                        key_field='purchaseOrderId',
                        flush_count=len(batch),  # Flush after each batch
                        close_after_send=False
                    )
                    total_pushed += len(batch)
                    logger.info(f"Batch {batch_idx + 1}/{num_batches} pushed successfully ({len(batch)} orders)")
                    
                except Exception as e:
                    total_failed += len(batch)
                    logger.error(f"Failed to push batch {batch_idx + 1}/{num_batches}: {e}")
                    # Continue with next batch
                    continue
            
            logger.info(f"Successfully pushed {total_pushed}/{len(orders)} orders to Kafka")
            if total_failed > 0:
                logger.warning(f"Failed to push {total_failed} orders")
            
            return total_pushed
            
        except Exception as e:
            logger.error(f"Failed to push orders to Kafka: {e}")
            raise
    
    def request_and_push_orders(self, start_date, end_date, ship_node_type=None, 
                                auto_split=True, limit=200):
        """
        Request orders for a time range and push to Kafka
        
        Args:
            start_date (str): Start date string in format "YYYY-MM-DDTHH:MM:SSZ"
            end_date (str): End date string in format "YYYY-MM-DDTHH:MM:SSZ"
            ship_node_type (str, optional): Ship node type, values: SellerFulfilled, WFSFulfilled, 3PLFulfilled
            auto_split (bool): Whether to use auto-split for large time ranges (default: True)
            limit (int): Maximum number of orders per request (default: 200)
            
        Returns:
            dict: Statistics about the operation
        """
        stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'orders_requested': 0,
            'orders_pushed': 0,
            'errors': []
        }
        
        try:
            # Ensure Kafka topic exists
            self._ensure_kafka_topic()
            
            # Get access token
            access_token = self._get_access_token()
            
            # Request orders
            logger.info(f"Requesting orders from {start_date} to {end_date}")
            
            if auto_split:
                order_list, order_details, status = self.order_requester.get_orders_with_auto_split(
                    access_token=access_token,
                    start_date=start_date,
                    end_date=end_date,
                    ship_node_type=ship_node_type,
                    limit=limit
                )
            else:
                order_list, order_details, status = self.order_requester.get_orders_for_time_range(
                    access_token=access_token,
                    start_date=start_date,
                    end_date=end_date,
                    ship_node_type=ship_node_type,
                    limit=limit
                )
            
            # Handle token expiration
            if status == "TOKEN_EXPIRED":
                logger.warning("Access token expired, refreshing...")
                access_token = self._get_access_token(force_refresh=True)
                
                # Retry request
                if auto_split:
                    order_list, order_details, status = self.order_requester.get_orders_with_auto_split(
                        access_token=access_token,
                        start_date=start_date,
                        end_date=end_date,
                        ship_node_type=ship_node_type,
                        limit=limit
                    )
                else:
                    order_list, order_details, status = self.order_requester.get_orders_for_time_range(
                        access_token=access_token,
                        start_date=start_date,
                        end_date=end_date,
                        ship_node_type=ship_node_type,
                        limit=limit
                    )
            
            # Check status
            if status.startswith("ERROR"):
                error_msg = f"Failed to request orders: {status}"
                logger.error(error_msg)
                stats['errors'].append(error_msg)
                return stats
            
            stats['orders_requested'] = len(order_list)
            logger.info(f"Successfully requested {len(order_list)} orders")
            
            # Push to Kafka
            if order_list:
                orders_pushed = self._push_orders_to_kafka(order_list)
                stats['orders_pushed'] = orders_pushed
            else:
                logger.warning("No orders to push to Kafka")
            
        except Exception as e:
            error_msg = f"Error in request_and_push_orders: {str(e)}"
            logger.error(error_msg)
            stats['errors'].append(error_msg)
            raise
        
        finally:
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
        
        return stats
    
    def request_and_push_daily_orders(self, date_str, ship_node_type=None):
        """
        Request orders for a single day and push to Kafka
        
        Args:
            date_str (str): Date string in format "YYYY-MM-DD"
            ship_node_type (str, optional): Ship node type
            
        Returns:
            dict: Statistics about the operation
        """
        start_date = f"{date_str}T00:00:00Z"
        end_date = f"{date_str}T23:59:59Z"
        
        return self.request_and_push_orders(
            start_date=start_date,
            end_date=end_date,
            ship_node_type=ship_node_type,
            auto_split=True
        )
    
    def request_and_push_date_range(self, start_date_str, end_date_str, 
                                     ship_node_type=None):
        """
        Request orders for a date range and push to Kafka
        
        Args:
            start_date_str (str): Start date string in format "YYYY-MM-DD"
            end_date_str (str): End date string in format "YYYY-MM-DD"
            ship_node_type (str, optional): Ship node type
            
        Returns:
            dict: Overall statistics
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        overall_stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'total_orders_requested': 0,
            'total_orders_pushed': 0,
            'days_processed': 0,
            'days_failed': 0,
            'errors': []
        }
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            logger.info("=" * 100)
            logger.info(f"Processing date: {date_str}")
            logger.info("=" * 100)
            
            try:
                stats = self.request_and_push_daily_orders(date_str, ship_node_type)
                overall_stats['total_orders_requested'] += stats.get('orders_requested', 0)
                overall_stats['total_orders_pushed'] += stats.get('orders_pushed', 0)
                overall_stats['days_processed'] += 1
                
                if stats.get('errors'):
                    overall_stats['errors'].extend(stats['errors'])
                    overall_stats['days_failed'] += 1
                
            except Exception as e:
                error_msg = f"Failed to process date {date_str}: {str(e)}"
                logger.error(error_msg)
                overall_stats['errors'].append(error_msg)
                overall_stats['days_failed'] += 1
            
            current_date += timedelta(days=1)
        
        overall_stats['end_time'] = datetime.now()
        overall_stats['duration'] = (overall_stats['end_time'] - overall_stats['start_time']).total_seconds()
        
        logger.info("=" * 100)
        logger.info("Date range processing completed!")
        logger.info(f"Date range: {start_date_str} to {end_date_str}")
        logger.info(f"Total orders requested: {overall_stats['total_orders_requested']}")
        logger.info(f"Total orders pushed: {overall_stats['total_orders_pushed']}")
        logger.info(f"Days processed: {overall_stats['days_processed']}")
        logger.info(f"Days failed: {overall_stats['days_failed']}")
        logger.info(f"Duration: {overall_stats['duration']:.2f} seconds")
        logger.info("=" * 100)
        
        return overall_stats
    
    def close(self):
        """Close Kafka connections"""
        if self.kafka_producer:
            try:
                self.kafka_producer.close()
                self.kafka_producer = None
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")


# Main execution
if __name__ == "__main__":
    logger.info("Walmart Order Request and Push to Kafka" + " -" * 100)
    
    # Initialize pusher
    pusher = WalmartOrderKafkaPusher(
        account_tag='eForCity',
        kafka_topic='walmart_order_raw',
        bootstrap_servers=['localhost:29092', 'localhost:39092']
    )
    
    try:
        # Example: Process single day
        # date_str = "2025-10-03"
        # stats = pusher.request_and_push_daily_orders(date_str)
        # logger.info(f"Processing result: {stats}")
        
        # Example: Process date range
        start_date_str = "2025-10-01"
        end_date_str = "2025-10-01"
        overall_stats = pusher.request_and_push_date_range(start_date_str, end_date_str)
        logger.info(f"Overall processing result: {overall_stats}")
        
    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        pusher.close()

