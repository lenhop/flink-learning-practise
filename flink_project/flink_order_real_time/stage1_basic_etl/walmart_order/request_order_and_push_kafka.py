import os
import sys
import json
import logging
from datetime import datetime, timedelta
import pytz

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
# Use flink_project root directory for logs
log_dir = os.path.join(flink_project_path, 'logs')
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
    
    def _parse_date_input(self, date_input):
        """
        Parse date input and convert to API format (UTC)
        
        Args:
            date_input (str): Date string in format:
                - "yyyy-MM-dd" -> Convert to "yyyy-MM-ddT00:00:00Z" or "yyyy-MM-ddT23:59:59Z"
                - "yyyy-MM-dd hh:mm:ss" -> Convert to "yyyy-MM-ddThh:mm:ssZ"
        
        Returns:
            tuple: (datetime_obj, is_full_day)
                - datetime_obj: datetime object in UTC
                - is_full_day: True if input is date only, False if datetime
        """
        try:
            # Try to parse as date only (yyyy-MM-dd)
            try:
                dt = datetime.strptime(date_input, "%Y-%m-%d")
                is_full_day = True
            except ValueError:
                # Try to parse as datetime (yyyy-MM-dd hh:mm:ss)
                try:
                    dt = datetime.strptime(date_input, "%Y-%m-%d %H:%M:%S")
                    is_full_day = False
                except ValueError:
                    # Try to parse as datetime with timezone (yyyy-MM-ddTHH:MM:SSZ)
                    try:
                        dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
                        is_full_day = False
                    except ValueError:
                        raise ValueError(f"Invalid date format: {date_input}. Expected 'yyyy-MM-dd' or 'yyyy-MM-dd hh:mm:ss' or 'yyyy-MM-ddTHH:MM:SSZ'")
            
            # Convert to UTC if not already
            if dt.tzinfo is None:
                # Assume local timezone, convert to UTC
                dt = pytz.UTC.localize(dt)
            else:
                dt = dt.astimezone(pytz.UTC)
            
            return dt, is_full_day
        except Exception as e:
            raise ValueError(f"Error parsing date input '{date_input}': {str(e)}")
    
    def _split_into_hourly_chunks(self, start_dt, end_dt):
        """
        Split time range into hourly chunks
        
        Args:
            start_dt (datetime): Start datetime in UTC
            end_dt (datetime): End datetime in UTC
        
        Returns:
            list: List of (start_datetime, end_datetime) tuples in UTC
        """
        chunks = []
        current_start = start_dt
        
        while current_start < end_dt:
            # Calculate end of current hour
            current_end = current_start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            
            # If current_end exceeds end_dt, use end_dt
            if current_end > end_dt:
                current_end = end_dt
            
            chunks.append((current_start, current_end))
            
            # Move to next hour
            current_start = current_end
        
        return chunks
    
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
                                limit=200):
        """
        Request orders for a time range and push to Kafka
        Supports two input formats:
        - "yyyy-MM-dd": Request full day (00:00:00 to 23:59:59)
        - "yyyy-MM-dd hh:mm:ss": Request specific time range
        - "yyyy-MM-ddTHH:MM:SSZ": API format (UTC)
        
        Time range will be split into hourly chunks for processing.
        If time range is less than 1 hour, use original time range.
        
        Args:
            start_date (str): Start date string in format:
                - "yyyy-MM-dd" -> Full day from 00:00:00
                - "yyyy-MM-dd hh:mm:ss" -> Specific start time
                - "yyyy-MM-ddTHH:MM:SSZ" -> API format (UTC)
            end_date (str): End date string in format:
                - "yyyy-MM-dd" -> Full day to 23:59:59
                - "yyyy-MM-dd hh:mm:ss" -> Specific end time
                - "yyyy-MM-ddTHH:MM:SSZ" -> API format (UTC)
            ship_node_type (str, optional): Ship node type, values: SellerFulfilled, WFSFulfilled, 3PLFulfilled
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
            
            # Parse input dates
            start_dt, start_is_full_day = self._parse_date_input(start_date)
            end_dt, end_is_full_day = self._parse_date_input(end_date)
            
            # If input is date only (yyyy-MM-dd), set to full day
            if start_is_full_day:
                start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            if end_is_full_day:
                end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Validate time range
            if start_dt >= end_dt:
                error_msg = f"Invalid time range: start_date ({start_dt}) must be before end_date ({end_dt})"
                logger.error(error_msg)
                stats['errors'].append(error_msg)
                return stats
            
            logger.info("=" * 100)
            logger.info(f"Requesting orders from {start_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info("=" * 100)
            
            # Split into hourly chunks
            hourly_chunks = self._split_into_hourly_chunks(start_dt, end_dt)
            logger.info(f"Time range split into {len(hourly_chunks)} hourly chunk(s)")
            
            # Get access token
            access_token = self._get_access_token()
            
            # Process each hourly chunk
            all_order_list = []
            all_order_details = {}
            overall_status = "SUCCESS"
            
            for chunk_idx, (chunk_start, chunk_end) in enumerate(hourly_chunks, 1):
                chunk_start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                duration_minutes = (chunk_end - chunk_start).total_seconds() / 60
                logger.info(f"Processing chunk {chunk_idx}/{len(hourly_chunks)}: {chunk_start.strftime('%Y-%m-%d %H:%M:%S')} to {chunk_end.strftime('%Y-%m-%d %H:%M:%S')} ({duration_minutes:.1f} minutes)")
                
                try:
                    # Request orders for this chunk
                    chunk_orders, chunk_details, status = self.order_requester.get_orders_for_time_range(
                        access_token=access_token,
                        start_date=chunk_start_str,
                        end_date=chunk_end_str,
                        ship_node_type=ship_node_type,
                        limit=limit
                    )
                    
                    # Handle token expiration
                    if status == "TOKEN_EXPIRED":
                        logger.warning("Access token expired, refreshing...")
                        access_token = self._get_access_token(force_refresh=True)
                        
                        # Retry request
                        chunk_orders, chunk_details, status = self.order_requester.get_orders_for_time_range(
                            access_token=access_token,
                            start_date=chunk_start_str,
                            end_date=chunk_end_str,
                            ship_node_type=ship_node_type,
                            limit=limit
                        )
                    
                    # Check status
                    if status == "TOKEN_EXPIRED":
                        error_msg = "Access token expired and refresh failed"
                        logger.error(error_msg)
                        stats['errors'].append(error_msg)
                        overall_status = "ERROR"
                        break
                    elif status.startswith("ERROR"):
                        error_msg = f"Failed to request orders for chunk {chunk_idx}: {status}"
                        logger.error(error_msg)
                        stats['errors'].append(error_msg)
                        if overall_status == "SUCCESS":
                            overall_status = status
                        continue
                    elif status == "NEED_SPLIT":
                        # If chunk needs split, it means too many orders
                        # For now, we'll log a warning and continue
                        logger.warning(f"Chunk {chunk_idx} has too many orders (NEED_SPLIT), but continuing with available data")
                        # Still merge available orders
                        for order in chunk_orders:
                            order_id = order.get('purchaseOrderId')
                            if order_id and order_id not in all_order_details:
                                all_order_list.append(order)
                                all_order_details[order_id] = order
                        continue
                    elif status == "SUCCESS":
                        # Merge chunk orders
                        for order in chunk_orders:
                            order_id = order.get('purchaseOrderId')
                            if order_id and order_id not in all_order_details:
                                all_order_list.append(order)
                                all_order_details[order_id] = order
                        
                        # Merge order details
                        for order_id, order_detail in chunk_details.items():
                            if order_id not in all_order_details:
                                all_order_details[order_id] = order_detail
                        
                        logger.info(f"  ✓ Chunk {chunk_idx} completed: {len(chunk_orders)} orders")
                
                except Exception as e:
                    error_msg = f"Error processing chunk {chunk_idx}: {str(e)}"
                    logger.error(error_msg)
                    stats['errors'].append(error_msg)
                    if overall_status == "SUCCESS":
                        overall_status = f"ERROR: {str(e)}"
                    continue
            
            # Check overall status
            if overall_status.startswith("ERROR"):
                error_msg = f"Failed to request orders: {overall_status}"
                logger.error(error_msg)
                stats['errors'].append(error_msg)
                # Still try to push available orders
            
            stats['orders_requested'] = len(all_order_list)
            logger.info(f"Total orders requested: {len(all_order_list)}")
            
            # Push to Kafka
            if all_order_list:
                orders_pushed = self._push_orders_to_kafka(all_order_list)
                stats['orders_pushed'] = orders_pushed
                logger.info(f"Total orders pushed to Kafka: {orders_pushed}")
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
            date_str (str): Date string in format "YYYY-MM-DD" or "YYYY-MM-DD hh:mm:ss"
                - "YYYY-MM-DD": Request full day (00:00:00 to 23:59:59)
                - "YYYY-MM-DD hh:mm:ss": Request from specified time to end of day (23:59:59)
            ship_node_type (str, optional): Ship node type
            
        Returns:
            dict: Statistics about the operation
        """
        # If input is date only, request full day
        # If input is datetime, use it as start time and set end to 23:59:59
        try:
            dt, is_full_day = self._parse_date_input(date_str)
            if is_full_day:
                # Full day: use date_str as-is for both start and end
                start_date = date_str
                end_date = date_str
            else:
                # Datetime: use as start, set end to same day 23:59:59
                start_date = date_str
                end_date = dt.strftime("%Y-%m-%d 23:59:59")
        except ValueError:
            # Fallback: assume date only format
            start_date = date_str
            end_date = date_str
        
        return self.request_and_push_orders(
            start_date=start_date,
            end_date=end_date,
            ship_node_type=ship_node_type
        )
    
    def request_and_push_date_range(self, start_date_str, end_date_str, 
                                     ship_node_type=None):
        """
        Request orders for a date range and push to Kafka
        
        Args:
            start_date_str (str): Start date string in format "YYYY-MM-DD" or "YYYY-MM-DD hh:mm:ss"
            end_date_str (str): End date string in format "YYYY-MM-DD" or "YYYY-MM-DD hh:mm:ss"
            ship_node_type (str, optional): Ship node type
            
        Returns:
            dict: Overall statistics
        """
        # Parse input dates to determine if we need to process day by day
        start_dt, start_is_full_day = self._parse_date_input(start_date_str)
        end_dt, end_is_full_day = self._parse_date_input(end_date_str)
        
        # If both are full days, process day by day
        # Otherwise, process as single time range
        if start_is_full_day and end_is_full_day:
            # Process day by day (original behavior)
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
        else:
            # Process as single time range (will be split into hourly chunks)
            return self.request_and_push_orders(
                start_date=start_date_str,
                end_date=end_date_str,
                ship_node_type=ship_node_type
            )
    
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
        # Get orders from last 1 hour in Los Angeles timezone
        # Set timezone to America/Los_Angeles (Pacific Time)
        la_timezone = pytz.timezone('America/Los_Angeles')
        
        # Get current time in Los Angeles timezone
        la_now = datetime.now(la_timezone)
        
        # Calculate 1 hour ago
        la_1_hour_ago = la_now - timedelta(hours=1)
        
        # Convert to UTC for Walmart API (API expects UTC time in ISO format)
        # Walmart API uses UTC timezone, so we need to convert LA time to UTC
        la_now_utc = la_now.astimezone(pytz.UTC)
        la_1_hour_ago_utc = la_1_hour_ago.astimezone(pytz.UTC)
        
        # Format for Walmart API: "YYYY-MM-DDTHH:MM:SSZ"
        start_date = la_1_hour_ago_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date = la_now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info("=" * 100)
        logger.info("Requesting orders from last 1 hour")
        logger.info(f"Los Angeles Time: {la_1_hour_ago.strftime('%Y-%m-%d %H:%M:%S %Z')} to {la_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"UTC Time (API format): {start_date} to {end_date}")
        logger.info("=" * 100)
        
        # Request and push orders
        stats = pusher.request_and_push_orders(
            start_date=start_date,
            end_date=end_date,
            ship_node_type=None,  # Request all types
            limit=200
        )
        
        logger.info("=" * 100)
        logger.info("Processing completed!")
        logger.info(f"Orders requested: {stats.get('orders_requested', 0)}")
        logger.info(f"Orders pushed: {stats.get('orders_pushed', 0)}")
        if stats.get('errors'):
            logger.warning(f"Errors: {stats.get('errors')}")
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        pusher.close()

