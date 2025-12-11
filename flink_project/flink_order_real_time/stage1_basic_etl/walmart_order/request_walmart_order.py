import requests
import json
import time
import os
import sys
import logging
from datetime import datetime, timedelta
import pytz

# Add flink_project directory to path
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

# Configure logger
# Create logs directory if it doesn't exist
# Use flink_project root directory for logs
log_dir = os.path.join(flink_project_path, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, 'walmart_order_requester.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WalmartOrderRequester:
    """Singleton class for requesting Walmart orders
    
    This class is responsible only for making order requests to Walmart API.
    Access token should be provided as parameter, not generated internally.
    """
    
    _instance = None
    _initialized = False
    
    BASE_URL = "https://marketplace.walmartapis.com/v3"
    
    def __new__(cls):
        """Singleton pattern implementation"""
        if cls._instance is None:
            cls._instance = super(WalmartOrderRequester, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize if not already initialized"""
        if not WalmartOrderRequester._initialized:
            WalmartOrderRequester._initialized = True
    
    def get_order_details(self, access_token, order_id):
        """Get order details by order ID
        
        Args:
            access_token (str): Walmart access token
            order_id (str): Order ID
            
        Returns:
            dict: Order details data
            
        Raises:
            Exception: If request fails
        """
        url = f"{self.BASE_URL}/orders/{order_id}"
        
        headers = {
            "WM_SEC.ACCESS_TOKEN": access_token,
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_QOS.CORRELATION_ID": f"correlation_id_{int(time.time())}",
            "Accept": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            order_data = response.json()
            # Add request_time to order details
            request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(order_data, dict):
                order_data['request_time'] = request_time
            return order_data
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get order details for {order_id}: {str(e)}")
    
    def _get_orders_for_single_type(self, access_token, start_date, end_date, ship_node_type, limit=200):
        """Request orders for a specific time range and ship node type (internal method)
        
        Args:
            access_token (str): Walmart access token
            start_date (str): Start date string in format "YYYY-MM-DDTHH:MM:SSZ"
            end_date (str): End date string in format "YYYY-MM-DDTHH:MM:SSZ"
            ship_node_type (str): Ship node type, values: SellerFulfilled, WFSFulfilled, 3PLFulfilled
            limit (int): Maximum number of orders to return (default: 200)
            
        Returns:
            tuple: (order_list, order_details_dict, status)
                - order_list: List of order dictionaries
                - order_details_dict: Dictionary mapping order_id to order details
                - status: "SUCCESS", "TOKEN_EXPIRED", "NEED_SPLIT", or "ERROR"
        """
        params = {
            "createdStartDate": start_date,
            "createdEndDate": end_date,
            "limit": limit,
            "acknowledgedStatus": "ALL",
            "sortBy": "CREATED_DATE",
            "sortOrder": "ASC",
            "shipNodeType": ship_node_type
        }
        
        headers = {
            "WM_SEC.ACCESS_TOKEN": access_token,
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_QOS.CORRELATION_ID": f"correlation_id_{int(time.time())}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        url = f"{self.BASE_URL}/orders"
        
        max_retries = 3
        response = None
        for retry in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=60)
                if response.status_code == 200:
                    break
                elif response.status_code == 429:
                    time.sleep(5)
                    continue
                elif response.status_code == 401:
                    return [], {}, "TOKEN_EXPIRED"
                elif response.status_code == 400:
                    error_msg = response.text[:200] if hasattr(response, 'text') else "Bad request"
                    return [], {}, f"ERROR: {error_msg}"
                else:
                    error_msg = response.text[:200] if hasattr(response, 'text') else f"HTTP {response.status_code}"
                    return [], {}, f"ERROR: {error_msg}"
            except requests.exceptions.Timeout:
                if retry == max_retries - 1:
                    return [], {}, "ERROR"
                continue
            except Exception as e:
                return [], {}, "ERROR"
        
        if response is None or response.status_code != 200:
            return [], {}, "ERROR"
        
        try:
            data = response.json()
            if isinstance(data, str):
                data = json.loads(data)
        except json.JSONDecodeError:
            return [], {}, "ERROR"
        
        if 'list' not in data or 'elements' not in data['list']:
            return [], {}, "ERROR"
        
        orders_element = data['list']['elements']
        if isinstance(orders_element, dict) and 'order' in orders_element:
            orders = orders_element['order']
        else:
            orders = orders_element if isinstance(orders_element, list) else []
        
        meta = data['list'].get('meta', {})
        total_count = meta.get('totalCount', 0)
        
        # Get current request time in format yyyy-MM-dd HH:mm:ss
        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        order_list = []
        order_details = {}
        for order in orders:
            if isinstance(order, dict):
                order_id = order.get('purchaseOrderId')
                if order_id:
                    # Add request_time to each order
                    order['request_time'] = request_time
                    order_list.append(order)
                    order_details[order_id] = order
        
        if total_count > limit:
            return [], {}, "NEED_SPLIT"
        
        return order_list, order_details, "SUCCESS"
    
    def get_orders_for_time_range(self, access_token, start_date, end_date, ship_node_type=None, limit=200):
        """Request orders for a specific time range
        
        If ship_node_type is None, will request all 3 types (SellerFulfilled, WFSFulfilled, 3PLFulfilled)
        and merge the results. Otherwise, requests only the specified type.
        
        Args:
            access_token (str): Walmart access token
            start_date (str): Start date string in format "YYYY-MM-DDTHH:MM:SSZ"
            end_date (str): End date string in format "YYYY-MM-DDTHH:MM:SSZ"
            ship_node_type (str, optional): Ship node type, values: SellerFulfilled, WFSFulfilled, 3PLFulfilled
                If None, requests all 3 types and merges results
            limit (int): Maximum number of orders to return per type (default: 200)
            
        Returns:
            tuple: (order_list, order_details_dict, status)
                - order_list: List of order dictionaries (merged and deduplicated)
                - order_details_dict: Dictionary mapping order_id to order details (merged)
                - status: "SUCCESS", "TOKEN_EXPIRED", "NEED_SPLIT", or "ERROR"
                
        Note: If status is "TOKEN_EXPIRED", you need to refresh the access token.
              If status is "NEED_SPLIT", the time range contains more than limit orders for any type.
        """
        ship_node_types = ["SellerFulfilled", "WFSFulfilled", "3PLFulfilled"]
        
        if ship_node_type:
            # Request single type
            return self._get_orders_for_single_type(access_token, start_date, end_date, ship_node_type, limit)
        
        # Request all 3 types and merge results
        all_order_list = []
        all_order_details = {}
        overall_status = "SUCCESS"
        
        for node_type in ship_node_types:
            order_list, order_details, status = self._get_orders_for_single_type(
                access_token, start_date, end_date, node_type, limit
            )
            
            if status == "TOKEN_EXPIRED":
                return [], {}, "TOKEN_EXPIRED"
            elif status == "NEED_SPLIT":
                overall_status = "NEED_SPLIT"
            elif status.startswith("ERROR"):
                if overall_status == "SUCCESS":
                    overall_status = status
            
            # Merge orders, deduplicate by order_id
            for order in order_list:
                order_id = order.get('purchaseOrderId')
                if order_id and order_id not in all_order_details:
                    all_order_list.append(order)
                    all_order_details[order_id] = order
            
            # Merge order details (order_details may have more data than order_list)
            for order_id, order_detail in order_details.items():
                if order_id not in all_order_details:
                    all_order_details[order_id] = order_detail
        
        return all_order_list, all_order_details, overall_status
    
    def _split_time_range(self, start_datetime, end_datetime, split_factor):
        """Split a time range into smaller chunks
        
        Args:
            start_datetime (datetime): Start datetime
            end_datetime (datetime): End datetime
            split_factor (int): Number of chunks to split into
            
        Returns:
            list: List of (start_datetime, end_datetime) tuples
        """
        duration = end_datetime - start_datetime
        chunk_duration = duration / split_factor
        
        chunks = []
        for i in range(split_factor):
            chunk_start = start_datetime + (chunk_duration * i)
            chunk_end = start_datetime + (chunk_duration * (i + 1))
            if i == split_factor - 1:
                chunk_end = end_datetime
            chunks.append((chunk_start, chunk_end))
        
        return chunks
    
    def get_orders_with_auto_split(self, access_token, start_date, end_date, ship_node_type=None, limit=200):
        """Get orders for a time range with automatic time splitting
        
        Splits the time range into hourly chunks. If any hour returns more than limit orders,
        that hour is further split into 6 chunks of 10 minutes each.
        
        Args:
            access_token (str): Walmart access token
            start_date (str): Start date string in format "YYYY-MM-DDTHH:MM:SSZ"
            end_date (str): End date string in format "YYYY-MM-DDTHH:MM:SSZ"
            ship_node_type (str, optional): Ship node type, values: SellerFulfilled, WFSFulfilled, 3PLFulfilled
                If None, requests all 3 types and merges results
            limit (int): Maximum number of orders per request (default: 200)
            
        Returns:
            tuple: (order_list, order_details_dict, status)
                - order_list: List of order dictionaries (merged and deduplicated)
                - order_details_dict: Dictionary mapping order_id to order details (merged)
                - status: "SUCCESS", "TOKEN_EXPIRED", or "ERROR"
        """
        # Parse datetime strings
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        # Calculate total duration in hours
        total_duration = end_dt - start_dt
        total_hours = total_duration.total_seconds() / 3600
        
        # Split into hourly chunks (not fixed 24, but based on actual duration)
        # For each hour, create a chunk
        hourly_chunks = []
        current_start = start_dt
        hour_idx = 0
        
        while current_start < end_dt:
            # Calculate end of current hour
            current_end = current_start + timedelta(hours=1)
            if current_end > end_dt:
                current_end = end_dt
            hourly_chunks.append((current_start, current_end))
            current_start = current_end
            hour_idx += 1
        
        all_order_list = []
        all_order_details = {}
        overall_status = "SUCCESS"
        
        for hour_idx, (hour_start, hour_end) in enumerate(hourly_chunks):
            hour_start_str = hour_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            hour_end_str = hour_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Calculate duration of this hour chunk
            hour_duration = (hour_end - hour_start).total_seconds() / 60  # in minutes
            
            # Progress bar for hourly requests
            progress = f"Processing hour {hour_idx+1}/{len(hourly_chunks)}: {hour_start.strftime('%H:%M')} - {hour_end.strftime('%H:%M')} ({hour_duration:.1f} minutes)"
            logger.info(progress)
            
            # Request orders for this hour
            hour_orders, hour_details, status = self.get_orders_for_time_range(
                access_token, hour_start_str, hour_end_str, ship_node_type, limit
            )
            
            if status == "TOKEN_EXPIRED":
                logger.error("Access token expired.")
                return [], {}, "TOKEN_EXPIRED"
            elif status.startswith("ERROR"):
                if overall_status == "SUCCESS":
                    overall_status = status
                continue
            
            # If status is NEED_SPLIT, split hour into 6 chunks of 10 minutes
            if status == "NEED_SPLIT":
                logger.info(f"Hour {hour_start.strftime('%H:%M')} - {hour_end.strftime('%H:%M')} has more than {limit} orders, splitting into 6 chunks of 10 minutes")
                ten_minute_chunks = self._split_time_range(hour_start, hour_end, 6)
                
                for chunk_idx, (chunk_start, chunk_end) in enumerate(ten_minute_chunks):
                    chunk_start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    chunk_end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Calculate duration of this 10-minute chunk
                    chunk_duration = (chunk_end - chunk_start).total_seconds() / 60
                    
                    # Progress bar for 10-minute chunks
                    chunk_progress = f"  Processing 10-minute chunk {chunk_idx+1}/6: {chunk_start.strftime('%H:%M')} - {chunk_end.strftime('%H:%M')} ({chunk_duration:.1f} minutes)"
                    logger.info(chunk_progress)
                    
                    chunk_orders, chunk_details, chunk_status = self.get_orders_for_time_range(
                        access_token, chunk_start_str, chunk_end_str, ship_node_type, limit
                    )
                    
                    if chunk_status == "TOKEN_EXPIRED":
                        logger.error("Access token expired.")
                        return [], {}, "TOKEN_EXPIRED"
                    elif chunk_status.startswith("ERROR"):
                        if overall_status == "SUCCESS":
                            overall_status = chunk_status
                        continue
                    elif chunk_status == "NEED_SPLIT":
                        # Still too many orders, skip this chunk
                        logger.warning(f"10-minute chunk {chunk_start.strftime('%H:%M')} - {chunk_end.strftime('%H:%M')} still has too many orders (>{limit}), skipping")
                        continue
                    elif chunk_status == "SUCCESS":
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
            elif status == "SUCCESS":
                # Merge hour orders
                for order in hour_orders:
                    order_id = order.get('purchaseOrderId')
                    if order_id and order_id not in all_order_details:
                        all_order_list.append(order)
                        all_order_details[order_id] = order
                
                # Merge order details
                for order_id, order_detail in hour_details.items():
                    if order_id not in all_order_details:
                        all_order_details[order_id] = order_detail
        
        return all_order_list, all_order_details, overall_status
    
    def get_orders(self, access_token, start_date, end_date, ship_node_type=None):
        """Get orders for a time range (wrapper method)
        
        Args:
            access_token (str): Walmart access token
            start_date (str): Start date string in format "YYYY-MM-DDTHH:MM:SSZ"
            end_date (str): End date string in format "YYYY-MM-DDTHH:MM:SSZ"
            ship_node_type (str, optional): Ship node type
            
        Returns:
            tuple: (order_list, order_details_dict, status)
        """
        return self.get_orders_for_time_range(access_token, start_date, end_date, ship_node_type)


def request_orders_by_date_range(start_date_str: str, end_date_str: str, account_tag: str = 'eForCity'):
    """
    Request orders by date range (loop through each day)
    
    Args:
        start_date_str: Start date in format YYYYMMDD (e.g., 20251101)
        end_date_str: End date in format YYYYMMDD (e.g., 20251102)
        account_tag: Walmart account tag (default: 'eForCity')
    
    Returns:
        List of order dictionaries
    """
    # Add stage1_basic_etl parent directory to path for token generator import
    stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    if stage1_parent_path not in sys.path:
        sys.path.insert(0, stage1_parent_path)
    
    from stage1_basic_etl.token_generator.walmart_access_token_generator import WalmartAccessTokenGenerator
    
    # Parse date strings
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    # Initialize requester and token generator
    requester = WalmartOrderRequester()
    token_generator = WalmartAccessTokenGenerator()
    
    all_orders = []
    all_order_details = {}
    
    # Loop through each day
    current_date = start_date
    while current_date <= end_date:
        # Set time range for the day (00:00:00 to 23:59:59 PST)
        pst_timezone = pytz.timezone('America/Los_Angeles')
        
        # Start of day in PST
        day_start_pst = pst_timezone.localize(
            datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)
        )
        # End of day in PST
        day_end_pst = pst_timezone.localize(
            datetime(current_date.year, current_date.month, current_date.day, 23, 59, 59)
        )
        
        # Convert to UTC for API
        day_start_utc = day_start_pst.astimezone(pytz.UTC)
        day_end_utc = day_end_pst.astimezone(pytz.UTC)
        
        # Format for API
        start_date_api = day_start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date_api = day_end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Requesting orders for date: {current_date.strftime('%Y-%m-%d')}")
        logger.info(f"PST Time: {day_start_pst.strftime('%Y-%m-%d %H:%M:%S %Z')} to {day_end_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"UTC Time (API): {start_date_api} to {end_date_api}")
        logger.info(f"{'='*80}")
        
        try:
            # Get access token
            token_result = token_generator.generate_access_token(account_tag)
            # Token generator returns {account_tag: response.json()}
            # response.json() contains: access_token, token_type, expires_in
            token_data = token_result.get(account_tag, {})
            access_token = token_data.get('access_token')
            
            if not access_token:
                logger.error(f"Error: No access token received for account {account_tag}")
                current_date += timedelta(days=1)
                continue
            
            # Request orders for the day
            order_list, order_details_dict, status = requester.get_orders_with_auto_split(
                access_token=access_token,
                start_date=start_date_api,
                end_date=end_date_api,
                ship_node_type=None,  # Request all types
                limit=200
            )
            
            if status == "SUCCESS":
                logger.info(f"✓ Successfully retrieved {len(order_list)} orders for {current_date.strftime('%Y-%m-%d')}")
                all_orders.extend(order_list)
                all_order_details.update(order_details_dict)
            else:
                logger.error(f"✗ Failed to retrieve orders for {current_date.strftime('%Y-%m-%d')}: {status}")
        
        except Exception as e:
            logger.error(f"✗ Error requesting orders for {current_date.strftime('%Y-%m-%d')}: {str(e)}")
        
        # Move to next day
        current_date += timedelta(days=1)
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Total orders retrieved: {len(all_orders)}")
    logger.info(f"Total order details: {len(all_order_details)}")
    logger.info(f"{'='*80}\n")
    
    # Convert order details to list format for parsing
    orders_for_parsing = list(all_order_details.values()) if all_order_details else all_orders
    
    return orders_for_parsing


def request_orders_by_hours(hours: int, account_tag: str = 'eForCity'):
    """
    Request orders for the last N hours (PST time)
    
    Args:
        hours: Number of hours to look back (PST time)
        account_tag: Walmart account tag (default: 'eForCity')
    
    Returns:
        List of order dictionaries
    """
    # Add stage1_basic_etl parent directory to path for token generator import
    stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    if stage1_parent_path not in sys.path:
        sys.path.insert(0, stage1_parent_path)
    
    from stage1_basic_etl.token_generator.walmart_access_token_generator import WalmartAccessTokenGenerator
    
    # Set timezone to PST
    pst_timezone = pytz.timezone('America/Los_Angeles')
    
    # Get current time in PST
    pst_now = datetime.now(pst_timezone)
    
    # Calculate N hours ago
    pst_n_hours_ago = pst_now - timedelta(hours=hours)
    
    # Convert to UTC for API
    pst_now_utc = pst_now.astimezone(pytz.UTC)
    pst_n_hours_ago_utc = pst_n_hours_ago.astimezone(pytz.UTC)
    
    # Format for API
    start_date_api = pst_n_hours_ago_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date_api = pst_now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Requesting orders for last {hours} hours (PST time)")
    logger.info(f"PST Time: {pst_n_hours_ago.strftime('%Y-%m-%d %H:%M:%S %Z')} to {pst_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"UTC Time (API): {start_date_api} to {end_date_api}")
    logger.info(f"{'='*80}\n")
    
    # Initialize requester and token generator
    requester = WalmartOrderRequester()
    token_generator = WalmartAccessTokenGenerator()
    
    try:
        # Get access token
        token_result = token_generator.generate_access_token(account_tag)
        # Token generator returns {account_tag: response.json()}
        # response.json() contains: access_token, token_type, expires_in
        token_data = token_result.get(account_tag, {})
        access_token = token_data.get('access_token')
        
        if not access_token:
            logger.error(f"Error: No access token received for account {account_tag}")
            return []
        
        # Request orders
        order_list, order_details_dict, status = requester.get_orders_with_auto_split(
            access_token=access_token,
            start_date=start_date_api,
            end_date=end_date_api,
            ship_node_type=None,  # Request all types
            limit=200
        )
        
        if status == "SUCCESS":
            logger.info(f"✓ Successfully retrieved {len(order_list)} orders")
            # Convert order details to list format for parsing
            orders_for_parsing = list(order_details_dict.values()) if order_details_dict else order_list
            return orders_for_parsing
        else:
            logger.error(f"✗ Failed to retrieve orders: {status}")
            return []
    
    except Exception as e:
        logger.error(f"✗ Error requesting orders: {str(e)}")
        return []


# Test example
if __name__ == "__main__":
    # Daily batch processing
    logger.info("Walmart Order Requester - Daily Batch Processing" + " -" * 100)
    
    # Note: access_token should be obtained from token generator
    # This is just an example structure
    access_token = 'xxx'
    
    requester = WalmartOrderRequester()
    
    # Date range: 2025-10-02 to 2025-10-03
    start_date_str = "2025-10-02"
    end_date_str = "2025-10-03"
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Get current directory for saving files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        current_date = start_date
        total_orders = 0
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            date_start = current_date.strftime("%Y-%m-%dT00:00:00Z")
            date_end = current_date.strftime("%Y-%m-%dT23:59:59Z")
            
            logger.info("=" * 100)
            logger.info(f"Processing date: {date_str}")
            logger.info("=" * 100)
            
            # Get orders for this day with auto split
            order_list, order_details, status = requester.get_orders_with_auto_split(
                access_token, date_start, date_end, ship_node_type=None
            )
            
            if status == "TOKEN_EXPIRED":
                logger.error(f"Access token expired for {date_str}. Please refresh token.")
                break
            elif status.startswith("ERROR"):
                logger.error(f"Failed to get orders for {date_str}. {status}")
                current_date += timedelta(days=1)
                continue
            elif status == "SUCCESS":
                logger.info(f"Successfully retrieved {len(order_list)} orders for {date_str}")
                
                # Save to JSON file
                filename = f"walmart_order_{date_str}.json"
                filepath = os.path.join(current_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(order_list, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Orders saved to: {filepath}")
                total_orders += len(order_list)
            
            current_date += timedelta(days=1)
        
        logger.info("=" * 100)
        logger.info("Batch processing completed!")
        logger.info(f"Date range: {start_date_str} to {end_date_str}")
        logger.info(f"Total orders collected: {total_orders}")
        logger.info(f"Files saved in: {current_dir}")
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()

