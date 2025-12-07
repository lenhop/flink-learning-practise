import requests
import json
import time
import os
import sys
import logging
from datetime import datetime, timedelta

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
            return response.json()
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
        
        order_list = []
        order_details = {}
        for order in orders:
            if isinstance(order, dict):
                order_id = order.get('purchaseOrderId')
                if order_id:
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
        
        Splits the time range into 24 hours. If any hour returns more than limit orders,
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
        
        # Split into 24 hours
        hour_chunks = self._split_time_range(start_dt, end_dt, 24)
        
        all_order_list = []
        all_order_details = {}
        overall_status = "SUCCESS"
        
        for hour_idx, (hour_start, hour_end) in enumerate(hour_chunks):
            hour_start_str = hour_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            hour_end_str = hour_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Progress bar for hourly requests
            progress = f"Processing hour {hour_idx+1}/24: {hour_start.strftime('%H:%M')} - {hour_end.strftime('%H:%M')}"
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
                logger.info(f"Hour {hour_start.strftime('%H:%M')} - {hour_end.strftime('%H:%M')} needs splitting into 10-minute chunks")
                ten_minute_chunks = self._split_time_range(hour_start, hour_end, 6)
                
                for chunk_idx, (chunk_start, chunk_end) in enumerate(ten_minute_chunks):
                    chunk_start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    chunk_end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Progress bar for 10-minute chunks
                    chunk_progress = f"  Processing chunk {chunk_idx+1}/6: {chunk_start.strftime('%H:%M')} - {chunk_end.strftime('%H:%M')}"
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
                        logger.warning(f"Chunk {chunk_start.strftime('%H:%M')} - {chunk_end.strftime('%H:%M')} still has too many orders, skipping")
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


# Test example
if __name__ == "__main__":
    # Daily batch processing
    logger.info("Walmart Order Requester - Daily Batch Processing" + " -" * 100)
    
    # Note: access_token should be obtained from token generator
    # This is just an example structure
    access_token = 'eyJraWQiOiJmODI1ZDdmZi04NTFiLTQ5OWYtOTcxMi04NGRiODc2YWY4YWIiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiZGlyIn0..Q3rWlDvuYLsGB9zh.wH5K79vq3_3DHJe1U5NSSvBrMLkJoZI68tP7-sKB7vbdXmsQb3gxnjEvqBm-FsPFdbuPhhVuGXkKtXsUHPDpJNJTKu2xCfZvHNKQJKcQTeIurIRlLbiHBdj4JI7HldG322QadTUtSavV2T2vEPpSkJs9aPLwGGWv2EI-cmwYue_79UDFDeQRLB9RIcO4fJscAAVK7z4Cq3LOt3C1XJNzT7JxYu6IbmijZ5_ZaPFWDBijhsgvIupaY4WWuNNON4mfNgR2ztJA5Q_8kggeH4LzHOmAaNXpqFsXBoQW9pNXavB89BZZV6UWw2UDstqmyqH0Jn4c3T7P_-WeqUyicmvbpCsgfTKAbS2klKRKTv6nVyphuWqYV8QjWtebkNDItqz5my-4WTzYZm0-LQB3RW-uvwMU76eGLok3Mapw1nKeorddfmWqnLwuLnFNwH8KWFVmE7rZCxUSLBvDITi-WAUntCk6nyrQA2yaZwnotPvynSIVHzgmkwOla7-EXQXS8YP8R4pNHHn7UIyO_W96aYgF5nd0eKpMkFDgW32_0_JV74mcdw3pX0hyVgEgDyieVD6HP6sdL6oynavL3h1jOyMHvKfJ52h3oMc0lzRaE74BgA-KGgLJEozqOJXmuhtcfXfYBQfWR3Z86ZuCdBdQK4XBAZ7LipQ-QiqkyXl1zVFCUQW4upSD_5PKZJP114WY.BUIKT7f96SmkqE7KVHWW3g'
    
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

