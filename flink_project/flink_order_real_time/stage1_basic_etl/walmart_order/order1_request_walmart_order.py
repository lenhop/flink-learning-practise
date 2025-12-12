#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 1: 获取 Walmart 订单（含 token 获取与自动拆分请求）。
"""

import logging
import os
import sys
from datetime import datetime, timedelta
import pytz

# 路径设置
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if stage1_parent_path not in sys.path:
    sys.path.insert(0, stage1_parent_path)

from stage1_basic_etl.token_generator.walmart_access_token_generator import WalmartAccessTokenGenerator  # noqa: E402
# 日志
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'order1_request_walmart_order.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class WalmartOrderRequester:
    """仅负责请求 Walmart 订单（含自动拆分逻辑）。"""

    BASE_URL = "https://marketplace.walmartapis.com/v3"

    def __init__(self):
        pass

    def _get_orders_for_single_type(self, access_token, start_date, end_date, ship_node_type, limit=200):
        import requests
        import json
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
            "WM_QOS.CORRELATION_ID": f"correlation_id_{int(datetime.now().timestamp())}",
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
                    import time
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
            except Exception:
                return [], {}, "ERROR"

        if response is None or response.status_code != 200:
            return [], {}, "ERROR"

        try:
            data = response.json()
            if isinstance(data, str):
                import json
                data = json.loads(data)
        except Exception:
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

        # 请求时间
        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        order_list = []
        order_details = {}
        for order in orders:
            if isinstance(order, dict):
                order_id = order.get('purchaseOrderId')
                if order_id:
                    order['request_time'] = request_time
                    order_list.append(order)
                    order_details[order_id] = order

        if total_count > limit:
            return [], {}, "NEED_SPLIT"

        return order_list, order_details, "SUCCESS"

    def get_orders_for_time_range(self, access_token, start_date, end_date, ship_node_type=None, limit=200):
        ship_node_types = ["SellerFulfilled", "WFSFulfilled", "3PLFulfilled"]
        if ship_node_type:
            return self._get_orders_for_single_type(access_token, start_date, end_date, ship_node_type, limit)

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

            for order in order_list:
                order_id = order.get('purchaseOrderId')
                if order_id and order_id not in all_order_details:
                    all_order_list.append(order)
                    all_order_details[order_id] = order
            for order_id, order_detail in order_details.items():
                if order_id not in all_order_details:
                    all_order_details[order_id] = order_detail

        return all_order_list, all_order_details, overall_status

    @staticmethod
    def _split_time_range(start_datetime, end_datetime, split_factor):
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
        logger.info(
            "Start requesting orders: start=%s end=%s ship_node_type=%s limit=%s",
            start_date, end_date, ship_node_type, limit
        )
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        hourly_chunks = []
        current_start = start_dt
        while current_start < end_dt:
            current_end = current_start + timedelta(hours=1)
            if current_end > end_dt:
                current_end = end_dt
            hourly_chunks.append((current_start, current_end))
            current_start = current_end
        logger.info("Hour chunks to process: %s", len(hourly_chunks))

        all_order_list = []
        all_order_details = {}
        overall_status = "SUCCESS"

        for hour_start, hour_end in hourly_chunks:
            logger.info(
                "Processing hour chunk: %s -> %s",
                hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                hour_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            hour_start_str = hour_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            hour_end_str = hour_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            hour_orders, hour_details, status = self.get_orders_for_time_range(
                access_token, hour_start_str, hour_end_str, ship_node_type, limit
            )
            logger.info(
                "Hour chunk status=%s orders=%s unique_total=%s",
                status, len(hour_orders), len(all_order_details)
            )
            if status == "TOKEN_EXPIRED":
                return [], {}, "TOKEN_EXPIRED"
            elif status.startswith("ERROR"):
                if overall_status == "SUCCESS":
                    overall_status = status
                continue

            if status == "NEED_SPLIT":
                logger.info("Need split for hour chunk, splitting into 10-minute chunks")
                ten_minute_chunks = self._split_time_range(hour_start, hour_end, 6)
                for chunk_start, chunk_end in ten_minute_chunks:
                    logger.info(
                        "Processing 10-minute chunk: %s -> %s",
                        chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                    )
                    chunk_start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    chunk_end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                    chunk_orders, chunk_details, chunk_status = self.get_orders_for_time_range(
                        access_token, chunk_start_str, chunk_end_str, ship_node_type, limit
                    )
                    if chunk_status == "TOKEN_EXPIRED":
                        return [], {}, "TOKEN_EXPIRED"
                    elif chunk_status.startswith("ERROR"):
                        if overall_status == "SUCCESS":
                            overall_status = chunk_status
                        continue
                    elif chunk_status == "NEED_SPLIT":
                        continue
                    elif chunk_status == "SUCCESS":
                        for order in chunk_orders:
                            order_id = order.get('purchaseOrderId')
                            if order_id and order_id not in all_order_details:
                                all_order_list.append(order)
                                all_order_details[order_id] = order
                        for order_id, order_detail in chunk_details.items():
                            if order_id not in all_order_details:
                                all_order_details[order_id] = order_detail
                        logger.info(
                            "10-minute chunk success: added=%s cumulative=%s",
                            len(chunk_orders), len(all_order_details)
                        )
            elif status == "SUCCESS":
                for order in hour_orders:
                    order_id = order.get('purchaseOrderId')
                    if order_id and order_id not in all_order_details:
                        all_order_list.append(order)
                        all_order_details[order_id] = order
                for order_id, order_detail in hour_details.items():
                    if order_id not in all_order_details:
                        all_order_details[order_id] = order_detail
                logger.info(
                    "Hour chunk success: added=%s cumulative=%s",
                    len(hour_orders), len(all_order_details)
                )

        return all_order_list, all_order_details, overall_status


class OrderRequestService:
    """封装 token 获取和订单请求（自动拆分）。"""

    def __init__(self, account_tag='eForCity', limit=200):
        self.account_tag = account_tag
        self.limit = limit
        self.token_generator = WalmartAccessTokenGenerator()
        self.requester = WalmartOrderRequester()
        self.access_token = None
        self.token_expires_at = None

    def _get_access_token(self, force_refresh=False):
        now = datetime.now()
        if (not force_refresh and self.access_token and self.token_expires_at
                and now < self.token_expires_at - timedelta(minutes=5)):
            return self.access_token

        logger.info(f"Generating new access token for account: {self.account_tag}")
        token_result = self.token_generator.generate_access_token(self.account_tag)
        token_data = token_result.get(self.account_tag, {})
        self.access_token = token_data.get('access_token')
        expires_in = token_data.get('expires_in', 900)
        self.token_expires_at = now + timedelta(seconds=expires_in)
        if not self.access_token:
            raise RuntimeError(f"No access token received for account {self.account_tag}")
        return self.access_token

    def request_with_auto_split(self, start_date, end_date, ship_node_type=None, limit=None):
        """调用 requester 的自动拆分获取订单。返回 (orders, details, status)。"""
        limit = limit or self.limit
        access_token = self._get_access_token()
        logger.info(
            "Request with auto split: start=%s end=%s ship_node_type=%s limit=%s",
            start_date, end_date, ship_node_type, limit
        )
        orders, details, status = self.requester.get_orders_with_auto_split(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            ship_node_type=ship_node_type,
            limit=limit
        )
        # token 过期重试一次
        if status == "TOKEN_EXPIRED":
            logger.warning("Access token expired, refreshing...")
            access_token = self._get_access_token(force_refresh=True)
            orders, details, status = self.requester.get_orders_with_auto_split(
                access_token=access_token,
                start_date=start_date,
                end_date=end_date,
                ship_node_type=ship_node_type,
                limit=limit
            )
        return orders, details, status

    @staticmethod
    def build_pst_range(hours=1):
        """构造最近 N 小时的 PST 时间范围（转 UTC ISO 字符串）。"""
        la_tz = pytz.timezone('America/Los_Angeles')
        now_la = datetime.now(la_tz)
        start_la = now_la - timedelta(hours=hours)
        start_utc = start_la.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_utc = now_la.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        return start_utc, end_utc


if __name__ == "__main__":
    print("This module provides order request service. Import and use in integration.")

