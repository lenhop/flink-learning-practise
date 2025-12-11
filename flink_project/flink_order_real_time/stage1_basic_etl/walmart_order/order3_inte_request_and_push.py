#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 3: 集成请求 Walmart 订单并推送到 Kafka, 提供 CLI。
"""

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
import pytz

flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)
# 添加 stage1_basic_etl 上级目录，便于导入 stage1_basic_etl.*
stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if stage1_parent_path not in sys.path:
    sys.path.insert(0, stage1_parent_path)

from stage1_basic_etl.walmart_order.order1_request_walmart_order import OrderRequestService  # noqa: E402
from stage1_basic_etl.walmart_order.order2_push_order_to_kafka import OrderKafkaPusher  # noqa: E402

# 日志
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'order3_inte_request_and_push.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def build_time_range_from_args(args):
    """根据参数构造 API 时间范围（UTC ISO）。"""
    if args.start_date:
        # 支持 YYYYMMDD 或 YYYY-MM-DD
        try:
            start_dt = datetime.strptime(args.start_date, '%Y%m%d')
            end_dt = datetime.strptime(args.end_date, '%Y%m%d')
        except ValueError:
            start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
        start_iso = start_dt.strftime('%Y-%m-%dT00:00:00Z')
        end_iso = end_dt.strftime('%Y-%m-%dT23:59:59Z')
        return start_iso, end_iso, f"{start_dt.date()} to {end_dt.date()}"

    # hours 模式，使用 PST
    la_tz = pytz.timezone('America/Los_Angeles')
    now_la = datetime.now(la_tz)
    start_la = now_la - timedelta(hours=args.hours)
    start_iso = start_la.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_iso = now_la.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    return start_iso, end_iso, f"last {args.hours}h (PST)"


def main():
    parser = argparse.ArgumentParser(
        description="Request Walmart orders and push to Kafka (integrated)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 日期范围（按天）
  python order3_inte_request_and_push.py -s 20251111 -e 20251111

  # 最近 N 小时（PST）
  python order3_inte_request_and_push.py -H 2

  # 指定 topic / account / ship_node_type
  python order3_inte_request_and_push.py -H 2 -t walmart_order_raw -a eForCity --ship-node-type SellerFulfilled
        """
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('-s', '--start-date', type=str, metavar='YYYYMMDD or YYYY-MM-DD',
                       help='Start date (with -e)')
    group.add_argument('-H', '--hours', type=int, metavar='N',
                       help='Last N hours (PST)')

    parser.add_argument('-e', '--end-date', type=str, metavar='YYYYMMDD or YYYY-MM-DD',
                        help='End date (with -s)')
    parser.add_argument('-a', '--account-tag', type=str, default='eForCity',
                        help='Walmart account tag')
    parser.add_argument('-t', '--topic', type=str, default='walmart_order_raw',
                        help='Kafka topic name')
    parser.add_argument('--ship-node-type', type=str, choices=['SellerFulfilled', 'WFSFulfilled', '3PLFulfilled'],
                        help='Ship node type (optional)')
    parser.add_argument('--batch-size', type=int, default=20, help='Kafka push batch size')

    args = parser.parse_args()

    if args.start_date and not args.end_date:
        parser.error("--start-date (-s) requires --end-date (-e)")
    if not args.start_date and not args.hours:
        args.hours = 1  # default last 1 hour

    start_iso, end_iso, label = build_time_range_from_args(args)
    logger.info("=" * 100)
    logger.info(f"Order request range: {label}")
    logger.info(f"UTC range: {start_iso} -> {end_iso}")
    logger.info(f"Account: {args.account_tag}, Topic: {args.topic}, ShipNode: {args.ship_node_type or 'ALL'}")
    logger.info("=" * 100)

    requester = OrderRequestService(account_tag=args.account_tag, limit=200)
    pusher = OrderKafkaPusher(kafka_topic=args.topic)

    try:
        orders, details, status = requester.request_with_auto_split(
            start_date=start_iso,
            end_date=end_iso,
            ship_node_type=args.ship_node_type,
            limit=200
        )
        if status != "SUCCESS":
            logger.warning(f"Request finished with status: {status}")

        total = len(orders)
        logger.info(f"Orders requested: {total}")

        if total > 0:
            pushed = pusher.push_orders(orders, batch_size=args.batch_size)
            logger.info(f"Orders pushed to Kafka: {pushed}")
        else:
            logger.warning("No orders to push")
    except Exception as e:
        logger.error(f"Integration failed: {e}")
        traceback.print_exc()
    finally:
        pusher.close()


if __name__ == "__main__":
    main()

