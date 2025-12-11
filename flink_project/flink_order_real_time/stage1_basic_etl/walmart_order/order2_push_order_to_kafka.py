#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 2: 将订单推送到 Kafka（仅负责 Kafka 相关）。
"""

import logging
import os
import sys

from datetime import datetime

# 路径设置
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

# utils 路径
utils_dir = os.path.join(flink_project_path, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

from config.config import BaseConfig  # noqa: E402
import kafka_admin_utils  # noqa: E402
import kafka_producer_utils  # noqa: E402
KafkaAdminUtils = kafka_admin_utils.KafkaAdminUtils
KafkaProducerUtils = kafka_producer_utils.KafkaProducerUtils

# 日志
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'order2_push_order_to_kafka.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class OrderKafkaPusher:
    """只负责 Kafka topic 确认与发送。"""

    def __init__(self, kafka_topic='walmart_order_raw', bootstrap_servers=None, config=None):
        self.kafka_topic = kafka_topic
        self.config = config or BaseConfig().cfg
        kafka_cfg = self.config.get('kafka', {})
        if bootstrap_servers is None:
            self.bootstrap_servers = kafka_cfg.get('bootstrap_servers', ['localhost:29092', 'localhost:39092'])
        else:
            self.bootstrap_servers = bootstrap_servers if isinstance(bootstrap_servers, list) else [bootstrap_servers]

        self.kafka_admin = None
        self.kafka_producer = None

    def _ensure_topic(self):
        if self.kafka_admin is None:
            self.kafka_admin = KafkaAdminUtils(bootstrap_servers=self.bootstrap_servers)
        exists = self.kafka_admin.check_topic_exists(self.kafka_topic)
        if not exists:
            logger.info(f"Topic '{self.kafka_topic}' does not exist, creating...")
            result = self.kafka_admin.create_topic(
                topic_name=self.kafka_topic,
                num_partitions=3,
                replication_factor=1,
                config={
                    'retention.ms': '604800000',
                    'max.message.bytes': '10485760'
                }
            )
            status = result.get(self.kafka_topic, {}).get('status')
            if status != 'success':
                msg = result.get(self.kafka_topic, {}).get('message', 'Unknown error')
                raise RuntimeError(f"Failed to create topic '{self.kafka_topic}': {msg}")
            logger.info(f"Topic '{self.kafka_topic}' created successfully")
        else:
            logger.info(f"Topic '{self.kafka_topic}' already exists")

    def _connect_producer(self):
        if self.kafka_producer is None:
            self.kafka_producer = KafkaProducerUtils(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.kafka_topic
            )
            self.kafka_producer.connect_kafka(
                max_request_size=10485760,
                request_timeout_ms=60000
            )

    def push_orders(self, orders, batch_size=20, key_field='purchaseOrderId'):
        if not orders:
            logger.warning("No orders to push to Kafka")
            return 0

        self._ensure_topic()
        self._connect_producer()

        if isinstance(orders, dict):
            orders = list(orders.values())
        if not isinstance(orders, list):
            orders = [orders]

        total = len(orders)
        total_pushed = 0
        total_failed = 0
        num_batches = (total + batch_size - 1) // batch_size

        logger.info(f"Pushing {total} orders to Kafka topic '{self.kafka_topic}' in batches of {batch_size}...")

        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total)
            batch = orders[start_idx:end_idx]
            logger.info(f"Pushing batch {batch_idx + 1}/{num_batches} ({len(batch)} orders)...")
            try:
                self.kafka_producer.send_messages_sync_wait(
                    messages=batch,
                    key_field=key_field,
                    flush_count=len(batch),
                    close_after_send=False
                )
                total_pushed += len(batch)
                logger.info(f"Batch {batch_idx + 1}/{num_batches} pushed successfully")
            except Exception as e:
                total_failed += len(batch)
                logger.error(f"Failed to push batch {batch_idx + 1}/{num_batches}: {e}")
                continue

        logger.info(f"Successfully pushed {total_pushed}/{total} orders to Kafka")
        if total_failed > 0:
            logger.warning(f"Failed to push {total_failed} orders")
        return total_pushed

    def close(self):
        if self.kafka_producer:
            try:
                self.kafka_producer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
            finally:
                self.kafka_producer = None


if __name__ == "__main__":
    print("This module only handles pushing orders to Kafka. Import and use in integration.")

