#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage 2: Windowed statistics pipeline scaffold.
- Kafka source (walmart_order_raw)
- Event-time Watermark
- Windowed aggregations (minute/hour, user/item/status)
- JDBC sink to MySQL (stats tables)

Fill in:
- Kafka bootstrap_servers/topic from config
- Watermark definition and timestamp extractor
- Window aggregations in window_aggregators.py
- JDBC sink options (URL/user/password/table)
"""

import os
import sys
import logging

# Path setup
FLINK_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if FLINK_PROJECT_PATH not in sys.path:
    sys.path.insert(0, FLINK_PROJECT_PATH)
STAGE_PARENT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if STAGE_PARENT not in sys.path:
    sys.path.insert(0, STAGE_PARENT)

from config.config import BaseConfig  # noqa: E402

# TODO: import PyFlink classes when implementing
# from pyflink.common import WatermarkStrategy, Types
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config():
    cfg = BaseConfig().cfg
    kafka_cfg = cfg.get('kafka', {})
    mysql_cfg = cfg.get('mysql', {})
    return {
        "kafka": {
            "bootstrap_servers": kafka_cfg.get('bootstrap_servers', ['localhost:29092', 'localhost:39092']),
            "topic": kafka_cfg.get('topic', 'walmart_order_raw'),
            "group_id": kafka_cfg.get('group_id', 'flink-walmart-order-stats'),
            "offset": kafka_cfg.get('offset', 'latest'),
        },
        "mysql": {
            "host": mysql_cfg.get('host', 'localhost'),
            "port": mysql_cfg.get('port', 3306),
            "user": mysql_cfg.get('user', 'root'),
            "password": mysql_cfg.get('password', 'root123456'),
            "database": mysql_cfg.get('database', 'ods'),
        }
    }


def main():
    # TODO: implement env, source, watermark, windows, sinks
    logger.info("Stage2 window statistics scaffold ready. Implement windowed aggregations next.")


if __name__ == "__main__":
    main()


