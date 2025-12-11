#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 4: Build data source for Flink job (e.g., Kafka -> DataStream).

参考 flink_walmart_order_pipeline.py 的数据源接入部分,提供: 
- 创建 Kafka source
- 从 source 创建数据流（可配置 watermark 策略,默认无水位线）


# create_kafka_source: 创建 Kafka Source,仅返回 Source 对象,还未消费数据
# create_data_stream: 把已有 Source(如 Kafka)用 env.from_source(...) 注册到 Flink,生成 DataStream,可配置 watermark 和名字
# 基于已有的 Source(可以是 Kafka,也可以是其他 Source), 调用 env.from_source(...) 将 Source 注册到 Flink 执行环境，生成实际的数据流
# 用法: 先 create_kafka_source,再交给 create_data_stream 得到可处理的数据流

Usage:
    builder = FlinkSourceBuilder(env, logger)
    kafka_source = builder.create_kafka_source(
        topic="walmart_order_raw",
        bootstrap_servers=["localhost:29092", "localhost:39092"],
        group_id="flink-walmart-order-pipeline",
        offset="latest"
    )
    raw_stream = builder.create_data_stream(kafka_source, source_name="Kafka Source")
"""

import logging
import os
import sys

from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

# Add flink_project utils path for FlinkSourceUtils
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
utils_dir = os.path.join(flink_project_path, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

from flink_source_utils import FlinkSourceUtils # noqa: E402

# Simple logger (可被外部传入替换)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
default_logger = logging.getLogger(__name__)


class FlinkSourceBuilder:
    """构建 Flink 数据源(Kafka)"""

    def __init__(self, env: StreamExecutionEnvironment, logger=None):
        if env is None:
            raise RuntimeError("Flink environment not provided.")
        self.env = env
        self.logger = logger or default_logger
        self.source_utils = FlinkSourceUtils()

    def create_kafka_source(self, topic, bootstrap_servers, group_id, offset="latest"):
        """创建 Kafka source。"""
        try:
            self.logger.info("Step 4.1: Creating Kafka source...")
            self.logger.info(f"  Topic: {topic}")
            self.logger.info(f"  Bootstrap Servers: {bootstrap_servers}")
            self.logger.info(f"  Consumer Group: {group_id}")
            self.logger.info(f"  Offset: {offset}")

            kafka_source = self.source_utils.create_kafka_source(
                env=self.env,
                topic=topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                offset=offset
            )
            self.logger.info("✓ Kafka source created successfully")
            return kafka_source
        except Exception as e:
            self.logger.error(f"Failed to create Kafka source: {e}")
            raise

    def create_data_stream(self, source, watermark_strategy=None, source_name="Kafka Source"):
        """从 Source 创建 DataStream,默认无水位线。"""
        try:
            self.logger.info("Step 4.2: Creating data stream from source...")
            wm = watermark_strategy if watermark_strategy is not None else WatermarkStrategy.no_watermarks()
            raw_stream = self.env.from_source(source, wm, source_name)
            self.logger.info(" Data stream created successfully")
            return raw_stream
        except Exception as e:
            self.logger.error(f"Failed to create data stream: {e}")
            raise


if __name__ == "__main__":
    print("This module builds Flink sources. Import and use in pipeline steps.")

