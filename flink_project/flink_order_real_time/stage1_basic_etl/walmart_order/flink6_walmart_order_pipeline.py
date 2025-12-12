#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink Walmart Order Pipeline (modular version using flink1~flink5 modules)

流程：
1) flink1_create_filnk_env: 创建执行环境
2) flink2_add_jar_to_flink: 加载 Kafka/MySQL 所需 JAR
3) flink3_add_parameter_to_flink: 配置并行度与 checkpoint
4) flink4_build_source: 创建 Kafka Source 并生成 DataStream
5) flink5_process_and_sink + flink5_build_mysql_sink: 解析订单并写入 MySQL
"""

import logging
import os
import sys
import traceback

# 路径设置（确保可找到 config 等模块）
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)
# 添加 stage1_basic_etl 上级目录，便于导入 stage1_basic_etl.*
stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if stage1_parent_path not in sys.path:
    sys.path.insert(0, stage1_parent_path)

from config.config import BaseConfig  # noqa: E402
from stage1_basic_etl.walmart_order.flink1_create_filnk_env import FlinkEnvironmentSetup  # noqa: E402
from stage1_basic_etl.walmart_order.flink2_add_jar_to_flink import FlinkJarManager  # noqa: E402
from stage1_basic_etl.walmart_order.flink3_add_parameter_to_flink import FlinkParameterConfigurator  # noqa: E402
from stage1_basic_etl.walmart_order.flink4_build_source import FlinkSourceBuilder  # noqa: E402
from stage1_basic_etl.walmart_order.flink5_process_and_sink import FlinkProcessAndSink  # noqa: E402
from stage1_basic_etl.walmart_order.flink5_build_mysql_sink import WalmartOrderMySQLSinkBuilder  # noqa: E402

# 日志配置
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'flink6_walmart_order_pipeline.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def load_config():
    cfg = BaseConfig().cfg
    kafka_cfg = cfg.get('kafka', {})
    mysql_cfg = cfg.get('mysql', {})
    return {
        "kafka": {
            "bootstrap_servers": kafka_cfg.get('bootstrap_servers', ['localhost:29092', 'localhost:39092']),
            "topic": 'walmart_order_raw',
            "consumer_group_id": 'flink-walmart-order-pipeline',
            "offset": 'latest',  # 如果需回放历史可改为 'earliest'
        },
        "mysql": {
            "host": mysql_cfg.get('host', 'localhost'),
            "port": mysql_cfg.get('port', 3306),
            "user": mysql_cfg.get('user', 'root'),
            "password": mysql_cfg.get('password', 'root123456'),
            "database": 'ods',
            "table_name": 'walmart_order'
        }
    }


def build_env_and_jars(config):
    # Step 1: create env
    env_setup = FlinkEnvironmentSetup(flink_project_path)
    env = env_setup.create_and_get_environment()

    # Step 2: add jars (Kafka mandatory, MySQL optional)
    jar_dir = os.path.join(flink_project_path, 'jar')
    FlinkJarManager.add_kafka_jars(env, jar_dir, logger=logger)
    # FlinkJarManager.add_mysql_jars(env, jar_dir, logger=logger)  # temporarily disabled to avoid JVM module access error

    # Step 3: configure parameters
    FlinkParameterConfigurator.configure_parallelism(env, parallelism=1, logger=logger)
    checkpoint_dir = os.path.join(flink_project_path, 'checkpoints', 'walmart_order_pipeline')
    FlinkParameterConfigurator.configure_checkpointing(env, checkpoint_dir=checkpoint_dir, interval_ms=60000, logger=logger)

    return env


def build_pipeline(env, config):
    # Build source
    source_builder = FlinkSourceBuilder(env, logger=logger)
    kafka_cfg = config["kafka"]
    kafka_source = source_builder.create_kafka_source(
        topic=kafka_cfg["topic"],
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=kafka_cfg["consumer_group_id"],
        offset=kafka_cfg["offset"]
    )
    raw_stream = source_builder.create_data_stream(kafka_source, source_name="Kafka Source")

    # Build MySQL sink
    mysql_cfg = config["mysql"]
    sink_builder = WalmartOrderMySQLSinkBuilder(logger=logger)
    mysql_sink = sink_builder.build(
        host=mysql_cfg["host"],
        port=mysql_cfg["port"],
        database=mysql_cfg["database"],
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        table_name=mysql_cfg["table_name"]
    )

    # Process & sink
    processor = FlinkProcessAndSink(env, logger=logger)
    row_stream = processor.parse_and_convert(raw_stream)
    processor.connect_mysql_sink(row_stream, mysql_sink)

    return env


def main():
    try:
        logger.info("=" * 100)
        logger.info("Flink Walmart Order Pipeline (modular) - Starting")
        logger.info("=" * 100)

        config = load_config()
        env = build_env_and_jars(config)
        env = build_pipeline(env, config)

        logger.info("=" * 100)
        logger.info("Pipeline: Kafka -> Parse -> MySQL")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 100)
        env.execute("Flink6 Walmart Order Pipeline")
    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

