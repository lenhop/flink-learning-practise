#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink Walmart Order Pipeline (using Flink JDBC instead of pymysql)

流程：
1) 创建执行环境
2) 加载 Kafka/MySQL 所需 JAR (including JDBC)
3) 配置并行度与 checkpoint
4) 创建 Kafka Source 并生成 DataStream
5) 解析订单并使用 Flink JDBC 写入 MySQL
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
from stage1_basic_etl.walmart_order.flink5_process_and_sink_jdbc import FlinkProcessAndSinkJDBC  # noqa: E402

# 日志配置 - 配置立即刷新以避免日志缓冲
# 使用 PYTHONUNBUFFERED 环境变量或 python -u 标志效果更好
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'flink6_walmart_order_pipeline.log')

# 创建自定义 StreamHandler 强制立即刷新
class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()  # Force flush after each log

file_handler = logging.FileHandler(log_file)
stream_handler = FlushStreamHandler(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger(__name__)


def log_with_print(msg, level="INFO", separator=False):
    """Helper function to log and print simultaneously (PyFlink hijacks logging)"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,') + str(datetime.now().microsecond)[:3]
    
    if separator:
        # 在步骤之间添加分隔线
        print(f"{timestamp} - {level} - " + "=" * 80, flush=True)
    
    print(f"{timestamp} - {level} - {msg}", flush=True)
    if level == "INFO":
        logger.info(msg)
    elif level == "WARNING":
        logger.warning(msg)
    elif level == "ERROR":
        logger.error(msg)


def load_config():
    cfg = BaseConfig().cfg
    kafka_cfg = cfg.get('kafka', {})
    mysql_cfg = cfg.get('mysql', {})
    return {
        "kafka": {
            "bootstrap_servers": kafka_cfg.get('bootstrap_servers', ['localhost:29092', 'localhost:39092']),
            "topic": 'walmart_order_raw',
            "consumer_group_id": 'flink-walmart-order-pipeline-jdbc',
            "offset": 'latest',  # 从最新的消息开始消费，避免重复处理
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
    # Step 1: create env using FlinkEnvironmentSetup (FORCE LOCAL MODE to avoid remote cluster hanging)
    logger.info("Step 1: Creating Flink execution environment...")
    sys.stdout.flush()
    env_setup = FlinkEnvironmentSetup(
        flink_project_path=flink_project_path,
        force_local_mode=True
    )
    env = env_setup.create_and_get_environment()
    log_with_print("✓ Step 1 completed: Flink environment created", separator=True)

    # Step 2: add jars (Kafka + JDBC + MySQL driver)
    log_with_print("Step 2: Loading JAR files...")
    jar_dir = os.path.join(flink_project_path, 'jar')
    jar_17_dir = os.path.join(jar_dir, '1.17')
    
    logger.info(f"  JAR directory: {jar_dir}")
    logger.info(f"  Flink 1.17 JAR directory: {jar_17_dir}")
    
    # Add Kafka JARs using FlinkJarManager (same as old working version)
    logger.info("  Loading Kafka JARs...")
    FlinkJarManager.add_kafka_jars(env, jar_17_dir, 
                                   jar_name='flink-connector-kafka-3.1.0-1.17.jar',
                                   clients_jar_name='../kafka-clients-3.4.1.jar', 
                                   logger=logger)
    log_with_print("  ✓ Kafka JARs loaded")
    
    # Add JDBC JARs
    logger.info("  Loading JDBC JARs...")
    print("", flush=True)
    jdbc_jar = os.path.join(jar_17_dir, 'flink-connector-jdbc-3.1.1-1.17.jar')
    mysql_jar = os.path.join(jar_dir, 'mysql-connector-java-8.0.28.jar')
    
    if os.path.exists(jdbc_jar):
        env.add_jars(f"file://{os.path.abspath(jdbc_jar)}")
        logger.info(f"  ✓ Added JDBC JAR: {os.path.basename(jdbc_jar)}")
    else:
        logger.error(f"  ✗ JDBC JAR missing: {jdbc_jar}")
        
    if os.path.exists(mysql_jar):
        env.add_jars(f"file://{os.path.abspath(mysql_jar)}")
        logger.info(f"  ✓ Added MySQL JAR: {os.path.basename(mysql_jar)}")
    else:
        logger.error(f"  ✗ MySQL JAR missing: {mysql_jar}")
    log_with_print("✓ Step 2 completed: All JARs loaded", separator=True)

    # Step 3: configure parameters using FlinkParameterConfigurator
    log_with_print("Step 3: Configuring Flink parameters...")
    FlinkParameterConfigurator.configure_parallelism(env, parallelism=1, logger=logger)
    checkpoint_dir = os.path.join(flink_project_path, 'checkpoints', 'walmart_order_pipeline')
    # Use shorter checkpoint interval to ensure data is committed more frequently
    FlinkParameterConfigurator.configure_checkpointing(env, checkpoint_dir=checkpoint_dir, interval_ms=10000, logger=logger)
    log_with_print("✓ Step 3 completed: Parameters configured")
    log_with_print("✓ Environment and JARs ready", separator=True)
    return env




# Row type info is now provided by FlinkProcessAndSinkJDBC class


def build_pipeline(env, config):
    """
    Build the complete Flink pipeline using modular components.
    
    Steps:
    1. Create Kafka source
    2. Parse and convert data using FlinkProcessAndSinkJDBC
    3. Connect JDBC sink to MySQL
    """
    # Step 1: Build Kafka source
    log_with_print("Building Kafka source...")
    source_builder = FlinkSourceBuilder(env, logger=logger)
    kafka_cfg = config["kafka"]
    kafka_source = source_builder.create_kafka_source(
        topic=kafka_cfg["topic"],
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=kafka_cfg["consumer_group_id"],
        offset=kafka_cfg["offset"]
    )
    raw_stream = source_builder.create_data_stream(kafka_source, source_name="Kafka Source")
    log_with_print("✓ Kafka source created")

    # Step 2 & 3: Parse, convert and sink using FlinkProcessAndSinkJDBC
    log_with_print("Setting up data processing and JDBC sink...")
    processor = FlinkProcessAndSinkJDBC(env, logger=logger)
    
    # Parse and convert JSON to Row objects
    row_stream = processor.parse_and_convert(raw_stream)
    log_with_print("✓ Step 4 completed: Parse & convert configured", separator=True)
    
    # Connect JDBC sink with optimized batch settings
    # Using smaller batch size (10) and interval (1000ms) for better reliability
    # rewriteBatchedStatements=true is included in the sink builder for performance
    processor.connect_jdbc_sink(
        row_stream=row_stream,
        mysql_config=config["mysql"],
        batch_size=10,
        batch_interval_ms=1000,
        max_retries=3
    )
    log_with_print("✓ Step 5 completed: JDBC sink connected", separator=True)
    log_with_print("✓ Step 6 completed: Pipeline configured successfully", separator=True)

    return env


def main():
    import sys
    try:
        logger.info("=" * 100)
        logger.info("Flink Walmart Order Pipeline (JDBC) - Starting")
        logger.info("=" * 100)
        sys.stdout.flush()

        logger.info("Loading configuration...")
        sys.stdout.flush()
        config = load_config()
        logger.info("✓ Configuration loaded")
        sys.stdout.flush()

        logger.info("Building Flink environment and loading JARs...")
        sys.stdout.flush()
        env = build_env_and_jars(config)
        logger.info("✓ Environment and JARs ready")
        sys.stdout.flush()

        log_with_print("Building pipeline...")
        env = build_pipeline(env, config)
        log_with_print("✓ Pipeline built")

        log_with_print("=" * 100)
        log_with_print("Pipeline: Kafka -> Parse -> JDBC -> MySQL")
        log_with_print("=" * 100)
        log_with_print("Starting Flink job execution...")
        logger.info("NOTE: The job will run continuously and wait for incoming data from Kafka")
        logger.info("      Data will be processed automatically as it arrives")
        logger.info("=" * 100)
        sys.stdout.flush()
        
        logger.info("Calling env.execute() - this may take a moment to initialize...")
        logger.info("  Flink will start the job and wait for data from Kafka")
        logger.info("  If you see this message, the job is starting...")
        sys.stdout.flush()
        
        try:
            logger.info("  [DEBUG] About to call env.execute()...")
            sys.stdout.flush()
            env.execute("Flink6 Walmart Order Pipeline (JDBC)")
            logger.info("Job execution completed (this should not appear unless job stops)")
        except Exception as execute_error:
            logger.error(f"Error during env.execute(): {execute_error}")
            logger.error(f"Error type: {type(execute_error).__name__}")
            sys.stdout.flush()
            traceback.print_exc()
            raise
    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
        sys.stdout.flush()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.stdout.flush()
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()