#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink Walmart Order Pipeline V2 (using Flink JDBC instead of pymysql)

流程：
1) flink1_create_filnk_env: 创建执行环境
2) flink2_add_jar_to_flink: 加载 Kafka/MySQL 所需 JAR (including JDBC)
3) flink3_add_parameter_to_flink: 配置并行度与 checkpoint
4) flink4_build_source: 创建 Kafka Source 并生成 DataStream
5) flink5_process_and_sink_jdbc: 解析订单并使用 Flink JDBC 写入 MySQL
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
from stage1_basic_etl.walmart_order.flink5_parse_walmart_order import parse_walmart_order_json_string_to_tuples  # noqa: E402

from pyflink.common import Types, Row
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

# 日志配置
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'flink6_walmart_order_pipeline_v2.log')
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
            "consumer_group_id": 'flink-walmart-order-pipeline-v2',
            "offset": 'latest',  # 从最新的消息开始消费
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
    # Step 1: create env (FORCE LOCAL MODE to avoid remote cluster hanging)
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.common import Configuration
    
    logger.info("Creating Flink environment in FORCED LOCAL MODE...")
    flink_config = Configuration()
    # Don't set jobmanager address - force local mode
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)

    # Step 2: add jars (Kafka + JDBC + MySQL driver)
    jar_dir = os.path.join(flink_project_path, 'jar')
    jar_17_dir = os.path.join(jar_dir, '1.17')
    
    # Add Kafka JARs
    FlinkJarManager.add_kafka_jars(env, jar_17_dir, 
                                   jar_name='flink-connector-kafka-3.1.0-1.17.jar',
                                   clients_jar_name='../kafka-clients-3.4.1.jar', 
                                   logger=logger)
    
    # Add JDBC JARs
    jdbc_jar = os.path.join(jar_17_dir, 'flink-connector-jdbc-3.1.1-1.17.jar')
    mysql_jar = os.path.join(jar_dir, 'mysql-connector-java-8.0.28.jar')
    
    if os.path.exists(jdbc_jar):
        env.add_jars(f"file://{os.path.abspath(jdbc_jar)}")
        logger.info(f"✓ Added JDBC JAR: {os.path.basename(jdbc_jar)}")
    else:
        logger.error(f"✗ JDBC JAR missing: {jdbc_jar}")
        
    if os.path.exists(mysql_jar):
        env.add_jars(f"file://{os.path.abspath(mysql_jar)}")
        logger.info(f"✓ Added MySQL JAR: {os.path.basename(mysql_jar)}")
    else:
        logger.error(f"✗ MySQL JAR missing: {mysql_jar}")

    # Step 3: configure parameters
    FlinkParameterConfigurator.configure_parallelism(env, parallelism=1, logger=logger)
    checkpoint_dir = os.path.join(flink_project_path, 'checkpoints', 'walmart_order_pipeline_v2')
    FlinkParameterConfigurator.configure_checkpointing(env, checkpoint_dir=checkpoint_dir, interval_ms=60000, logger=logger)

    return env


def get_row_type_info():
    """Define the row type matching MySQL table structure."""
    return Types.ROW([
        # Order basic information (5 fields)
        Types.LONG(),           # purchaseOrderId BIGINT
        Types.LONG(),           # customerOrderId BIGINT
        Types.STRING(),         # customerEmailId VARCHAR(100)
        Types.LONG(),           # orderDate BIGINT
        Types.SQL_TIMESTAMP(),  # orderDate_formatted TIMESTAMP

        # Ship node information (3 fields)
        Types.STRING(),         # shipNode_type VARCHAR(50)
        Types.STRING(),         # shipNode_name VARCHAR(100)
        Types.STRING(),         # shipNode_id VARCHAR(50)

        # Data source information (2 fields)
        Types.STRING(),         # source_file VARCHAR(100)
        Types.STRING(),         # phone VARCHAR(20)

        # Estimated delivery information (5 fields)
        Types.LONG(),           # estimatedDeliveryDate BIGINT
        Types.SQL_TIMESTAMP(),  # estimatedDeliveryDate_formatted TIMESTAMP
        Types.LONG(),           # estimatedShipDate BIGINT
        Types.SQL_TIMESTAMP(),  # estimatedShipDate_formatted TIMESTAMP
        Types.STRING(),         # methodCode VARCHAR(50)

        # Recipient address information (8 fields)
        Types.STRING(),         # recipient_name VARCHAR(100)
        Types.STRING(),         # address1 VARCHAR(200)
        Types.STRING(),         # address2 VARCHAR(200)
        Types.STRING(),         # city VARCHAR(100)
        Types.STRING(),         # state VARCHAR(50)
        Types.STRING(),         # postalCode VARCHAR(20)
        Types.STRING(),         # country VARCHAR(10)
        Types.STRING(),         # addressType VARCHAR(20)

        # Order line item information (6 fields)
        Types.INT(),            # lineNumber INT
        Types.STRING(),         # sku VARCHAR(50)
        Types.STRING(),         # productName TEXT
        Types.STRING(),         # product_condition VARCHAR(50)
        Types.INT(),            # quantity INT
        Types.STRING(),         # unitOfMeasurement VARCHAR(20)

        # Order status information (6 fields)
        Types.LONG(),           # statusDate BIGINT
        Types.SQL_TIMESTAMP(),  # statusDate_formatted TIMESTAMP
        Types.STRING(),         # fulfillmentOption VARCHAR(50)
        Types.STRING(),         # shipMethod VARCHAR(50)
        Types.STRING(),         # storeId VARCHAR(50)
        Types.STRING(),         # shippingProgramType VARCHAR(50)

        # Charge information (6 fields)
        Types.STRING(),         # chargeType VARCHAR(50)
        Types.STRING(),         # chargeName VARCHAR(100)
        Types.DOUBLE(),         # chargeAmount DECIMAL(10,2)
        Types.STRING(),         # currency VARCHAR(10)
        Types.DOUBLE(),         # taxAmount DECIMAL(10,2)
        Types.STRING(),         # taxName VARCHAR(50)

        # Order line status information (3 fields)
        Types.STRING(),         # orderLineStatus VARCHAR(50)
        Types.INT(),            # statusQuantity INT
        Types.STRING(),         # cancellationReason VARCHAR(200)

        # Shipping information (6 fields)
        Types.LONG(),           # shipDateTime BIGINT
        Types.SQL_TIMESTAMP(),  # shipDateTime_formatted TIMESTAMP
        Types.STRING(),         # carrierName VARCHAR(100)
        Types.STRING(),         # carrierMethodCode VARCHAR(50)
        Types.STRING(),         # trackingNumber VARCHAR(100)
        Types.STRING(),         # trackingURL VARCHAR(500)

        # Data processing information (2 fields)
        Types.SQL_TIMESTAMP(),  # request_time DATETIME
        Types.SQL_TIMESTAMP()   # load_time DATETIME
    ])


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

    # Parse JSON and convert to rows
    def parse_order_json(json_str):
        try:
            logger.info(f"Parsing message (length: {len(json_str)})")
            tuples = parse_walmart_order_json_string_to_tuples(json_str)
            logger.info(f"✓ Parsed {len(tuples)} records from message")
            
            for idx, t in enumerate(tuples):
                logger.info(f"  Processing record {idx+1}/{len(tuples)}: order={t[0]}, sku={t[24]}")
                # Convert tuple to Row object for JDBC
                row = Row(
                    # Order basic information (5 fields)
                    t[0],   # purchaseOrderId
                    t[1],   # customerOrderId
                    t[2],   # customerEmailId
                    t[3],   # orderDate
                    t[4],   # orderDate_formatted
                    
                    # Ship node information (3 fields)
                    t[5],   # shipNode_type
                    t[6],   # shipNode_name
                    t[7],   # shipNode_id
                    
                    # Data source information (2 fields)
                    t[8],   # source_file
                    t[9],   # phone
                    
                    # Estimated delivery information (5 fields)
                    t[10],  # estimatedDeliveryDate
                    t[11],  # estimatedDeliveryDate_formatted
                    t[12],  # estimatedShipDate
                    t[13],  # estimatedShipDate_formatted
                    t[14],  # methodCode
                    
                    # Recipient address information (8 fields)
                    t[15],  # recipient_name
                    t[16],  # address1
                    t[17],  # address2
                    t[18],  # city
                    t[19],  # state
                    t[20],  # postalCode
                    t[21],  # country
                    t[22],  # addressType
                    
                    # Order line item information (6 fields)
                    t[23],  # lineNumber
                    t[24],  # sku
                    t[25],  # productName
                    t[26],  # product_condition
                    t[27],  # quantity
                    t[28],  # unitOfMeasurement
                    
                    # Order status information (6 fields)
                    t[29],  # statusDate
                    t[30],  # statusDate_formatted
                    t[31],  # fulfillmentOption
                    t[32],  # shipMethod
                    t[33],  # storeId
                    t[34],  # shippingProgramType
                    
                    # Charge information (6 fields)
                    t[35],  # chargeType
                    t[36],  # chargeName
                    t[37],  # chargeAmount
                    t[38],  # currency
                    t[39],  # taxAmount
                    t[40],  # taxName
                    
                    # Order line status information (3 fields)
                    t[41],  # orderLineStatus
                    t[42],  # statusQuantity
                    t[43],  # cancellationReason
                    
                    # Shipping information (6 fields)
                    t[44],  # shipDateTime
                    t[45],  # shipDateTime_formatted
                    t[46],  # carrierName
                    t[47],  # carrierMethodCode
                    t[48],  # trackingNumber
                    t[49],  # trackingURL
                    
                    # Data processing information (2 fields)
                    t[50],  # request_time
                    t[51]   # load_time
                )
                logger.info(f"  ✓ Row created with {len(row)} fields, yielding to JDBC sink")
                yield row
        except Exception as e:
            logger.error(f"❌ Parse error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Convert to proper row format
    row_stream = raw_stream.flat_map(parse_order_json, output_type=get_row_type_info())
    logger.info("✓ Parse & convert configured")

    # Build JDBC sink
    mysql_cfg = config["mysql"]
    
    # JDBC connection options
    jdbc_conn_opts = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_driver_name("com.mysql.cj.jdbc.Driver") \
        .with_url(f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}?useSSL=false&allowPublicKeyRetrieval=true") \
        .with_user_name(mysql_cfg['user']) \
        .with_password(mysql_cfg['password']) \
        .build()
    
    # JDBC execution options
    jdbc_exec_opts = JdbcExecutionOptions.builder() \
        .with_batch_size(100) \
        .with_batch_interval_ms(5000) \
        .with_max_retries(3) \
        .build()
    
    # Create JDBC sink with REPLACE INTO statement
    insert_sql = """REPLACE INTO ods.walmart_order (
        purchaseOrderId, customerOrderId, customerEmailId, orderDate, orderDate_formatted,
        shipNode_type, shipNode_name, shipNode_id, source_file, phone,
        estimatedDeliveryDate, estimatedDeliveryDate_formatted, estimatedShipDate, estimatedShipDate_formatted, methodCode,
        recipient_name, address1, address2, city, state, postalCode, country, addressType,
        lineNumber, sku, productName, product_condition, quantity, unitOfMeasurement,
        statusDate, statusDate_formatted, fulfillmentOption, shipMethod, storeId, shippingProgramType,
        chargeType, chargeName, chargeAmount, currency, taxAmount, taxName,
        orderLineStatus, statusQuantity, cancellationReason,
        shipDateTime, shipDateTime_formatted, carrierName, carrierMethodCode, trackingNumber, trackingURL,
        request_time, load_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    
    logger.info(f"JDBC URL: jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}")
    logger.info(f"JDBC User: {mysql_cfg['user']}")
    logger.info(f"INSERT SQL has {insert_sql.count('?')} placeholders")
    
    # Create JDBC sink - Row fields will be automatically mapped to SQL parameters
    jdbc_sink = JdbcSink.sink(
        insert_sql,
        get_row_type_info(),  # Type info for Row structure
        jdbc_conn_opts,
        jdbc_exec_opts
    )
    
    # Add sink to stream
    row_stream.add_sink(jdbc_sink)
    logger.info("✓ JDBC sink configured")

    return env


def main():
    try:
        logger.info("=" * 100)
        logger.info("Flink Walmart Order Pipeline V2 (JDBC) - Starting")
        logger.info("=" * 100)

        config = load_config()
        env = build_env_and_jars(config)
        env = build_pipeline(env, config)

        logger.info("=" * 100)
        logger.info("Pipeline: Kafka -> Parse -> JDBC -> MySQL")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 100)
        env.execute("Flink6 Walmart Order Pipeline V2 (JDBC)")
    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()