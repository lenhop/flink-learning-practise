#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink Walmart Order Complete Pipeline - Kafka -> Flink -> Parse -> MySQL

This script implements the complete pipeline in a SINGLE Flink Job:
1. Kafka consumption (from walmart_order_raw)
2. Order parsing (using parse_walmart_order_json_string)
3. Data transformation (dict to row format)
4. MySQL insertion (to ods.walmart_order)

Data Flow (all in ONE Flink Job, no additional Kafka topics):
  Kafka Source -> Parse (flatMap) -> Transform (map) -> MySQL Sink
     ↓              ↓                    ↓                ↓
  JSON String   Order Dict          Row Tuple        MySQL Table

This is the RECOMMENDED approach for production use.

Usage:
    python flink_walmart_order_pipeline.py
"""

import os
import sys
import logging
from datetime import datetime

# Add flink_project directory to path
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

from config.config import BaseConfig

# Import Flink utils
utils_dir = os.path.join(flink_project_path, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

import flink_source_utils
import flink_sink_utils
FlinkSourceUtils = flink_source_utils.FlinkSourceUtils
FlinkSinkUtils = flink_sink_utils.FlinkSinkUtils

# Import parse function
from stage1_basic_etl.walmart_order.parse_walmart_order import parse_walmart_order_json_string

# Configure logger
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, 'flink_walmart_order_pipeline.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_order_flatmap(json_str: str):
    """
    FlatMap function to parse order JSON string
    
    Args:
        json_str: JSON string from Kafka
        
    Yields:
        Parsed order dictionaries (one per order line)
    """
    try:
        parsed_orders = parse_walmart_order_json_string(json_str)
        for order in parsed_orders:
            yield order
    except Exception as e:
        logger.error(f"Error parsing order: {str(e)}")


def convert_to_row_data(order_dict: dict) -> tuple:
    """
    Convert order dictionary to tuple for MySQL insertion
    
    Args:
        order_dict: Parsed order dictionary
        
    Returns:
        Tuple of values matching SQL placeholders
    """
    def parse_timestamp(ts_str):
        if ts_str is None:
            return None
        try:
            return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    return (
        order_dict.get('purchaseOrderId'),
        order_dict.get('customerOrderId'),
        order_dict.get('customerEmailId'),
        order_dict.get('orderDate'),
        parse_timestamp(order_dict.get('orderDate_formatted')),
        order_dict.get('shipNode_type'),
        order_dict.get('shipNode_name'),
        order_dict.get('shipNode_id'),
        order_dict.get('source_file'),
        order_dict.get('phone'),
        order_dict.get('estimatedDeliveryDate'),
        parse_timestamp(order_dict.get('estimatedDeliveryDate_formatted')),
        order_dict.get('estimatedShipDate'),
        parse_timestamp(order_dict.get('estimatedShipDate_formatted')),
        order_dict.get('methodCode'),
        order_dict.get('recipient_name'),
        order_dict.get('address1'),
        order_dict.get('address2'),
        order_dict.get('city'),
        order_dict.get('state'),
        order_dict.get('postalCode'),
        order_dict.get('country'),
        order_dict.get('addressType'),
        order_dict.get('lineNumber'),
        order_dict.get('sku'),
        order_dict.get('productName'),
        order_dict.get('product_condition'),
        order_dict.get('quantity'),
        order_dict.get('unitOfMeasurement'),
        order_dict.get('statusDate'),
        parse_timestamp(order_dict.get('statusDate_formatted')),
        order_dict.get('fulfillmentOption'),
        order_dict.get('shipMethod'),
        order_dict.get('storeId'),
        order_dict.get('shippingProgramType'),
        order_dict.get('chargeType'),
        order_dict.get('chargeName'),
        order_dict.get('chargeAmount'),
        order_dict.get('currency'),
        order_dict.get('taxAmount'),
        order_dict.get('taxName'),
        order_dict.get('orderLineStatus'),
        order_dict.get('statusQuantity'),
        order_dict.get('cancellationReason'),
        order_dict.get('shipDateTime'),
        parse_timestamp(order_dict.get('shipDateTime_formatted')),
        order_dict.get('carrierName'),
        order_dict.get('carrierMethodCode'),
        order_dict.get('trackingNumber'),
        order_dict.get('trackingURL')
    )


def main():
    """Main function to create and execute complete Flink pipeline"""
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.common.watermark_strategy import WatermarkStrategy
    from pyflink.common.typeinfo import Types
    from pyflink.datastream.functions import FlatMapFunction, MapFunction
    
    # Load configuration
    config = BaseConfig().cfg
    
    # Kafka configuration
    kafka_config = config.get('kafka', {})
    bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:29092', 'localhost:39092'])
    kafka_topic = 'walmart_order_raw'
    consumer_group_id = 'flink-walmart-order-pipeline'
    
    # MySQL configuration
    mysql_config = config.get('mysql', {})
    mysql_host = mysql_config.get('host', 'localhost')
    mysql_port = mysql_config.get('port', 3306)
    mysql_user = mysql_config.get('user', 'root')
    mysql_password = mysql_config.get('password', 'root123456')
    mysql_database = 'ods'
    mysql_table = 'walmart_order'
    
    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?useSSL=false&serverTimezone=UTC"
    jdbc_driver = "com.mysql.cj.jdbc.Driver"
    
    logger.info("=" * 100)
    logger.info("Flink Walmart Order Complete Pipeline - Starting")
    logger.info("=" * 100)
    logger.info("Pipeline Design: Single Flink Job (no additional Kafka topics)")
    logger.info("")
    logger.info("Data Flow:")
    logger.info("  1. Kafka Source -> Read from 'walmart_order_raw'")
    logger.info("  2. Parse -> Extract order data (flatMap)")
    logger.info("  3. Transform -> Convert to MySQL row format (map)")
    logger.info("  4. MySQL Sink -> Write to ods.walmart_order")
    logger.info("")
    logger.info("Configuration:")
    logger.info(f"  Kafka Topic: {kafka_topic}")
    logger.info(f"  Bootstrap Servers: {bootstrap_servers}")
    logger.info(f"  MySQL: {mysql_host}:{mysql_port}/{mysql_database}.{mysql_table}")
    logger.info("=" * 100)
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Add required JAR files for Kafka and JDBC connectors
    # These JARs are required for KafkaSource and JdbcSink to work
    jar_dir = os.path.join(flink_project_path, 'jar')
    kafka_connector_jar = os.path.join(jar_dir, 'flink-connector-kafka-3.1.0-1.17.jar')
    jdbc_connector_jar = os.path.join(jar_dir, 'flink-connector-jdbc-3.1.1-1.17.jar')
    mysql_driver_jar = os.path.join(jar_dir, 'mysql-connector-java-8.0.28.jar')
    
    # Add JARs to Flink environment
    jar_files = []
    for jar_file in [kafka_connector_jar, jdbc_connector_jar, mysql_driver_jar]:
        if os.path.exists(jar_file):
            jar_files.append(f"file://{jar_file}")
            logger.info(f"Added JAR: {os.path.basename(jar_file)}")
        else:
            logger.warning(f"JAR file not found: {jar_file}")
            raise FileNotFoundError(f"JAR file not found: {jar_file}")
    
    if jar_files:
        env.add_jars(*jar_files)
        logger.info(f"Total {len(jar_files)} JAR files added to Flink environment")
    else:
        logger.warning("No JAR files found. Make sure JARs are in flink_project/jar/ directory")
        logger.warning("Required JARs:")
        logger.warning("  - flink-connector-kafka-3.1.0-1.17.jar (for Kafka source)")
        logger.warning("  - flink-connector-jdbc-3.1.1-1.17.jar (for JDBC sink)")
        logger.warning("  - mysql-connector-java-8.0.28.jar (for MySQL driver)")
    
    # Create source and sink utils
    source_utils = FlinkSourceUtils()
    sink_utils = FlinkSinkUtils()
    
    # Step 1: Create Kafka source
    logger.info("Step 1: Creating Kafka source...")
    kafka_source = source_utils.create_kafka_source(
        env=env,
        topic=kafka_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group_id,
        offset="earliest"
    )
    
    # Step 2: Create data stream from Kafka
    logger.info("Step 2: Creating data stream from Kafka...")
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    )
    
    # Step 3: Parse orders using flatMap
    logger.info("Step 3: Parsing orders...")
    
    class ParseOrderFlatMap(FlatMapFunction):
        """FlatMap function to parse order JSON"""
        def flat_map(self, value):
            parsed_orders = parse_walmart_order_json_string(value)
            for order in parsed_orders:
                yield order
    
    parsed_stream = raw_stream.flat_map(ParseOrderFlatMap(), output_type=Types.PICKLED_BYTE_ARRAY())
    
    # Step 4: Convert to row data for MySQL
    logger.info("Step 4: Converting to MySQL row format...")
    
    class ConvertToRowMap(MapFunction):
        """Map function to convert dict to tuple"""
        def map(self, value):
            return convert_to_row_data(value)
    
    row_stream = parsed_stream.map(ConvertToRowMap(), output_type=Types.ROW([
        Types.LONG(), Types.LONG(), Types.STRING(), Types.LONG(), Types.SQL_TIMESTAMP(),
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.LONG(), Types.SQL_TIMESTAMP(), Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(),
        Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
        Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
        Types.STRING(), Types.INT(), Types.STRING(),
        Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()
    ]))
    
    # Step 5: Create MySQL sink
    logger.info("Step 5: Creating MySQL sink...")
    insert_sql = """
    INSERT INTO walmart_order (
        purchaseOrderId, customerOrderId, customerEmailId, orderDate, orderDate_formatted,
        shipNode_type, shipNode_name, shipNode_id, source_file, phone,
        estimatedDeliveryDate, estimatedDeliveryDate_formatted, estimatedShipDate, estimatedShipDate_formatted, methodCode,
        recipient_name, address1, address2, city, state, postalCode, country, addressType,
        lineNumber, sku, productName, product_condition, quantity, unitOfMeasurement,
        statusDate, statusDate_formatted, fulfillmentOption, shipMethod, storeId, shippingProgramType,
        chargeType, chargeName, chargeAmount, currency, taxAmount, taxName,
        orderLineStatus, statusQuantity, cancellationReason,
        shipDateTime, shipDateTime_formatted, carrierName, carrierMethodCode, trackingNumber, trackingURL
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    type_info = Types.ROW([
        Types.LONG(), Types.LONG(), Types.STRING(), Types.LONG(), Types.SQL_TIMESTAMP(),
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.LONG(), Types.SQL_TIMESTAMP(), Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(),
        Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
        Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
        Types.STRING(), Types.INT(), Types.STRING(),
        Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()
    ])
    
    mysql_sink = sink_utils.create_mysql_sink(
        url=jdbc_url,
        driver=jdbc_driver,
        username=mysql_user,
        password=mysql_password,
        table_name=mysql_table,
        sql=insert_sql,
        type_info=type_info,
        batch_size=1000,
        batch_interval_ms=2000,
        max_retries=3
    )
    
    # Step 6: Add sink to stream
    logger.info("Step 6: Connecting sink to stream...")
    row_stream.add_sink(mysql_sink)
    
    # Execute Flink job
    logger.info("=" * 100)
    logger.info("Pipeline setup complete!")
    logger.info("Executing Flink job...")
    logger.info("Pipeline: Kafka -> Parse -> MySQL")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 100)
    
    try:
        env.execute("Walmart Order Pipeline - Kafka to MySQL")
    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
    except Exception as e:
        logger.error(f"Error executing Flink job: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

