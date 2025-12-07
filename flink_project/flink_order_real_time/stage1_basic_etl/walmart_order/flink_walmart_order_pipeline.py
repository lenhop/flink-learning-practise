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

# Standard library imports
import logging
import os
import socket
import sys
import traceback

# Third-party imports
import pymysql
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction

# Add flink_project directory to path
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

# Project imports (after path setup)
from config.config import BaseConfig

# Import Flink utils
utils_dir = os.path.join(flink_project_path, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

import flink_source_utils
FlinkSourceUtils = flink_source_utils.FlinkSourceUtils

# Import parse and convert functions
from parse_walmart_order import parse_walmart_order_json_string_to_tuples

# Configure logger
# Use flink_project root directory for logs
log_dir = os.path.join(flink_project_path, 'logs')
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


class WalmartOrderMySQLSink:
    """
    MySQL Sink class for Walmart order data
    Handles all MySQL operations including connection and data insertion
    """
    
    def __init__(self, host, port, database, user, password, table_name='walmart_order'):
        """
        Initialize MySQL sink with connection parameters
        
        Args:
            host: MySQL host address
            port: MySQL port number
            database: Database name
            user: MySQL username
            password: MySQL password
            table_name: Target table name (default: 'walmart_order')
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.insert_sql = self._build_insert_sql()
    
    def _build_insert_sql(self):
        """
        Build INSERT SQL statement matching table structure
        
        Returns:
            str: INSERT SQL statement with placeholders
        """
        return f"""
        INSERT INTO {self.table_name} (
            purchaseOrderId, customerOrderId, customerEmailId, orderDate, orderDate_formatted,
            shipNode_type, shipNode_name, shipNode_id, source_file, phone,
            estimatedDeliveryDate, estimatedDeliveryDate_formatted, estimatedShipDate, estimatedShipDate_formatted, methodCode,
            recipient_name, address1, address2, city, state, postalCode, country, addressType,
            lineNumber, sku, productName, product_condition, quantity, unitOfMeasurement,
            statusDate, statusDate_formatted, fulfillmentOption, shipMethod, storeId, shippingProgramType,
            chargeType, chargeName, chargeAmount, currency, taxAmount, taxName,
            orderLineStatus, statusQuantity, cancellationReason,
            shipDateTime, shipDateTime_formatted, carrierName, carrierMethodCode, trackingNumber, trackingURL
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def _connect(self):
        """
        Create MySQL connection using pymysql
        
        Returns:
            pymysql.Connection: MySQL connection object
        """
        return pymysql.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            charset='utf8mb4',
            autocommit=True
        )
    
    def _convert_to_tuple(self, value):
        """
        Convert value to tuple format for MySQL insertion
        
        Args:
            value: Input value (can be tuple, Row object, list, etc.)
            
        Returns:
            tuple: Tuple of values for SQL insertion
        """
        if isinstance(value, tuple):
            return value
        elif hasattr(value, '_values'):  # Row object
            return tuple(value._values)
        elif isinstance(value, list):
            return tuple(value)
        else:
            return tuple(value) if hasattr(value, '__iter__') and not isinstance(value, str) else (value,)
    
    def insert_record(self, value):
        """
        Insert a single record into MySQL table
        
        Args:
            value: Record data (tuple, Row object, or list)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert value to tuple
            params = self._convert_to_tuple(value)
            
            # Connect to MySQL
            connection = self._connect()
            cursor = connection.cursor()
            
            # Execute INSERT
            cursor.execute(self.insert_sql, params)
            connection.commit()
            
            # Close connection
            cursor.close()
            connection.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error inserting record to MySQL: {e}")
            logger.error(f"  Value type: {type(value)}, Value preview: {str(value)[:100] if value else 'None'}")
            return False
    
    def get_sink_function(self):
        """
        Get MySQL sink function for Flink map operation
        
        Returns:
            callable: Function that can be used in Flink map operation
        """
        def sink_func(value):
            """MySQL sink function - writes data to MySQL table"""
            self.insert_record(value)
        
        return sink_func
    
    def get_connection_info(self):
        """
        Get MySQL connection information string
        
        Returns:
            str: Connection info string for logging
        """
        return f"{self.host}:{self.port}/{self.database}.{self.table_name}"


class ParseAndConvertFlatMap(FlatMapFunction):
    """FlatMap function to parse order JSON and convert directly to tuples"""
    def flat_map(self, value):
        # Parse JSON string and convert directly to tuples
        tuples = parse_walmart_order_json_string_to_tuples(value)
        for tuple_data in tuples:
            yield tuple_data


def main():
    """Main function to create and execute complete Flink pipeline"""
    
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
    
    logger.info("=" * 100)
    logger.info("Flink Walmart Order Complete Pipeline - Starting")
    logger.info("=" * 100)
    logger.info("Pipeline Design: Single Flink Job (no additional Kafka topics)")
    logger.info("")
    logger.info("Data Flow:")
    logger.info("  1. Kafka Source -> Read from 'walmart_order_raw'")
    logger.info("  2. Parse & Convert -> Extract order data and convert to MySQL row format (flatMap)")
    logger.info("  3. MySQL Sink -> Write to ods.walmart_order")
    logger.info("")
    # Create MySQL sink instance
    mysql_sink = WalmartOrderMySQLSink(
        host=mysql_host,
        port=mysql_port,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password,
        table_name='walmart_order'
    )
    
    logger.info("Configuration:")
    logger.info(f"  Kafka Topic: {kafka_topic}")
    logger.info(f"  Bootstrap Servers: {bootstrap_servers}")
    logger.info(f"  MySQL: {mysql_sink.get_connection_info()}")
    logger.info("=" * 100)
    
    # Prepare JAR files BEFORE creating execution environment
    # JARs must be loaded before creating Kafka connector
    # Using Flink 1.20 compatible versions (Flink cluster is 1.20.3)
    # Note: MySQL connection uses pymysql (Python library), no JDBC connector JAR needed
    jar_dir = os.path.join(flink_project_path, 'jar')
    kafka_connector_jar = os.path.join(jar_dir, 'flink-connector-kafka-3.3.0-1.20.jar')
    kafka_clients_jar = os.path.join(jar_dir, 'kafka-clients-3.4.1.jar')
    
    # Collect JAR file paths
    jar_files = []
    for jar_file in [kafka_connector_jar, kafka_clients_jar]:
        if os.path.exists(jar_file):
            jar_files.append(f"file://{jar_file}")
            logger.info(f"Found JAR: {os.path.basename(jar_file)}")
        else:
            logger.error(f"JAR file not found: {jar_file}")
            raise FileNotFoundError(f"JAR file not found: {jar_file}")
    
    logger.info(f"Total {len(jar_files)} JAR files to be loaded")
    
    # Create Flink execution environment
    # PyFlink uses local Java gateway, need to configure JARs via pipeline.classpaths
    try:
        logger.info("Creating Flink execution environment...")
        
        # Convert JAR paths to absolute paths
        absolute_jar_files = []
        for jar_path in jar_files:
            if jar_path.startswith("file://"):
                abs_path = jar_path.replace("file://", "")
                absolute_jar_files.append(f"file://{os.path.abspath(abs_path)}")
            else:
                absolute_jar_files.append(f"file://{os.path.abspath(jar_path)}")
        
        # Configure remote cluster connection
        # JARs are already in Flink container's /opt/flink/lib directory
        config = Configuration()
        
        # Try to connect to remote Flink cluster
        # Check if Flink cluster is accessible
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 6123))
            sock.close()
            if result == 0:
                logger.info("Flink cluster detected, using remote mode")
                config.set_string("jobmanager.address", "localhost")
                config.set_string("jobmanager.rpc.port", "6123")
            else:
                logger.info("Flink cluster not accessible, using local mode")
        except Exception as e:
            logger.warning(f"Could not check Flink cluster: {e}, using local mode")
        
        # Still configure JARs for local mode fallback
        if absolute_jar_files:
            classpaths_str = ';'.join(absolute_jar_files)
            config.set_string("pipeline.classpaths", classpaths_str)
            logger.info(f"Configured pipeline.classpaths with {len(absolute_jar_files)} JAR files")
            for jar in absolute_jar_files:
                logger.info(f"  - {jar}")
        
        # Create environment with configuration
        env = StreamExecutionEnvironment.get_execution_environment(config)
        
        # Also add JARs via add_jars() for compatibility
        if absolute_jar_files:
            env.add_jars(*absolute_jar_files)
            logger.info(f"✓ Also added JARs via add_jars() method")
        
        logger.info("✓ Successfully created Flink execution environment")
    except Exception as e:
        logger.error(f"Failed to create Flink execution environment: {e}")
        logger.error("Trying fallback without explicit config...")
        # Fallback: try without config
        try:
            env = StreamExecutionEnvironment.get_execution_environment()
            if jar_files:
                env.add_jars(*jar_files)
            logger.info("✓ Created environment with fallback method")
        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")
            traceback.print_exc()
            raise
    
    env.set_parallelism(1)
    
    # Configure execution mode
    try:
        # Set checkpointing interval (optional, for fault tolerance)
        env.enable_checkpointing(60000)  # 60 seconds
        logger.info("Checkpointing enabled (interval: 60s)")
    except Exception as e:
        logger.warning(f"Could not enable checkpointing: {e}")
    
    # Create source utils
    source_utils = FlinkSourceUtils()
    
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
    raw_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    
    # Step 3: Parse orders and convert to tuples using flatMap (merged step)
    logger.info("Step 3: Parsing orders and converting to MySQL row format...")
    
    # Define Flink Types.ROW matching SQL table structure exactly
    # Type mapping: BIGINT->LONG, INT->INT, DECIMAL(10,2)->DOUBLE, VARCHAR/TEXT->STRING, TIMESTAMP->SQL_TIMESTAMP
    row_stream = raw_stream.flat_map(ParseAndConvertFlatMap(), output_type=Types.ROW([
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
        Types.STRING()          # trackingURL VARCHAR(500)
    ]))
    
    # Step 4: Create MySQL sink function using MySQL sink class
    # This approach is more reliable than standard JDBC Sink due to PyFlink compatibility issues
    logger.info("Step 4: Creating MySQL sink function (using pymysql)...")
    
    # Get sink function from MySQL sink instance
    mysql_sink_func = mysql_sink.get_sink_function()
    
    logger.info("✓ MySQL sink function created successfully (using pymysql)")
    
    # Step 5: Add sink to stream using map operation
    logger.info("Step 5: Connecting MySQL sink to stream...")
    try:
        # Use map operation with the sink function
        # The function performs side effects (writing to MySQL) and returns None
        row_stream.map(mysql_sink_func)
        logger.info("✓ MySQL sink added to data stream (using map)")
    except Exception as e:
        logger.error(f"Failed to add MySQL sink: {e}")
        traceback.print_exc()
        return
    
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
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

