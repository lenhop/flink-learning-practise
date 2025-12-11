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

Note on Warnings:
    You may see the following warnings in logs, which are safe to ignore:
    
    1. "Could not find a compatible bson package"
       - This is a warning from PyFlink dependencies, not an error
       - Safe to ignore if you're not using MongoDB/BSON functionality
       
    2. "No module named google.cloud.bigquery_storage_v1"
       - This warning appears because PyFlink includes BigQuery connector code
       - This pipeline does NOT use BigQuery, so this warning can be safely ignored
       - Only install google-cloud-bigquery-storage if you actually use BigQuery
    
    These warnings do not affect the functionality of this Kafka -> MySQL pipeline.
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
        Build REPLACE SQL statement matching table structure
        REPLACE will delete existing record with same primary key (purchaseOrderId, sku) and insert new one
        
        Returns:
            str: REPLACE SQL statement with placeholders
        """
        return f"""
        REPLACE INTO {self.table_name} (
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
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
        Insert or replace a single record into MySQL table
        Uses REPLACE INTO which will delete existing record with same primary key (purchaseOrderId, sku) and insert new one
        
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
            
            # Execute REPLACE (will replace existing record with same primary key)
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
        """
        FlatMapFunction interface method - REQUIRED by PyFlink FlatMapFunction interface
        
        NOTE: This method name MUST be 'flat_map' (cannot be renamed) because it's required
        by the FlatMapFunction interface. Flink framework will call this method at runtime.
        
        This is DIFFERENT from:
        - Line 645: raw_stream.flat_map(...) - This is Flink DataStream API method call
        - Line 239: def flat_map(...) - This is the interface implementation (current method)
        
        The relationship:
        1. Line 645 calls Flink API: raw_stream.flat_map(ParseAndConvertFlatMap(), ...)
        2. Flink framework registers the ParseAndConvertFlatMap instance
        3. At runtime, Flink calls this method (line 239) for each data record
        
        This method delegates to parse_order_json_to_tuples for actual processing.
        
        Args:
            value: Input value from data stream (JSON string in this case)
            
        Yields:
            tuple: MySQL row format tuple for each order line
        """
        # Delegate to the actual parsing method
        for tuple_data in self.parse_order_json_to_tuples(value):
            yield tuple_data
    
    def parse_order_json_to_tuples(self, json_str):
        """
        Parse Walmart order JSON string and convert to tuples
        
        This is the actual parsing logic extracted from flat_map for better code organization.
        It parses a single order JSON string (which may contain multiple order lines) and
        converts each order line to a tuple format suitable for MySQL insertion.
        
        Args:
            json_str: JSON string of a single order or list of orders
            
        Yields:
            tuple: MySQL row format tuple (52 fields) for each order line
        """
        # Parse JSON string and convert directly to tuples
        tuples = parse_walmart_order_json_string_to_tuples(json_str)
        for tuple_data in tuples:
            yield tuple_data


class PipelineConfig:
    """Configuration management for Flink pipeline"""
    
    def __init__(self):
        """Load and initialize configuration"""
        self.config = BaseConfig().cfg
        self.kafka_config = self._load_kafka_config()
        self.mysql_config = self._load_mysql_config()
    
    def _load_kafka_config(self):
        """Load Kafka configuration"""
        kafka_config = self.config.get('kafka', {})
        return {
            'bootstrap_servers': kafka_config.get('bootstrap_servers', ['localhost:29092', 'localhost:39092']),
            'topic': 'walmart_order_raw',
            'consumer_group_id': 'flink-walmart-order-pipeline',
            # Offset 配置说明：
            # - 'earliest': 当 Consumer Group 没有已保存的 offset 时，从 topic 中最早的消息开始消费
            #   应用场景：
            #   1. 首次运行，需要处理所有历史数据
            #   2. 数据回放/重放，需要重新处理历史数据
            #   3. 数据修复，需要重新处理特定时间段的数据
            #   4. 测试环境，需要完整数据
            #   注意：如果 Consumer Group 已有已保存的 offset（通过 checkpoint 或 Kafka consumer group offset），
            #         Flink 会从已保存的 offset 继续读取，而不会从 earliest 开始
            #
            # - 'latest': 当 Consumer Group 没有已保存的 offset 时，从 topic 中最新的消息开始消费（只处理新消息）
            #   应用场景：
            #   1. 生产环境持续运行，只关心新产生的数据
            #   2. 实时流处理，不需要历史数据
            #   3. 首次运行时跳过历史数据，只处理启动后的新数据
            #   注意：如果 Consumer Group 已有已保存的 offset，Flink 会从已保存的 offset 继续读取
            #
            # 当前配置为 'latest'，适合生产环境持续运行，只处理新产生的数据
            # 如果历史数据已处理完成，使用 'latest' 可以避免重复处理
            # 如果需要重新处理历史数据，可以改为 'earliest'
            'offset': 'latest'
        }
    
    def _load_mysql_config(self):
        """Load MySQL configuration"""
        mysql_config = self.config.get('mysql', {})
        return {
            'host': mysql_config.get('host', 'localhost'),
            'port': mysql_config.get('port', 3306),
            'user': mysql_config.get('user', 'root'),
            'password': mysql_config.get('password', 'root123456'),
            'database': 'ods',
            'table_name': 'walmart_order'
        }
    
    def log_configuration(self, mysql_sink):
        """Log pipeline configuration"""
        logger.info("Configuration:")
        logger.info(f"  Kafka Topic: {self.kafka_config['topic']}")
        logger.info(f"  Bootstrap Servers: {self.kafka_config['bootstrap_servers']}")
        logger.info(f"  MySQL: {mysql_sink.get_connection_info()}")


class FlinkEnvironmentSetup:
    """Flink execution environment setup and configuration"""
    
    def __init__(self, flink_project_path):
        """
        Initialize Flink environment setup
        
        Args:
            flink_project_path: Path to flink_project directory
        """
        self.flink_project_path = flink_project_path
        self.jar_files = []
        self.env = None
        # Setup checkpoint directory
        self.checkpoint_dir = os.path.join(flink_project_path, 'checkpoints', 'walmart_order_pipeline')
    
    def prepare_jar_files(self):
        """Prepare and validate JAR files for Flink"""
        jar_dir = os.path.join(self.flink_project_path, 'jar')
        kafka_connector_jar = os.path.join(jar_dir, 'flink-connector-kafka-3.3.0-1.20.jar')
        kafka_clients_jar = os.path.join(jar_dir, 'kafka-clients-3.4.1.jar')
        
        for jar_file in [kafka_connector_jar, kafka_clients_jar]:
            if os.path.exists(jar_file):
                self.jar_files.append(f"file://{jar_file}")
                logger.info(f"Found JAR: {os.path.basename(jar_file)}")
            else:
                logger.error(f"JAR file not found: {jar_file}")
                raise FileNotFoundError(f"JAR file not found: {jar_file}")
        
        logger.info(f"Total {len(self.jar_files)} JAR files to be loaded")
    
    def _check_flink_cluster(self):
        """Check if Flink cluster is accessible"""
        try:
            # Create a TCP socket to check connectivity to the Flink JobManager
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Try to connect to the Flink JobManager at localhost:6123 (default port for standalone Flink cluster)
            result = sock.connect_ex(('localhost', 6123))
            sock.close()
            if result == 0:
                return True
            else:
                return False
        except Exception as e:
            logger.warning(f"Could not check Flink cluster: {e}")
            return False
    
    def _convert_to_absolute_paths(self):
        """Convert JAR paths to absolute paths"""
        absolute_jar_files = []
        for jar_path in self.jar_files:
            if jar_path.startswith("file://"):
                abs_path = jar_path.replace("file://", "")
                absolute_jar_files.append(f"file://{os.path.abspath(abs_path)}")
            else:
                absolute_jar_files.append(f"file://{os.path.abspath(jar_path)}")
        return absolute_jar_files
    
    def create_environment(self):
        """Create and configure Flink execution environment"""
        try:
            logger.info("Creating Flink execution environment...")
            
            absolute_jar_files = self._convert_to_absolute_paths()
            flink_config = Configuration()
            
            # Check for remote Flink cluster
            if self._check_flink_cluster():
                logger.info("Flink cluster detected, using remote mode")
                flink_config.set_string("jobmanager.address", "localhost")
                flink_config.set_string("jobmanager.rpc.port", "6123")
            else:
                logger.info("Flink cluster not accessible, using local mode")
            
            # Configure JARs
            if absolute_jar_files:
                classpaths_str = ';'.join(absolute_jar_files)
                flink_config.set_string("pipeline.classpaths", classpaths_str)
                logger.info(f"Configured pipeline.classpaths with {len(absolute_jar_files)} JAR files")
                for jar in absolute_jar_files:
                    logger.info(f"  - {jar}")
            
            # Create environment
            logger.info("Calling StreamExecutionEnvironment.get_execution_environment()...")
            self.env = StreamExecutionEnvironment.get_execution_environment(flink_config)
            logger.info("✓ StreamExecutionEnvironment created successfully")
            
            # Also add JARs via add_jars() for compatibility
            if absolute_jar_files:
                logger.info("Adding JARs via add_jars() method...")
                self.env.add_jars(*absolute_jar_files)
                logger.info(f"✓ Also added JARs via add_jars() method")
            
            logger.info("✓ Successfully created Flink execution environment")
            
        except Exception as e:
            logger.error(f"Failed to create Flink execution environment: {e}")
            logger.error("Trying fallback without explicit config...")
            self._create_environment_fallback()
    
    def _create_environment_fallback(self):
        """
        Fallback method to create Flink environment without explicit config
        
        This is a fault-tolerant mechanism (Fallback Pattern) that is called when
        create_environment() fails. It provides a simplified, more compatible way
        to create the Flink execution environment.
        
        Design Purpose:
        - When create_environment() fails (e.g., due to Configuration object issues,
          unsupported pipeline.classpaths, or Flink version compatibility problems),
          this method provides an alternative approach.
        - Uses default configuration (no Configuration object) which is more
          compatible across different Flink versions.
        - Only uses add_jars() method to add JAR files, avoiding potential
          configuration conflicts.
        
        Differences from create_environment():
        1. No Configuration object: Uses get_execution_environment() without parameters
        2. No cluster check: Relies on Flink's automatic detection
        3. No pipeline.classpaths: Only uses add_jars() method
        4. Simpler approach: More compatible but less configurable
        
        Execution Flow:
        create_environment() → fails → _create_environment_fallback() → success/failure
        
        If this method also fails, the exception will be raised to the caller.
        """
        try:
            logger.info("Creating environment with fallback method (no explicit config)...")
            # Use default configuration (no Configuration object) for better compatibility
            self.env = StreamExecutionEnvironment.get_execution_environment()
            
            # Use absolute paths in fallback as well for consistency
            if self.jar_files:
                absolute_jar_files = self._convert_to_absolute_paths()
                logger.info(f"Adding {len(absolute_jar_files)} JAR files via add_jars() in fallback mode...")
                # Only use add_jars() method, avoiding pipeline.classpaths configuration
                self.env.add_jars(*absolute_jar_files)
                logger.info("✓ JAR files added in fallback mode")
            
            logger.info("✓ Created environment with fallback method")
        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")
            traceback.print_exc()
            raise
    
    def configure_parallelism(self, parallelism=1):
        """Configure Flink parallelism"""
        if self.env is None:
            raise RuntimeError("Flink environment not created. Call create_environment() first.")
        
        logger.info(f"Setting parallelism to {parallelism}...")
        self.env.set_parallelism(parallelism)
        logger.info(f"✓ Parallelism set to {parallelism}")
    
    def configure_checkpointing(self, interval_ms=60000):
        """
        Configure Flink checkpointing with persistent storage
        
        Args:
            interval_ms: Checkpoint interval in milliseconds (default: 60000ms = 1 minute)
        """
        if self.env is None:
            logger.warning("Flink environment not created, skipping checkpointing configuration")
            return
        
        try:
            # Create checkpoint directory if it doesn't exist
            if not os.path.exists(self.checkpoint_dir):
                os.makedirs(self.checkpoint_dir, exist_ok=True)
                logger.info(f"Created checkpoint directory: {self.checkpoint_dir}")
            
            logger.info("Enabling checkpointing...")
            logger.info(f"  Checkpoint directory: {self.checkpoint_dir}")
            logger.info(f"  Checkpoint interval: {interval_ms/1000}s")
            
            # Enable checkpointing
            self.env.enable_checkpointing(interval_ms)
            
            # Try to configure checkpoint storage if API is available
            try:
                checkpoint_config = self.env.get_checkpoint_config()
                # Set checkpoint storage to persistent file system
                # This ensures checkpoints survive job restarts
                checkpoint_config.set_checkpoint_storage(f"file://{self.checkpoint_dir}")
                logger.info(f"✓ Checkpoint storage configured: file://{self.checkpoint_dir}")
            except (AttributeError, Exception) as e:
                # If checkpoint storage API is not available, log warning
                logger.warning(f"Could not configure checkpoint storage: {e}")
                logger.warning("  Checkpoints may be stored in temporary location")
                logger.warning("  Consider using Flink cluster mode for persistent checkpoints")
            
            logger.info(f"✓ Checkpointing enabled (interval: {interval_ms/1000}s)")
        except Exception as e:
            logger.warning(f"Could not enable checkpointing: {e}")
            logger.warning("  Checkpointing may not be persistent across restarts")
    
    def get_environment(self):
        """Get Flink execution environment
        
        Returns:
            StreamExecutionEnvironment: Flink execution environment
            
        Raises:
            RuntimeError: If environment has not been created
        """
        if self.env is None:
            raise RuntimeError("Flink environment not created. Call create_environment() first.")
        return self.env


class FlinkPipelineBuilder:
    """Build Flink data processing pipeline"""
    
    def __init__(self, env, config, mysql_sink):
        """
        Initialize pipeline builder
        
        Args:
            env: Flink StreamExecutionEnvironment
            config: PipelineConfig instance
            mysql_sink: WalmartOrderMySQLSink instance
        """
        self.env = env
        self.config = config
        self.mysql_sink = mysql_sink
        self.source_utils = FlinkSourceUtils()
    
    def create_kafka_source(self):
        """Create Kafka source"""
        logger.info("Step 1: Creating Kafka source...")
        logger.info(f"  Topic: {self.config.kafka_config['topic']}")
        logger.info(f"  Bootstrap Servers: {self.config.kafka_config['bootstrap_servers']}")
        logger.info(f"  Consumer Group: {self.config.kafka_config['consumer_group_id']}")
        logger.info(f"  Offset: {self.config.kafka_config['offset']}")
        try:
            kafka_source = self.source_utils.create_kafka_source(
                env=self.env,
                topic=self.config.kafka_config['topic'],
                bootstrap_servers=self.config.kafka_config['bootstrap_servers'],
                group_id=self.config.kafka_config['consumer_group_id'],
                offset=self.config.kafka_config['offset']
            )
            logger.info("✓ Kafka source created successfully")
            return kafka_source
        except Exception as e:
            logger.error(f"Failed to create Kafka source: {e}")
            traceback.print_exc()
            raise
    
    def create_data_stream(self, kafka_source):
        """Create data stream from Kafka source"""
        logger.info("Step 2: Creating data stream from Kafka...")
        try:
            raw_stream = self.env.from_source(
                kafka_source, 
                WatermarkStrategy.no_watermarks(), 
                "Kafka Source"
            )
            logger.info("✓ Data stream created successfully")
            return raw_stream
        except Exception as e:
            logger.error(f"Failed to create data stream: {e}")
            traceback.print_exc()
            raise
    
    def _get_row_type_info(self):
        """Get Flink Types.ROW definition matching SQL table structure"""
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
    
    def parse_and_convert(self, raw_stream):
        """Parse orders and convert to MySQL row format"""
        logger.info("Step 3: Parsing orders and converting to MySQL row format...")
        try:
            logger.info("  Creating ParseAndConvertFlatMap function...")
            # NOTE: This flat_map is Flink DataStream API method (different from line 239)
            # Line 239: def flat_map(...) - Interface implementation method (required by FlatMapFunction)
            # Line 645: raw_stream.flat_map(...) - Flink API method call (registers the transformation)
            # At runtime, Flink will call the flat_map method (line 239) of ParseAndConvertFlatMap instance
            row_stream = raw_stream.flat_map(
                ParseAndConvertFlatMap(), 
                output_type=self._get_row_type_info()
            )
            logger.info("✓ Parse and convert transformation configured successfully")
            return row_stream
        except Exception as e:
            logger.error(f"Failed to configure parse and convert: {e}")
            traceback.print_exc()
            raise
    
    def connect_mysql_sink(self, row_stream):
        """Connect MySQL sink to data stream"""
        logger.info("Step 4: Creating MySQL sink function (using pymysql)...")
        mysql_sink_func = self.mysql_sink.get_sink_function()
        logger.info("✓ MySQL sink function created successfully (using pymysql)")
        
        logger.info("Step 5: Connecting MySQL sink to stream...")
        try:
            row_stream.map(mysql_sink_func)
            logger.info("✓ MySQL sink added to data stream (using map)")
        except Exception as e:
            logger.error(f"Failed to add MySQL sink: {e}")
            traceback.print_exc()
            raise


class FlinkPipelineExecutor:
    """Execute Flink pipeline"""
    
    def __init__(self, env, job_name="Walmart Order Pipeline - Kafka to MySQL"):
        """
        Initialize pipeline executor
        
        Args:
            env: Flink StreamExecutionEnvironment
            job_name: Name of the Flink job
        """
        self.env = env
        self.job_name = job_name
    
    def execute(self):
        """Execute Flink pipeline"""
        logger.info("=" * 100)
        logger.info("Pipeline setup complete!")
        logger.info("Executing Flink job...")
        logger.info("Pipeline: Kafka -> Parse -> MySQL")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 100)
        
        try:
            self.env.execute(self.job_name)
        except KeyboardInterrupt:
            logger.info("Job interrupted by user")
        except Exception as e:
            logger.error(f"Error executing Flink job: {str(e)}")
            traceback.print_exc()
            raise


def main():
    """Main function to create and execute complete Flink pipeline"""
    
    # Log pipeline information
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
    
    # Step 1: Load configuration
    config = PipelineConfig()
    
    # Step 2: Create MySQL sink instance
    mysql_sink = WalmartOrderMySQLSink(
        host=config.mysql_config['host'],
        port=config.mysql_config['port'],
        database=config.mysql_config['database'],
        user=config.mysql_config['user'],
        password=config.mysql_config['password'],
        table_name=config.mysql_config['table_name']
    )
    
    # Log configuration
    # 这一行的作用是：打印当前管道配置，包括Kafka和MySQL的连接信息，便于在日志中查看和调试配置信息。
    config.log_configuration(mysql_sink)
    logger.info("=" * 100)
    
    # Step 3: Setup Flink environment
    logger.info("=" * 100)
    logger.info("Step 3: Setting up Flink environment...")
    env_setup = FlinkEnvironmentSetup(flink_project_path)
    logger.info("  Preparing JAR files...")
    env_setup.prepare_jar_files()
    logger.info("  Creating Flink execution environment...")
    env_setup.create_environment()
    # 配置 Flink 并行度为 1，保证数据处理的有序性（单并发），便于调试和测试
    env_setup.configure_parallelism(parallelism=1)
    # 配置 Flink 的 checkpointing（检查点），以支持容错能力。
    # 这里设置 checkpoint 间隔为 60000 毫秒（即 1 分钟），确保任务在失败时能够从最近的 state 恢复，提升数据一致性和可靠性。
    env_setup.configure_checkpointing(interval_ms=60000)
    env = env_setup.get_environment()
    logger.info("✓ Flink environment setup completed")
    logger.info("=" * 100)
    
    # Step 4: Build pipeline
    logger.info("Step 4: Building Flink pipeline...")
    pipeline_builder = FlinkPipelineBuilder(env, config, mysql_sink)
    kafka_source = pipeline_builder.create_kafka_source()
    # 第1步：创建数据流，raw_stream 从 Kafka 源（kafka_source）获取原始的订单数据流（字符串，每条记录为一条原始 JSON）
    raw_stream = pipeline_builder.create_data_stream(kafka_source)
    # 第2步：解析和转换，将订单原始 JSON 数据通过 flatMap 转换为与 MySQL 表字段结构一致的 Row 或 tuple，便于后续直接入库
    row_stream = pipeline_builder.parse_and_convert(raw_stream)
    pipeline_builder.connect_mysql_sink(row_stream)
    logger.info("✓ Pipeline building completed")
    logger.info("=" * 100)
    
    # Step 5: Execute pipeline
    logger.info("Step 5: Executing Flink pipeline...")
    executor = FlinkPipelineExecutor(env)
    executor.execute()


if __name__ == "__main__":
    main()

