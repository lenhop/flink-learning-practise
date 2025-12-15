#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test case for Flink JDBC connection to MySQL.

This test:
1. Creates a test table in MySQL testdb schema
2. Tests Flink JDBC connection using JAR files
3. Loads test data into the table
"""

import logging
import os
import sys
import traceback

# Add project path to sys.path (go up 3 levels: tests -> walmart_order -> stage1_basic_etl -> flink_order_real_time)
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)

# Add stage1_basic_etl parent directory (go up 2 levels: tests -> walmart_order -> stage1_basic_etl)
stage1_parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if stage1_parent_path not in sys.path:
    sys.path.insert(0, stage1_parent_path)

# Add flink_project root (go up 4 levels: tests -> walmart_order -> stage1_basic_etl -> flink_order_real_time -> flink_project)
flink_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
if flink_project_root not in sys.path:
    sys.path.insert(0, flink_project_root)

from config.config import BaseConfig
from stage1_basic_etl.walmart_order.flink2_add_jar_to_flink import FlinkJarManager
from stage1_basic_etl.walmart_order.flink3_add_parameter_to_flink import FlinkParameterConfigurator

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

import pymysql
from datetime import datetime

# Logging configuration
log_dir = os.path.join(flink_project_path, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'test_flink_jdbc_mysql.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def create_test_table():
    """Create test table in MySQL testdb schema."""
    cfg = BaseConfig().cfg
    mysql_cfg = cfg.get('mysql', {})
    
    host = mysql_cfg.get('host', 'localhost')
    port = mysql_cfg.get('port', 3306)
    user = mysql_cfg.get('user', 'root')
    password = mysql_cfg.get('password', 'root123456')
    
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # Create testdb database if not exists
        cursor.execute("CREATE DATABASE IF NOT EXISTS testdb")
        logger.info("✓ Database testdb created or already exists")
        
        # Use testdb
        cursor.execute("USE testdb")
        
        # Create test table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS flink_test_table (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) NOT NULL,
            value INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        cursor.execute(create_table_sql)
        logger.info("✓ Test table flink_test_table created or already exists")
        
        # Clear existing test data
        cursor.execute("TRUNCATE TABLE flink_test_table")
        logger.info("✓ Cleared existing test data")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"Failed to create test table: {e}")
        traceback.print_exc()
        return False


def get_test_row_type_info():
    """Define row type for test table."""
    return Types.ROW([
        Types.STRING(),         # name VARCHAR(100)
        Types.INT(),            # value INT
        Types.SQL_TIMESTAMP()  # created_at TIMESTAMP
    ])


def generate_test_data():
    """Generate test data rows."""
    test_data = [
        ("test_item_1", 100, datetime.now()),
        ("test_item_2", 200, datetime.now()),
        ("test_item_3", 300, datetime.now()),
        ("test_item_4", 400, datetime.now()),
        ("test_item_5", 500, datetime.now()),
    ]
    return test_data


def test_flink_jdbc_connection():
    """Test Flink JDBC connection to MySQL."""
    try:
        logger.info("=" * 100)
        logger.info("Flink JDBC MySQL Connection Test - Starting")
        logger.info("=" * 100)
        
        # Step 1: Create test table
        logger.info("Step 1: Creating test table in MySQL testdb...")
        if not create_test_table():
            logger.error("Failed to create test table")
            return False
        logger.info("✓ Step 1 completed")
        
        # Step 2: Create Flink environment
        logger.info("Step 2: Creating Flink execution environment...")
        flink_config = Configuration()
        env = StreamExecutionEnvironment.get_execution_environment(flink_config)
        logger.info("✓ Step 2 completed")
        
        # Step 3: Add JAR files from 1.17 directory
        logger.info("Step 3: Loading JAR files from jar/1.17 directory...")
        # JAR directory is at flink_project root level (go up 4 levels from tests)
        jar_dir = os.path.join(flink_project_root, 'jar')
        jar_17_dir = os.path.join(jar_dir, '1.17')
        
        logger.info(f"  JAR directory: {jar_dir}")
        logger.info(f"  Flink 1.17 JAR directory: {jar_17_dir}")
        
        # Add JDBC JAR from 1.17 directory
        jdbc_jar = os.path.join(jar_17_dir, 'flink-connector-jdbc-3.1.1-1.17.jar')
        # MySQL connector is in jar root directory (not in 1.17 subdirectory)
        mysql_jar = os.path.join(jar_dir, 'mysql-connector-java-8.0.28.jar')
        
        if os.path.exists(jdbc_jar):
            env.add_jars(f"file://{os.path.abspath(jdbc_jar)}")
            logger.info(f"✓ Added JDBC JAR from 1.17: {os.path.basename(jdbc_jar)}")
        else:
            logger.error(f"✗ JDBC JAR missing: {jdbc_jar}")
            return False
            
        if os.path.exists(mysql_jar):
            env.add_jars(f"file://{os.path.abspath(mysql_jar)}")
            logger.info(f"✓ Added MySQL JAR: {os.path.basename(mysql_jar)}")
        else:
            logger.error(f"✗ MySQL JAR missing: {mysql_jar}")
            return False
        
        logger.info("✓ Step 3 completed")
        
        # Step 4: Configure parameters
        logger.info("Step 4: Configuring Flink parameters...")
        FlinkParameterConfigurator.configure_parallelism(env, parallelism=1, logger=logger)
        checkpoint_dir = os.path.join(flink_project_path, 'checkpoints', 'test_flink_jdbc')
        FlinkParameterConfigurator.configure_checkpointing(env, checkpoint_dir=checkpoint_dir, interval_ms=10000, logger=logger)
        logger.info("✓ Step 4 completed")
        
        # Step 5: Create test data stream
        logger.info("Step 5: Creating test data stream...")
        test_data = generate_test_data()
        
        # Create data stream from collection - convert tuples to Row objects first
        row_data = [Row(name, value, created_at) for name, value, created_at in test_data]
        
        # Use from_collection to create data stream
        data_stream = env.from_collection(collection=row_data, type_info=get_test_row_type_info())
        logger.info(f"✓ Step 5 completed: Created data stream with {len(test_data)} test records")
        
        # Step 6: Build JDBC sink
        logger.info("Step 6: Building JDBC sink...")
        cfg = BaseConfig().cfg
        mysql_cfg = cfg.get('mysql', {})
        
        host = mysql_cfg.get('host', 'localhost')
        port = mysql_cfg.get('port', 3306)
        user = mysql_cfg.get('user', 'root')
        password = mysql_cfg.get('password', 'root123456')
        
        # Data stream already contains Row objects, no need to convert
        row_stream = data_stream
        
        # JDBC connection options
        jdbc_conn_opts = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
            .with_driver_name("com.mysql.cj.jdbc.Driver") \
            .with_url(f"jdbc:mysql://{host}:{port}/testdb?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true") \
            .with_user_name(user) \
            .with_password(password) \
            .build()
        
        # JDBC execution options
        jdbc_exec_opts = JdbcExecutionOptions.builder() \
            .with_batch_size(10) \
            .with_batch_interval_ms(1000) \
            .with_max_retries(3) \
            .build()
        
        # Insert SQL (note: id is AUTO_INCREMENT, so we don't insert it)
        insert_sql = """INSERT INTO flink_test_table (name, value, created_at) VALUES (?, ?, ?)"""
        
        jdbc_sink = JdbcSink.sink(
            insert_sql,
            get_test_row_type_info(),
            jdbc_conn_opts,
            jdbc_exec_opts
        )
        
        row_stream.add_sink(jdbc_sink)
        logger.info("✓ Step 6 completed: JDBC sink configured")
        
        # Step 7: Execute Flink job
        logger.info("Step 7: Executing Flink job...")
        logger.info("=" * 100)
        logger.info("Starting Flink job execution...")
        logger.info("=" * 100)
        
        env.execute("Flink JDBC MySQL Connection Test")
        
        logger.info("✓ Step 7 completed: Flink job executed")
        
        # Step 8: Verify data in MySQL
        logger.info("Step 8: Verifying data in MySQL...")
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database='testdb',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM flink_test_table")
        count = cursor.fetchone()[0]
        cursor.execute("SELECT * FROM flink_test_table ORDER BY id")
        rows = cursor.fetchall()
        
        logger.info(f"✓ Found {count} rows in test table")
        for row in rows:
            logger.info(f"  Row: {row}")
        
        cursor.close()
        conn.close()
        
        if count == len(test_data):
            logger.info("=" * 100)
            logger.info("✓ TEST PASSED: All data successfully loaded into MySQL")
            logger.info("=" * 100)
            return True
        else:
            logger.error("=" * 100)
            logger.error(f"✗ TEST FAILED: Expected {len(test_data)} rows, got {count}")
            logger.error("=" * 100)
            return False
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Test failed: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_flink_jdbc_connection()
    sys.exit(0 if success else 1)
