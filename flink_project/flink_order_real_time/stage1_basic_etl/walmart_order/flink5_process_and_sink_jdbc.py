#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 5: Process stream and sink to target using Flink JDBC (parse -> transform -> MySQL).

参考 flink_walmart_order_pipeline.py 的处理与写出部分，封装为类：
- ParseAndConvertFlatMap: 将原始 JSON 字符串解析为 MySQL 行 tuple
- WalmartOrderJDBCSinkBuilder: 构建 Flink JDBC sink（整合自 flink5_build_mysql_sink_jdbc.py）
- FlinkProcessAndSinkJDBC: 提供 parse_and_convert、connect_jdbc_sink 两步

Usage:
    processor = FlinkProcessAndSinkJDBC(env, logger)
    row_stream = processor.parse_and_convert(raw_stream)
    processor.connect_jdbc_sink(row_stream, mysql_config)
"""

import logging
import os
import sys

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from pyflink.common import Types, Row
from pyflink.datastream.functions import FlatMapFunction
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

from stage1_basic_etl.walmart_order.flink5_parse_walmart_order import parse_walmart_order_json_string_to_tuples

# 简单 logger（可被外部传入替换）
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
default_logger = logging.getLogger(__name__)


class WalmartOrderJDBCSinkBuilder:
    """Builder to create Flink JDBC sink for Walmart order data."""

    def __init__(self, logger=None):
        self.logger = logger or default_logger

    def get_row_type_info(self):
        """
        Get the row type info matching MySQL table structure.
        This is a public method to allow external access to row type definition.
        """
        return self._get_row_type_info()
    
    def _get_row_type_info(self):
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

    def _build_insert_sql(self, table_name):
        """Build REPLACE SQL matching table structure."""
        return f"""REPLACE INTO {table_name} (
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

    def build(self, host, port, database, user, password, table_name='walmart_order', 
              batch_size=100, batch_interval_ms=5000, max_retries=3):
        """
        Build Flink JDBC sink for MySQL.
        
        Args:
            host: MySQL host
            port: MySQL port
            database: MySQL database name
            user: MySQL username
            password: MySQL password
            table_name: Target table name
            batch_size: JDBC batch size
            batch_interval_ms: Batch interval in milliseconds
            max_retries: Maximum retry attempts
            
        Returns:
            Flink JDBC sink object
        """
        try:
            # JDBC connection options
            # Add rewriteBatchedStatements=true for better batch insert performance
            jdbc_conn_opts = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
                .with_driver_name("com.mysql.cj.jdbc.Driver") \
                .with_url(f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true") \
                .with_user_name(user) \
                .with_password(password) \
                .build()
            
            # JDBC execution options
            jdbc_exec_opts = JdbcExecutionOptions.builder() \
                .with_batch_size(batch_size) \
                .with_batch_interval_ms(batch_interval_ms) \
                .with_max_retries(max_retries) \
                .build()
            
            # Build insert SQL
            insert_sql = self._build_insert_sql(table_name)
            
            # Create JDBC sink
            jdbc_sink = JdbcSink.sink(
                insert_sql,
                self._get_row_type_info(),
                jdbc_conn_opts,
                jdbc_exec_opts
            )
            
            self.logger.info(f"JDBC sink ready: {host}:{port}/{database}.{table_name}")
            self.logger.info(f"Batch config: size={batch_size}, interval={batch_interval_ms}ms, retries={max_retries}")
            
            return jdbc_sink
            
        except Exception as e:
            self.logger.error(f"Failed to build JDBC sink: {e}")
            raise

    def get_connection_info(self, host, port, database, table_name):
        """Get connection info string."""
        return f"{host}:{port}/{database}.{table_name}"


class ParseAndConvertFlatMap(FlatMapFunction):
    """Parse order JSON string and emit Row objects ready for MySQL."""

    def __init__(self, logger=None):
        self.logger = logger or default_logger
        self.processed_count = 0

    def flat_map(self, value):
        """
        PyFlink FlatMapFunction 入口。
        输入：单条 JSON 字符串（Kafka 消费到的每条记录）
        输出：逐条 yield 转换后的 Row 对象（Flink JDBC sink 需要 Row 对象，不是 tuple）
        """
        try:
            tuples = parse_walmart_order_json_string_to_tuples(value)
            self.processed_count += len(tuples)
            if self.processed_count == 1 or self.processed_count % 10 == 0:
                self.logger.info(f"Processing data: received {self.processed_count} order line(s) so far...")
                import sys
                sys.stdout.flush()
            rows_yielded = 0
            for t in tuples:
                # Convert tuple to Row object for JDBC sink
                # Row object is required by Flink JDBC connector
                # Tuple has 52 fields matching the MySQL table structure
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
                rows_yielded += 1
                yield row
            if rows_yielded > 0 and (self.processed_count == 1 or self.processed_count % 100 == 0):
                self.logger.info(f"  -> Yielded {rows_yielded} row(s) to JDBC sink for this batch")
                import sys
                sys.stdout.flush()
        except Exception as e:
            self.logger.error(f"Error parsing order data: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            import sys
            sys.stdout.flush()
            raise


class FlinkProcessAndSinkJDBC:
    """
    处理数据流并使用 Flink JDBC 写出到 MySQL。
    
    Step 4: parse_and_convert - 连接数据源并解析转换
    Step 5: connect_jdbc_sink - 转换数据并加载到 MySQL
    """

    def __init__(self, env, logger=None):
        if env is None:
            raise RuntimeError("Flink environment not provided.")
        self.env = env
        self.logger = logger or default_logger
        # Use integrated JDBC sink builder
        self.sink_builder = WalmartOrderJDBCSinkBuilder(logger=self.logger)

    def get_row_type_info(self):
        """Get row type info from sink builder."""
        return self.sink_builder.get_row_type_info()

    def parse_and_convert(self, raw_stream):
        """
        Step 4: 解析原始 JSON 流并转换为 MySQL 行格式。
        
        Args:
            raw_stream: Raw DataStream from Kafka source
            
        Returns:
            DataStream with row tuples ready for MySQL
        """
        try:
            self.logger.info("Step 4: Parsing orders and converting to row tuples...")
            self.logger.info("  Waiting for data from Kafka source...")
            row_type_info = self.sink_builder.get_row_type_info()
            row_stream = raw_stream.flat_map(
                ParseAndConvertFlatMap(logger=self.logger),
                output_type=row_type_info
            )
            self.logger.info("✓ Parse & convert configured")
            self.logger.info("  Data processing will start automatically when messages arrive from Kafka")
            return row_stream
        except Exception as e:
            self.logger.error(f"Failed to configure parse_and_convert: {e}")
            raise

    def connect_jdbc_sink(self, row_stream, mysql_config, batch_size=100, batch_interval_ms=5000, max_retries=3):
        """
        Step 5: 将转换后的数据使用 Flink JDBC 写入 MySQL。
        
        Args:
            row_stream: DataStream with row tuples
            mysql_config: Dictionary with MySQL connection config (host, port, database, user, password, table_name)
            batch_size: JDBC batch size
            batch_interval_ms: Batch interval in milliseconds
            max_retries: Maximum retry attempts
        """
        try:
            self.logger.info("Step 5: Connecting JDBC sink...")
            
            # Build JDBC sink using integrated builder
            jdbc_sink = self.sink_builder.build(
                host=mysql_config['host'],
                port=mysql_config['port'],
                database=mysql_config['database'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                table_name=mysql_config['table_name'],
                batch_size=batch_size,
                batch_interval_ms=batch_interval_ms,
                max_retries=max_retries
            )
            
            # Add sink to stream
            row_stream.add_sink(jdbc_sink)
            
            self.logger.info("✓ JDBC sink added to stream")
            self.logger.info(f"Connection: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}.{mysql_config['table_name']}")
            
        except Exception as e:
            self.logger.error(f"Failed to add JDBC sink: {e}")
            raise


if __name__ == "__main__":
    print("This module handles processing and JDBC sink. Import and use in pipeline steps.")