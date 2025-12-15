#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 5: Process stream and sink to target (parse -> transform -> MySQL).

参考 flink_walmart_order_pipeline.py 的处理与写出部分，封装为类：
- ParseAndConvertFlatMap: 将原始 JSON 字符串解析为 MySQL 行 tuple
- FlinkProcessAndSink: 提供 parse_and_convert、connect_mysql_sink 两步

Usage:
    processor = FlinkProcessAndSink(env, logger)
    row_stream = processor.parse_and_convert(raw_stream)
    processor.connect_mysql_sink(row_stream, mysql_sink)
"""

import logging

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import FlatMapFunction

from flink5_parse_walmart_order import parse_walmart_order_json_string_to_tuples

# 简单 logger（可被外部传入替换）
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
default_logger = logging.getLogger(__name__)


class ParseAndConvertFlatMap(FlatMapFunction):
    """Parse order JSON string and emit tuples ready for MySQL."""

    def flat_map(self, value):
        """
        PyFlink FlatMapFunction 入口。
        输入：单条 JSON 字符串（Kafka 消费到的每条记录）
        输出：逐条 yield 转换后的 tuple
        """
        tuples = parse_walmart_order_json_string_to_tuples(value)
        for t in tuples:
            yield t


class FlinkProcessAndSink:
    """处理数据流并写出到 MySQL。"""

    def __init__(self, env, logger=None):
        if env is None:
            raise RuntimeError("Flink environment not provided.")
        self.env = env
        self.logger = logger or default_logger

    def _get_row_type_info(self):
        """与 MySQL 表结构对应的 Types.ROW 定义。"""
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
        """解析原始 JSON 流并转换为 MySQL 行格式。"""
        try:
            self.logger.info("Step 5.1: Parsing orders and converting to row tuples...")
            row_stream = raw_stream.flat_map(
                ParseAndConvertFlatMap(),
                output_type=self._get_row_type_info()
            )
            self.logger.info("✓ Parse & convert configured")
            return row_stream
        except Exception as e:
            self.logger.error(f"Failed to configure parse_and_convert: {e}")
            raise

    def connect_mysql_sink(self, row_stream, mysql_sink):
        """将转换后的数据写入 MySQL。"""
        try:
            self.logger.info("Step 5.2: Connecting MySQL sink...")
            sink_func = mysql_sink.get_sink_function()
            row_stream.map(sink_func)
            self.logger.info("✓ MySQL sink added to stream")
        except Exception as e:
            self.logger.error(f"Failed to add MySQL sink: {e}")
            raise


if __name__ == "__main__":
    print("This module handles processing and sink. Import and use in pipeline steps.")

