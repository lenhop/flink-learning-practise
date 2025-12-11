#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build MySQL sink for Flink pipeline (Step 5 dependency).

参考 flink_walmart_order_pipeline.py 的 MySQL Sink 实现，封装为类：
- WalmartOrderMySQLSinkBuilder:负责创建并返回 sink 对象
- WalmartOrderMySQLSink: 提供插入/替换逻辑与 sink 函数

Usage:
    builder = WalmartOrderMySQLSinkBuilder(logger)
    mysql_sink = builder.build(host, port, database, user, password, table_name='walmart_order')
    sink_func = mysql_sink.get_sink_function()
    row_stream.map(sink_func)
"""

import logging
import pymysql

# 简单 logger（可被外部传入替换）
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
default_logger = logging.getLogger(__name__)


class WalmartOrderMySQLSink:
    """MySQL Sink class for Walmart order data (REPLACE INTO)."""

    def __init__(self, host, port, database, user, password, table_name='walmart_order', logger=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.logger = logger or default_logger
        self.insert_sql = self._build_insert_sql()

    def _build_insert_sql(self):
        """Build REPLACE SQL matching table structure (PK: purchaseOrderId, sku)."""
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
        """Create MySQL connection."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            charset='utf8mb4',
            autocommit=True
        )

    @staticmethod
    def _convert_to_tuple(value):
        """Normalize value to tuple for insertion."""
        if isinstance(value, tuple):
            return value
        if hasattr(value, '_values'):
            return tuple(value._values)
        if isinstance(value, list):
            return tuple(value)
        return tuple(value) if hasattr(value, '__iter__') and not isinstance(value, str) else (value,)

    def insert_record(self, value):
        """Insert/replace a single record."""
        try:
            params = self._convert_to_tuple(value)
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(self.insert_sql, params)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting record: {e}")
            self.logger.error(f"  Value preview: {str(value)[:100] if value else 'None'}")
            return False

    def get_sink_function(self):
        """Return a sink function for Flink map()."""
        def sink_func(value):
            self.insert_record(value)
        return sink_func

    def get_connection_info(self):
        return f"{self.host}:{self.port}/{self.database}.{self.table_name}"


class WalmartOrderMySQLSinkBuilder:
    """Builder to create WalmartOrderMySQLSink."""

    def __init__(self, logger=None):
        self.logger = logger or default_logger

    def build(self, host, port, database, user, password, table_name='walmart_order'):
        sink = WalmartOrderMySQLSink(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            table_name=table_name,
            logger=self.logger
        )
        self.logger.info(f"MySQL sink ready: {sink.get_connection_info()}")
        return sink


if __name__ == "__main__":
    print("This module builds MySQL sink for Walmart orders. Import and use in pipeline.")

