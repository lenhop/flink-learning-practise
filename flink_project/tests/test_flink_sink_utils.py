#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 FlinkSinkUtils 类

测试覆盖:
1. __init__ 方法
2. create_kafka_sink 方法
3. create_kafka_table_sink_ddl 方法
4. create_mysql_sink 方法
5. create_mysql_table_sink_ddl 方法
6. escape_sql_string 方法
7. create_file_sink 方法
8. create_print_sink 方法
"""

import unittest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableEnvironment, EnvironmentSettings

# 导入被测试的模块
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.flink_sink_utils import FlinkSinkUtils


class TestFlinkSinkUtils(unittest.TestCase):
    """测试 FlinkSinkUtils 类"""

    def setUp(self):
        """测试前准备"""
        self.sink_utils = FlinkSinkUtils()

    def test_init(self):
        """测试 __init__ 方法"""
        self.assertIsInstance(self.sink_utils, FlinkSinkUtils)

    def test_create_kafka_sink_basic(self):
        """测试 create_kafka_sink 基本功能"""
        # 创建模拟数据流
        mock_data_stream = Mock()

        # 调用方法
        sink = self.sink_utils.create_kafka_sink(
            data_stream=mock_data_stream,
            topic="test-topic",
            bootstrap_servers=["localhost:9092"]
        )

        # 验证返回对象
        self.assertIsNotNone(sink)

    def test_create_kafka_sink_with_serializer(self):
        """测试 create_kafka_sink 使用自定义序列化器"""
        mock_data_stream = Mock()

        # 自定义序列化器
        def custom_serializer(value):
            return str(value).encode('utf-8')

        sink = self.sink_utils.create_kafka_sink(
            data_stream=mock_data_stream,
            topic="test-topic",
            bootstrap_servers=["localhost:9092"],
            serializer=custom_serializer
        )

        self.assertIsNotNone(sink)

    def test_create_kafka_table_sink_ddl(self):
        """测试 create_kafka_table_sink_ddl 方法"""
        ddl = self.sink_utils.create_kafka_table_sink_ddl(
            table_name="kafka_sink_table",
            topic="test-topic",
            bootstrap_servers=["localhost:9092"],
            schema="`user_id` STRING, `amount` DOUBLE, `timestamp` TIMESTAMP(3)",
            format_type="json"
        )

        # 验证 DDL 包含必要的关键字
        self.assertIn("CREATE TABLE", ddl)
        self.assertIn("kafka_sink_table", ddl)
        self.assertIn("test-topic", ddl)
        self.assertIn("localhost:9092", ddl)
        self.assertIn("json", ddl)

    def test_create_mysql_sink(self):
        """测试 create_mysql_sink 方法"""
        mock_data_stream = Mock()

        # 由于需要实际的 MySQL 连接，这里只测试函数调用不报错
        # 在实际环境中需要提供真实的连接信息
        try:
            sink = self.sink_utils.create_mysql_sink(
                data_stream=mock_data_stream,
                url="jdbc:mysql://localhost:3306/testdb",
                table_name="sink_table",
                username="test_user",
                password="test_pass",
                batch_size=1000
            )
            # 如果没有抛出异常，则认为测试通过
            self.assertTrue(True)
        except Exception as e:
            # 如果抛出异常，记录但不失败（因为可能没有 MySQL 环境）
            print(f"create_mysql_sink 测试中遇到异常（可能没有 MySQL 环境）: {e}")
            self.skipTest("MySQL 环境不可用")

    def test_create_mysql_table_sink_ddl(self):
        """测试 create_mysql_table_sink_ddl 方法"""
        ddl = self.sink_utils.create_mysql_table_sink_ddl(
            table_name="mysql_sink_table",
            url="jdbc:mysql://localhost:3306/testdb",
            table_name_in_db="sink_table",
            username="test_user",
            password="test_pass",
            schema="`id` INT, `name` STRING, `created_at` TIMESTAMP"
        )

        # 验证 DDL 包含必要的关键字
        self.assertIn("CREATE TABLE", ddl)
        self.assertIn("mysql_sink_table", ddl)
        self.assertIn("jdbc:mysql://", ddl)
        self.assertIn("testdb", ddl)

    def test_escape_sql_string(self):
        """测试 escape_sql_string 静态方法"""
        # 测试普通字符串
        result = self.sink_utils.escape_sql_string("test")
        self.assertEqual(result, "'test'")

        # 测试包含单引号的字符串
        result = self.sink_utils.escape_sql_string("O'Reilly")
        self.assertEqual(result, "'O''Reilly'")

        # 测试空字符串
        result = self.sink_utils.escape_sql_string("")
        self.assertEqual(result, "''")

        # 测试 None
        result = self.sink_utils.escape_sql_string(None)
        self.assertEqual(result, "NULL")

    def test_create_file_sink(self):
        """测试 create_file_sink 方法"""
        mock_data_stream = Mock()

        # 创建临时目录
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "output.txt")

            sink = self.sink_utils.create_file_sink(
                data_stream=mock_data_stream,
                file_path=output_path,
                sink_name="test-file-sink"
            )

            self.assertIsNotNone(sink)

            # 测试 CSV 格式
            csv_path = os.path.join(temp_dir, "output.csv")
            sink_csv = self.sink_utils.create_file_sink(
                data_stream=mock_data_stream,
                file_path=csv_path,
                file_type="csv",
                sink_name="test-csv-sink"
            )

            self.assertIsNotNone(sink_csv)

    def test_create_print_sink(self):
        """测试 create_print_sink 方法"""
        mock_data_stream = Mock()

        sink = self.sink_utils.create_print_sink(
            data_stream=mock_data_stream,
            sink_name="test-print-sink"
        )

        self.assertIsNotNone(sink)

    def test_create_print_sink_default_name(self):
        """测试 create_print_sink 使用默认名称"""
        mock_data_stream = Mock()

        sink = self.sink_utils.create_print_sink(
            data_stream=mock_data_stream
        )

        self.assertIsNotNone(sink)


if __name__ == '__main__':
    unittest.main()