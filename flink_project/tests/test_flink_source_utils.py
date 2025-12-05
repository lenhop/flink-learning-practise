#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 FlinkSourceUtils 类

测试覆盖:
1. __init__ 方法
2. create_kafka_source 方法
3. create_kafka_table_source_ddl 方法
4. create_file_source 方法
5. create_jdbc_table_source_ddl 方法
6. create_socket_source 方法
7. create_custom_source 方法
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
from utils.flink_source_utils import FlinkSourceUtils


class TestFlinkSourceUtils(unittest.TestCase):
    """测试 FlinkSourceUtils 类"""

    def setUp(self):
        """测试前准备"""
        self.source_utils = FlinkSourceUtils()

    def test_init(self):
        """测试 __init__ 方法"""
        self.assertIsInstance(self.source_utils, FlinkSourceUtils)

    def test_create_kafka_source_basic(self):
        """测试 create_kafka_source 基本功能"""
        # 创建模拟环境
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 调用方法
        kafka_source = self.source_utils.create_kafka_source(
            env=mock_env,
            topic="test-topic",
            bootstrap_servers=["localhost:9092"],
            group_id="test-group",
            offset="earliest"
        )

        # 验证返回对象
        self.assertIsNotNone(kafka_source)

    def test_create_kafka_source_with_single_server(self):
        """测试 create_kafka_source 使用单个服务器地址"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        kafka_source = self.source_utils.create_kafka_source(
            env=mock_env,
            topic="test-topic",
            bootstrap_servers="localhost:9092",  # 单个字符串
            offset="latest"
        )

        self.assertIsNotNone(kafka_source)

    def test_create_kafka_table_source_ddl(self):
        """测试 create_kafka_table_source_ddl 方法"""
        ddl = self.source_utils.create_kafka_table_source_ddl(
            table_name="kafka_source_table",
            topic="test-topic",
            bootstrap_servers=["localhost:9092"],
            schema="`user_id` STRING, `amount` DOUBLE, `timestamp` TIMESTAMP(3)",
            format_type="json"
        )

        # 验证 DDL 包含必要的关键字
        self.assertIn("CREATE TABLE", ddl)
        self.assertIn("kafka_source_table", ddl)
        self.assertIn("test-topic", ddl)
        self.assertIn("localhost:9092", ddl)
        self.assertIn("json", ddl)

    def test_create_file_source(self):
        """测试 create_file_source 方法"""
        # 创建临时文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("line1\nline2\nline3\n")
            temp_file = f.name

        try:
            mock_env = Mock(spec=StreamExecutionEnvironment)

            # 测试文本文件
            data_stream = self.source_utils.create_file_source(
                env=mock_env,
                file_path=temp_file,
                source_name="test-file-source"
            )

            self.assertIsNotNone(data_stream)

            # 测试 CSV 文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                f.write("name,age,city\nAlice,30,Beijing\nBob,25,Shanghai\n")
                csv_file = f.name

            data_stream_csv = self.source_utils.create_file_source(
                env=mock_env,
                file_path=csv_file,
                file_type="csv",
                source_name="test-csv-source"
            )

            self.assertIsNotNone(data_stream_csv)
            os.unlink(csv_file)

        finally:
            os.unlink(temp_file)

    def test_create_jdbc_table_source_ddl(self):
        """测试 create_jdbc_table_source_ddl 方法"""
        ddl = self.source_utils.create_jdbc_table_source_ddl(
            table_name="jdbc_source_table",
            url="jdbc:mysql://localhost:3306/testdb",
            table_name_in_db="source_table",
            username="test_user",
            password="test_pass",
            schema="`id` INT, `name` STRING, `created_at` TIMESTAMP"
        )

        # 验证 DDL 包含必要的关键字
        self.assertIn("CREATE TABLE", ddl)
        self.assertIn("jdbc_source_table", ddl)
        self.assertIn("jdbc:mysql://", ddl)
        self.assertIn("testdb", ddl)

    def test_create_socket_source(self):
        """测试 create_socket_source 方法"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        data_stream = self.source_utils.create_socket_source(
            env=mock_env,
            host="localhost",
            port=9999,
            source_name="test-socket-source"
        )

        self.assertIsNotNone(data_stream)

    def test_create_custom_source(self):
        """测试 create_custom_source 方法"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 创建自定义数据生成函数
        def custom_data_generator():
            return ["data1", "data2", "data3"]

        data_stream = self.source_utils.create_custom_source(
            env=mock_env,
            data_generator=custom_data_generator,
            source_name="test-custom-source"
        )

        self.assertIsNotNone(data_stream)

    def test_create_kafka_source_invalid_offset(self):
        """测试 create_kafka_source 使用无效的 offset 参数"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 使用无效的 offset，应该回退到 earliest
        kafka_source = self.source_utils.create_kafka_source(
            env=mock_env,
            topic="test-topic",
            bootstrap_servers=["localhost:9092"],
            offset="invalid_offset"
        )

        self.assertIsNotNone(kafka_source)


if __name__ == '__main__':
    unittest.main()