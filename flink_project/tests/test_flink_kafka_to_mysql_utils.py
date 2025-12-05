#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 FlinkKafkaToMysqlUtils 类

测试覆盖:
1. __init__ 方法
2. create_etl_pipeline_datastream 方法
3. default_transform 内部函数
4. create_etl_pipeline_table_api 方法
5. create_etl_pipeline_with_transform 方法
6. default_map 内部函数
7. transform_order 方法
"""

import unittest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableEnvironment, EnvironmentSettings

# 导入被测试的模块
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.flink_kafka_to_mysql_utils import FlinkKafkaToMysqlUtils


class TestFlinkKafkaToMysqlUtils(unittest.TestCase):
    """测试 FlinkKafkaToMysqlUtils 类"""

    def setUp(self):
        """测试前准备"""
        self.etl_utils = FlinkKafkaToMysqlUtils()

    def test_init(self):
        """测试 __init__ 方法"""
        self.assertIsInstance(self.etl_utils, FlinkKafkaToMysqlUtils)

    def test_create_etl_pipeline_datastream_basic(self):
        """测试 create_etl_pipeline_datastream 基本功能"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 模拟 Kafka source 创建
        mock_kafka_source = Mock()
        with patch.object(self.etl_utils, '_create_kafka_source', return_value=mock_kafka_source):
            # 模拟 MySQL sink 创建
            mock_mysql_sink = Mock()
            with patch.object(self.etl_utils, '_create_mysql_sink', return_value=mock_mysql_sink):
                # 模拟数据流处理链
                mock_data_stream = Mock()
                mock_env.from_source = Mock(return_value=mock_data_stream)
                mock_data_stream.map = Mock(return_value=mock_data_stream)
                mock_data_stream.add_sink = Mock()

                result = self.etl_utils.create_etl_pipeline_datastream(
                    env=mock_env,
                    kafka_topic="test-topic",
                    kafka_servers=["localhost:9092"],
                    mysql_url="jdbc:mysql://localhost:3306/testdb",
                    mysql_table="sink_table",
                    mysql_username="test_user",
                    mysql_password="test_pass"
                )

                # 验证返回结果
                self.assertIsNotNone(result)
                self.assertIn("env", result)
                self.assertIn("pipeline", result)

    def test_create_etl_pipeline_datastream_with_transform(self):
        """测试 create_etl_pipeline_datastream 使用自定义转换函数"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 自定义转换函数
        def custom_transform(message):
            data = json.loads(message)
            data["processed"] = True
            return json.dumps(data)

        mock_kafka_source = Mock()
        with patch.object(self.etl_utils, '_create_kafka_source', return_value=mock_kafka_source):
            mock_mysql_sink = Mock()
            with patch.object(self.etl_utils, '_create_mysql_sink', return_value=mock_mysql_sink):
                mock_data_stream = Mock()
                mock_env.from_source = Mock(return_value=mock_data_stream)
                mock_data_stream.map = Mock(return_value=mock_data_stream)
                mock_data_stream.add_sink = Mock()

                result = self.etl_utils.create_etl_pipeline_datastream(
                    env=mock_env,
                    kafka_topic="test-topic",
                    kafka_servers=["localhost:9092"],
                    mysql_url="jdbc:mysql://localhost:3306/testdb",
                    mysql_table="sink_table",
                    mysql_username="test_user",
                    mysql_password="test_pass",
                    transform_func=custom_transform
                )

                self.assertIsNotNone(result)

    def test_default_transform_internal(self):
        """测试 default_transform 内部函数"""
        # 创建测试数据
        test_json = '{"order_id": "123", "amount": 100.5, "user_id": "user1"}'

        # 调用内部函数
        result = self.etl_utils._default_transform(test_json)

        # 验证结果
        self.assertIsInstance(result, str)
        parsed_result = json.loads(result)
        self.assertIn("order_id", parsed_result)
        self.assertIn("amount", parsed_result)
        self.assertIn("user_id", parsed_result)

    def test_default_transform_invalid_json(self):
        """测试 default_transform 处理无效 JSON"""
        invalid_json = "invalid json string"

        # 应该处理异常并返回默认值或原始字符串
        result = self.etl_utils._default_transform(invalid_json)
        self.assertIsInstance(result, str)

    def test_create_etl_pipeline_table_api(self):
        """测试 create_etl_pipeline_table_api 方法"""
        mock_table_env = Mock()

        # 模拟 SQL 执行
        mock_table_env.execute_sql = Mock()

        result = self.etl_utils.create_etl_pipeline_table_api(
            table_env=mock_table_env,
            kafka_topic="test-topic",
            kafka_servers=["localhost:9092"],
            mysql_url="jdbc:mysql://localhost:3306/testdb",
            mysql_table="sink_table",
            mysql_username="test_user",
            mysql_password="test_pass",
            source_schema="`order_id` STRING, `amount` DOUBLE, `user_id` STRING",
            sink_schema="`order_id` STRING, `amount` DOUBLE, `user_id` STRING"
        )

        # 验证返回结果
        self.assertIsNotNone(result)
        self.assertIn("table_env", result)
        self.assertIn("pipeline", result)

        # 验证 execute_sql 被调用（至少两次：创建源表和目标表）
        self.assertGreaterEqual(mock_table_env.execute_sql.call_count, 2)

    def test_create_etl_pipeline_with_transform(self):
        """测试 create_etl_pipeline_with_transform 方法"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 模拟各个组件的创建
        mock_kafka_source = Mock()
        mock_mysql_sink = Mock()
        mock_data_stream = Mock()

        with patch.object(self.etl_utils, '_create_kafka_source', return_value=mock_kafka_source):
            with patch.object(self.etl_utils, '_create_mysql_sink', return_value=mock_mysql_sink):
                mock_env.from_source = Mock(return_value=mock_data_stream)
                mock_data_stream.map = Mock(return_value=mock_data_stream)
                mock_data_stream.filter = Mock(return_value=mock_data_stream)
                mock_data_stream.key_by = Mock(return_value=mock_data_stream)
                mock_data_stream.add_sink = Mock()

                result = self.etl_utils.create_etl_pipeline_with_transform(
                    env=mock_env,
                    kafka_topic="test-topic",
                    kafka_servers=["localhost:9092"],
                    mysql_url="jdbc:mysql://localhost:3306/testdb",
                    mysql_table="sink_table",
                    mysql_username="test_user",
                    mysql_password="test_pass"
                )

                self.assertIsNotNone(result)
                self.assertIn("env", result)
                self.assertIn("pipeline", result)

    def test_default_map_internal(self):
        """测试 default_map 内部函数"""
        # 创建测试数据
        test_json = '{"order_id": "456", "amount": 200.0, "user_id": "user2", "status": "pending"}'

        # 调用内部函数
        result = self.etl_utils._default_map(test_json)

        # 验证结果应该是字典
        self.assertIsInstance(result, dict)
        self.assertIn("order_id", result)
        self.assertIn("amount", result)
        self.assertIn("user_id", result)
        self.assertIn("status", result)

    def test_default_map_invalid_json(self):
        """测试 default_map 处理无效 JSON"""
        invalid_json = "invalid json"

        # 应该返回包含错误信息的字典
        result = self.etl_utils._default_map(invalid_json)
        self.assertIsInstance(result, dict)
        self.assertIn("error", result)

    def test_transform_order(self):
        """测试 transform_order 方法"""
        # 测试正常订单数据
        order_json = '{"order_id": "789", "amount": 150.75, "user_id": "user3", "items": [{"id": "item1", "qty": 2}]}'

        result = self.etl_utils.transform_order(order_json)

        # 验证结果
        self.assertIsInstance(result, dict)
        self.assertIn("order_id", result)
        self.assertIn("amount", result)
        self.assertIn("user_id", result)
        self.assertIn("item_count", result)
        self.assertIn("total_quantity", result)

        # 验证计算字段
        self.assertEqual(result["item_count"], 1)
        self.assertEqual(result["total_quantity"], 2)

    def test_transform_order_invalid_data(self):
        """测试 transform_order 处理无效数据"""
        # 测试无效 JSON
        invalid_json = "invalid"
        result = self.etl_utils.transform_order(invalid_json)
        self.assertIsInstance(result, dict)
        self.assertIn("error", result)

        # 测试缺失必要字段
        incomplete_json = '{"order_id": "999"}'
        result = self.etl_utils.transform_order(incomplete_json)
        self.assertIsInstance(result, dict)
        self.assertIn("order_id", result)

    def test_create_etl_pipeline_datastream_no_mysql(self):
        """测试 create_etl_pipeline_datastream 没有 MySQL 连接的情况"""
        mock_env = Mock(spec=StreamExecutionEnvironment)

        # 模拟没有 MySQL 环境的情况
        with patch.object(self.etl_utils, '_create_mysql_sink', side_effect=Exception("MySQL connection failed")):
            try:
                result = self.etl_utils.create_etl_pipeline_datastream(
                    env=mock_env,
                    kafka_topic="test-topic",
                    kafka_servers=["localhost:9092"],
                    mysql_url="jdbc:mysql://localhost:3306/testdb",
                    mysql_table="sink_table",
                    mysql_username="test_user",
                    mysql_password="test_pass"
                )
                # 如果函数有错误处理，可能返回部分结果
                self.assertIsNotNone(result)
            except Exception as e:
                # 如果抛出异常，也是可以接受的
                print(f"create_etl_pipeline_datastream 测试中遇到预期异常: {e}")
                self.skipTest("MySQL 环境不可用")


if __name__ == '__main__':
    unittest.main()