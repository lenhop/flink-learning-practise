#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 FlinkTableAPIUtils 类

测试覆盖:
1. __init__ 方法
2. create_table_environment 方法
3. create_stream_table_environment 方法
4. create_table_from_datastream 方法
5. convert_table_to_datastream 方法
6. execute_sql_query 方法
7. execute_sql_ddl 方法
8. execute_sql_insert 方法
9. register_temporary_table 方法
10. create_table_from_path 方法
"""

import unittest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.schema import Schema

# 导入被测试的模块
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.flink_table_api_utils import FlinkTableAPIUtils


class TestFlinkTableAPIUtils(unittest.TestCase):
    """测试 FlinkTableAPIUtils 类"""

    def setUp(self):
        """测试前准备"""
        self.table_utils = FlinkTableAPIUtils()

    def test_init(self):
        """测试 __init__ 方法"""
        self.assertIsInstance(self.table_utils, FlinkTableAPIUtils)

    def test_create_table_environment_batch(self):
        """测试 create_table_environment 批处理模式"""
        table_env = self.table_utils.create_table_environment(
            execution_mode="batch"
        )

        self.assertIsNotNone(table_env)

    def test_create_table_environment_streaming(self):
        """测试 create_table_environment 流处理模式"""
        table_env = self.table_utils.create_table_environment(
            execution_mode="streaming"
        )

        self.assertIsNotNone(table_env)

    def test_create_table_environment_with_config(self):
        """测试 create_table_environment 使用自定义配置"""
        config = {
            "table.exec.resource.default-parallelism": "4",
            "table.exec.mini-batch.enabled": "true"
        }

        table_env = self.table_utils.create_table_environment(
            execution_mode="streaming",
            config=config
        )

        self.assertIsNotNone(table_env)

    def test_create_stream_table_environment(self):
        """测试 create_stream_table_environment 方法"""
        # 创建模拟的 StreamExecutionEnvironment
        mock_env = Mock(spec=StreamExecutionEnvironment)

        table_env = self.table_utils.create_stream_table_environment(
            stream_env=mock_env
        )

        self.assertIsNotNone(table_env)

    def test_create_table_from_datastream(self):
        """测试 create_table_from_datastream 方法"""
        mock_data_stream = Mock()
        mock_table_env = Mock()

        # 模拟 from_data_stream 方法
        mock_table = Mock()
        mock_table_env.from_data_stream = Mock(return_value=mock_table)

        table = self.table_utils.create_table_from_datastream(
            data_stream=mock_data_stream,
            table_env=mock_table_env,
            table_name="test_table"
        )

        self.assertIsNotNone(table)

    def test_create_table_from_datastream_with_schema(self):
        """测试 create_table_from_datastream 使用自定义 schema"""
        mock_data_stream = Mock()
        mock_table_env = Mock()

        mock_table = Mock()
        mock_table_env.from_data_stream = Mock(return_value=mock_table)

        # 创建 schema
        schema = Schema.new_builder() \
            .column("id", DataTypes.INT()) \
            .column("name", DataTypes.STRING()) \
            .column("amount", DataTypes.DOUBLE()) \
            .build()

        table = self.table_utils.create_table_from_datastream(
            data_stream=mock_data_stream,
            table_env=mock_table_env,
            table_name="test_table_with_schema",
            schema=schema
        )

        self.assertIsNotNone(table)

    def test_convert_table_to_datastream(self):
        """测试 convert_table_to_datastream 方法"""
        mock_table = Mock()
        mock_table_env = Mock()

        # 模拟 to_data_stream 方法
        mock_data_stream = Mock()
        mock_table_env.to_data_stream = Mock(return_value=mock_data_stream)

        data_stream = self.table_utils.convert_table_to_datastream(
            table=mock_table,
            table_env=mock_table_env,
            data_type=DataTypes.ROW([
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING())
            ])
        )

        self.assertIsNotNone(data_stream)

    def test_execute_sql_query(self):
        """测试 execute_sql_query 方法"""
        mock_table_env = Mock()

        # 模拟 sql_query 方法
        mock_table = Mock()
        mock_table_env.sql_query = Mock(return_value=mock_table)

        result_table = self.table_utils.execute_sql_query(
            table_env=mock_table_env,
            sql_query="SELECT * FROM source_table WHERE amount > 100"
        )

        self.assertIsNotNone(result_table)

    def test_execute_sql_ddl(self):
        """测试 execute_sql_ddl 方法"""
        mock_table_env = Mock()

        # 模拟 execute_sql 方法
        mock_table_env.execute_sql = Mock()

        self.table_utils.execute_sql_ddl(
            table_env=mock_table_env,
            ddl_statement="CREATE TABLE source_table (id INT, name STRING) WITH (...)"
        )

        # 验证 execute_sql 被调用
        mock_table_env.execute_sql.assert_called_once()

    def test_execute_sql_insert(self):
        """测试 execute_sql_insert 方法"""
        mock_table_env = Mock()

        # 模拟 execute_sql 方法
        mock_table_env.execute_sql = Mock()

        self.table_utils.execute_sql_insert(
            table_env=mock_table_env,
            insert_statement="INSERT INTO sink_table SELECT * FROM source_table"
        )

        # 验证 execute_sql 被调用
        mock_table_env.execute_sql.assert_called_once()

    def test_register_temporary_table(self):
        """测试 register_temporary_table 方法"""
        mock_table = Mock()
        mock_table_env = Mock()

        # 模拟 create_temporary_view 方法
        mock_table_env.create_temporary_view = Mock()

        self.table_utils.register_temporary_table(
            table=mock_table,
            table_env=mock_table_env,
            table_name="temp_table"
        )

        # 验证 create_temporary_view 被调用
        mock_table_env.create_temporary_view.assert_called_once()

    def test_create_table_from_path(self):
        """测试 create_table_from_path 方法"""
        mock_table_env = Mock()

        # 创建临时文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,amount\n1,Alice,100.5\n2,Bob,200.3\n")
            temp_file = f.name

        try:
            # 模拟 execute_sql 方法
            mock_table_env.execute_sql = Mock()

            self.table_utils.create_table_from_path(
                table_env=mock_table_env,
                file_path=temp_file,
                table_name="file_table",
                file_type="csv",
                schema="`id` INT, `name` STRING, `amount` DOUBLE"
            )

            # 验证 execute_sql 被调用
            mock_table_env.execute_sql.assert_called_once()

        finally:
            os.unlink(temp_file)

    def test_create_table_from_path_json(self):
        """测试 create_table_from_path 使用 JSON 格式"""
        mock_table_env = Mock()

        # 创建临时 JSON 文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"id": 1, "name": "Alice", "amount": 100.5}\n')
            f.write('{"id": 2, "name": "Bob", "amount": 200.3}\n')
            temp_file = f.name

        try:
            mock_table_env.execute_sql = Mock()

            self.table_utils.create_table_from_path(
                table_env=mock_table_env,
                file_path=temp_file,
                table_name="json_table",
                file_type="json",
                schema="`id` INT, `name` STRING, `amount` DOUBLE"
            )

            mock_table_env.execute_sql.assert_called_once()

        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main()