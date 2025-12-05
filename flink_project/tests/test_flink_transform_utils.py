#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 FlinkTransformUtils 类

测试覆盖:
1. __init__ 方法
2. map_transform 方法
3. filter_data 方法
4. flatmap_transform 方法
5. key_by_field 方法
6. filter_null_values 方法
7. is_not_null 内部方法
8. validate_data_format 方法
9. handle_errors_with_side_output 方法
10. select_fields_table_api 方法
11. filter_table_api 方法
12. join_streams 方法
13. join_with_lookup_table_sql 方法
"""

import unittest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import col

# 导入被测试的模块
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.flink_transform_utils import FlinkTransformUtils


class TestFlinkTransformUtils(unittest.TestCase):
    """测试 FlinkTransformUtils 类"""

    def setUp(self):
        """测试前准备"""
        self.transform_utils = FlinkTransformUtils()

    def test_init(self):
        """测试 __init__ 方法"""
        self.assertIsInstance(self.transform_utils, FlinkTransformUtils)

    def test_map_transform(self):
        """测试 map_transform 方法"""
        mock_data_stream = Mock()

        # 定义映射函数
        def double_value(x):
            return x * 2

        transformed_stream = self.transform_utils.map_transform(
            data_stream=mock_data_stream,
            map_func=double_value,
            transform_name="test-map"
        )

        self.assertIsNotNone(transformed_stream)

    def test_map_transform_with_lambda(self):
        """测试 map_transform 使用 lambda 函数"""
        mock_data_stream = Mock()

        transformed_stream = self.transform_utils.map_transform(
            data_stream=mock_data_stream,
            map_func=lambda x: x.upper(),
            transform_name="test-lambda-map"
        )

        self.assertIsNotNone(transformed_stream)

    def test_filter_data(self):
        """测试 filter_data 方法"""
        mock_data_stream = Mock()

        # 定义过滤函数
        def filter_even(x):
            return x % 2 == 0

        filtered_stream = self.transform_utils.filter_data(
            data_stream=mock_data_stream,
            filter_func=filter_even,
            transform_name="test-filter"
        )

        self.assertIsNotNone(filtered_stream)

    def test_flatmap_transform(self):
        """测试 flatmap_transform 方法"""
        mock_data_stream = Mock()

        # 定义 flatmap 函数
        def split_words(sentence):
            return sentence.split()

        transformed_stream = self.transform_utils.flatmap_transform(
            data_stream=mock_data_stream,
            flatmap_func=split_words,
            transform_name="test-flatmap"
        )

        self.assertIsNotNone(transformed_stream)

    def test_key_by_field(self):
        """测试 key_by_field 方法"""
        mock_data_stream = Mock()

        # 测试按字段名分组
        keyed_stream = self.transform_utils.key_by_field(
            data_stream=mock_data_stream,
            key_field="user_id",
            transform_name="test-key-by"
        )

        self.assertIsNotNone(keyed_stream)

    def test_key_by_field_with_key_selector(self):
        """测试 key_by_field 使用 key_selector 函数"""
        mock_data_stream = Mock()

        def key_selector(record):
            return record.get("category", "unknown")

        keyed_stream = self.transform_utils.key_by_field(
            data_stream=mock_data_stream,
            key_field=key_selector,
            transform_name="test-key-selector"
        )

        self.assertIsNotNone(keyed_stream)

    def test_filter_null_values(self):
        """测试 filter_null_values 方法"""
        mock_data_stream = Mock()

        filtered_stream = self.transform_utils.filter_null_values(
            data_stream=mock_data_stream,
            transform_name="test-filter-null"
        )

        self.assertIsNotNone(filtered_stream)

    def test_is_not_null_internal(self):
        """测试 is_not_null 内部方法"""
        # 测试非空值
        self.assertTrue(self.transform_utils._FlinkTransformUtils__is_not_null("test"))
        self.assertTrue(self.transform_utils._FlinkTransformUtils__is_not_null(123))
        self.assertTrue(self.transform_utils._FlinkTransformUtils__is_not_null(0))
        self.assertTrue(self.transform_utils._FlinkTransformUtils__is_not_null(False))

        # 测试空值
        self.assertFalse(self.transform_utils._FlinkTransformUtils__is_not_null(None))
        self.assertFalse(self.transform_utils._FlinkTransformUtils__is_not_null(""))

    def test_validate_data_format(self):
        """测试 validate_data_format 方法"""
        mock_data_stream = Mock()

        # 定义验证函数
        def validate_json(value):
            try:
                json.loads(value)
                return True
            except:
                return False

        validated_stream = self.transform_utils.validate_data_format(
            data_stream=mock_data_stream,
            validation_func=validate_json,
            error_handler=lambda x: f"Invalid JSON: {x}",
            transform_name="test-validate"
        )

        self.assertIsNotNone(validated_stream)

    def test_handle_errors_with_side_output(self):
        """测试 handle_errors_with_side_output 方法"""
        mock_data_stream = Mock()

        # 定义处理函数（可能抛出异常）
        def risky_transform(value):
            if value == "error":
                raise ValueError("Test error")
            return value.upper()

        result = self.transform_utils.handle_errors_with_side_output(
            data_stream=mock_data_stream,
            process_func=risky_transform,
            transform_name="test-error-handling"
        )

        # 应该返回主数据流和侧输出流
        self.assertIsNotNone(result)
        self.assertIn("main_stream", result)
        self.assertIn("error_stream", result)

    def test_select_fields_table_api(self):
        """测试 select_fields_table_api 方法"""
        mock_table = Mock()

        # 模拟 select 方法
        mock_table.select = Mock(return_value=mock_table)

        selected_table = self.transform_utils.select_fields_table_api(
            table=mock_table,
            fields=["field1", "field2", "field3"],
            transform_name="test-select-fields"
        )

        self.assertIsNotNone(selected_table)

    def test_filter_table_api(self):
        """测试 filter_table_api 方法"""
        mock_table = Mock()

        # 模拟 filter 方法
        mock_table.filter = Mock(return_value=mock_table)

        filtered_table = self.transform_utils.filter_table_api(
            table=mock_table,
            condition=col("amount") > 100,
            transform_name="test-table-filter"
        )

        self.assertIsNotNone(filtered_table)

    def test_join_streams(self):
        """测试 join_streams 方法"""
        mock_stream1 = Mock()
        mock_stream2 = Mock()

        # 定义连接条件
        def join_condition(value1, value2):
            return value1.get("id") == value2.get("id")

        # 定义连接函数
        def join_function(value1, value2):
            return {**value1, **value2}

        joined_stream = self.transform_utils.join_streams(
            stream1=mock_stream1,
            stream2=mock_stream2,
            join_condition=join_condition,
            join_function=join_function,
            window_size="5 seconds",
            transform_name="test-join"
        )

        self.assertIsNotNone(joined_stream)

    def test_join_with_lookup_table_sql(self):
        """测试 join_with_lookup_table_sql 方法"""
        mock_table = Mock()
        mock_table_env = Mock()

        # 模拟执行 SQL
        mock_table_env.sql_query = Mock(return_value=mock_table)

        joined_table = self.transform_utils.join_with_lookup_table_sql(
            main_table=mock_table,
            table_env=mock_table_env,
            lookup_table_name="lookup_table",
            join_condition="main_table.id = lookup_table.id",
            select_fields="main_table.*, lookup_table.additional_info",
            transform_name="test-sql-join"
        )

        self.assertIsNotNone(joined_table)


if __name__ == '__main__':
    unittest.main()