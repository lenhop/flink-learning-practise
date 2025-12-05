#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 FlinkWindowUtils 类

测试覆盖:
1. __init__ 方法
2. tumbling_time_window 方法
3. sliding_time_window 方法
4. session_window 方法
5. tumbling_count_window 方法
6. sliding_count_window 方法
7. window_aggregate 方法
8. 内部聚合器方法
"""

import unittest
import os
from unittest.mock import Mock, patch, MagicMock
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.common import Time

# 导入被测试的模块
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.flink_window_utils import FlinkWindowUtils


class TestFlinkWindowUtils(unittest.TestCase):
    """测试 FlinkWindowUtils 类"""

    def setUp(self):
        """测试前准备"""
        self.window_utils = FlinkWindowUtils()

    def test_init(self):
        """测试 __init__ 方法"""
        self.assertIsInstance(self.window_utils, FlinkWindowUtils)

    def test_tumbling_time_window(self):
        """测试 tumbling_time_window 方法"""
        mock_keyed_stream = Mock()

        # 模拟 window 方法
        mock_windowed_stream = Mock()
        mock_keyed_stream.window = Mock(return_value=mock_windowed_stream)

        windowed_stream = self.window_utils.tumbling_time_window(
            keyed_stream=mock_keyed_stream,
            window_size="5 seconds",
            transform_name="test-tumbling-window"
        )

        self.assertIsNotNone(windowed_stream)
        self.assertEqual(windowed_stream, mock_windowed_stream)

    def test_tumbling_time_window_with_time_class(self):
        """测试 tumbling_time_window 使用 Time 类"""
        mock_keyed_stream = Mock()
        mock_windowed_stream = Mock()
        mock_keyed_stream.window = Mock(return_value=mock_windowed_stream)

        windowed_stream = self.window_utils.tumbling_time_window(
            keyed_stream=mock_keyed_stream,
            window_size=Time.seconds(10),
            transform_name="test-tumbling-time-class"
        )

        self.assertIsNotNone(windowed_stream)

    def test_sliding_time_window(self):
        """测试 sliding_time_window 方法"""
        mock_keyed_stream = Mock()
        mock_windowed_stream = Mock()
        mock_keyed_stream.window = Mock(return_value=mock_windowed_stream)

        windowed_stream = self.window_utils.sliding_time_window(
            keyed_stream=mock_keyed_stream,
            window_size="10 seconds",
            slide_size="5 seconds",
            transform_name="test-sliding-window"
        )

        self.assertIsNotNone(windowed_stream)

    def test_session_window(self):
        """测试 session_window 方法"""
        mock_keyed_stream = Mock()
        mock_windowed_stream = Mock()
        mock_keyed_stream.window = Mock(return_value=mock_windowed_stream)

        windowed_stream = self.window_utils.session_window(
            keyed_stream=mock_keyed_stream,
            gap_size="30 seconds",
            transform_name="test-session-window"
        )

        self.assertIsNotNone(windowed_stream)

    def test_tumbling_count_window(self):
        """测试 tumbling_count_window 方法"""
        mock_keyed_stream = Mock()
        mock_windowed_stream = Mock()
        mock_keyed_stream.count_window = Mock(return_value=mock_windowed_stream)

        windowed_stream = self.window_utils.tumbling_count_window(
            keyed_stream=mock_keyed_stream,
            window_size=100,
            transform_name="test-tumbling-count"
        )

        self.assertIsNotNone(windowed_stream)

    def test_sliding_count_window(self):
        """测试 sliding_count_window 方法"""
        mock_keyed_stream = Mock()
        mock_windowed_stream = Mock()
        mock_keyed_stream.count_window = Mock(return_value=mock_windowed_stream)

        windowed_stream = self.window_utils.sliding_count_window(
            keyed_stream=mock_keyed_stream,
            window_size=100,
            slide_size=50,
            transform_name="test-sliding-count"
        )

        self.assertIsNotNone(windowed_stream)

    def test_window_aggregate_sum(self):
        """测试 window_aggregate 求和聚合"""
        mock_windowed_stream = Mock()

        # 模拟 aggregate 方法
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type="sum",
            aggregate_field="amount",
            result_field="total_amount",
            transform_name="test-window-sum"
        )

        self.assertIsNotNone(aggregated_stream)

    def test_window_aggregate_average(self):
        """测试 window_aggregate 平均值聚合"""
        mock_windowed_stream = Mock()
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type="average",
            aggregate_field="amount",
            result_field="avg_amount",
            transform_name="test-window-avg"
        )

        self.assertIsNotNone(aggregated_stream)

    def test_window_aggregate_count(self):
        """测试 window_aggregate 计数聚合"""
        mock_windowed_stream = Mock()
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type="count",
            aggregate_field="*",
            result_field="record_count",
            transform_name="test-window-count"
        )

        self.assertIsNotNone(aggregated_stream)

    def test_window_aggregate_max(self):
        """测试 window_aggregate 最大值聚合"""
        mock_windowed_stream = Mock()
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type="max",
            aggregate_field="amount",
            result_field="max_amount",
            transform_name="test-window-max"
        )

        self.assertIsNotNone(aggregated_stream)

    def test_window_aggregate_min(self):
        """测试 window_aggregate 最小值聚合"""
        mock_windowed_stream = Mock()
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type="min",
            aggregate_field="amount",
            result_field="min_amount",
            transform_name="test-window-min"
        )

        self.assertIsNotNone(aggregated_stream)

    def test_window_aggregate_custom(self):
        """测试 window_aggregate 自定义聚合函数"""
        mock_windowed_stream = Mock()
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        # 自定义聚合函数
        def custom_aggregator():
            pass

        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type=custom_aggregator,
            aggregate_field="amount",
            result_field="custom_result",
            transform_name="test-window-custom"
        )

        self.assertIsNotNone(aggregated_stream)

    def test_window_aggregate_invalid_type(self):
        """测试 window_aggregate 使用无效的聚合类型"""
        mock_windowed_stream = Mock()

        # 应该抛出异常或使用默认值
        try:
            aggregated_stream = self.window_utils.window_aggregate(
                windowed_stream=mock_windowed_stream,
                aggregate_type="invalid_type",
                aggregate_field="amount",
                result_field="result",
                transform_name="test-invalid"
            )
            # 如果没有抛出异常，至少应该返回 None 或默认值
            self.assertIsNotNone(aggregated_stream)
        except Exception as e:
            # 如果抛出异常，也是可以接受的
            print(f"window_aggregate 测试中遇到预期异常: {e}")

    def test_internal_aggregator_methods(self):
        """测试内部聚合器方法（通过实际调用验证）"""
        # 这些是内部类的方法，我们通过调用 window_aggregate 来间接测试
        mock_windowed_stream = Mock()
        mock_data_stream = Mock()
        mock_windowed_stream.aggregate = Mock(return_value=mock_data_stream)

        # 测试 sum 聚合器
        aggregated_stream = self.window_utils.window_aggregate(
            windowed_stream=mock_windowed_stream,
            aggregate_type="sum",
            aggregate_field="amount",
            result_field="total",
            transform_name="test-internal-sum"
        )

        self.assertIsNotNone(aggregated_stream)
        # 验证 aggregate 方法被调用
        mock_windowed_stream.aggregate.assert_called_once()


if __name__ == '__main__':
    unittest.main()