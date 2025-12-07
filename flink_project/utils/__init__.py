#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink 工具类模块

提供 Flink 数据处理的工具类, 支持 DataStream API 和 Table API

主要工具类:
- FlinkSourceUtils: 数据源工具类
- FlinkSinkUtils: 数据输出工具类
- FlinkTableApiUtils: Table API 工具类
- FlinkWindowUtils: 窗口工具类
- FlinkTransformUtils: 数据转换工具类
- FlinkKafkaToMysqlUtils: Kafka 到 MySQL 数据管道工具类

"""

try:
    from .flink_source_utils import FlinkSourceUtils
except ImportError:
    FlinkSourceUtils = None

try:
    from .flink_sink_utils import FlinkSinkUtils
except ImportError:
    FlinkSinkUtils = None

try:
    from .flink_table_api_utils import FlinkTableApiUtils
except ImportError:
    FlinkTableApiUtils = None

try:
    from .flink_window_utils import FlinkWindowUtils
except ImportError:
    FlinkWindowUtils = None

try:
    from .flink_transform_utils import FlinkTransformUtils
except ImportError:
    FlinkTransformUtils = None

try:
    from .flink_kafka_to_mysql_utils import FlinkKafkaToMysqlUtils
except ImportError:
    FlinkKafkaToMysqlUtils = None

__all__ = [
    'FlinkSourceUtils',
    'FlinkSinkUtils',
    'FlinkTableApiUtils',
    'FlinkWindowUtils',
    'FlinkTransformUtils',
    'FlinkKafkaToMysqlUtils',
]

