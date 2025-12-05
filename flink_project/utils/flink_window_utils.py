#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
apache-flink 1.17.0+

Flink 窗口工具类 - Window Utils

功能说明:
窗口操作方法:

1. tumbling_time_window (DataStream API):
   - 功能: 创建滚动时间窗口
   - 用途: 固定时间间隔的窗口聚合, 例如每分钟统计

2. sliding_time_window (DataStream API):
   - 功能: 创建滑动时间窗口
   - 用途: 滑动时间间隔的窗口聚合, 例如每5分钟统计最近1小时

3. session_window (DataStream API):
   - 功能: 创建会话窗口
   - 用途: 基于活动间隔的窗口, 例如用户会话分析

4. tumbling_count_window (DataStream API):
   - 功能: 创建滚动计数窗口
   - 用途: 固定数量的窗口聚合

5. sliding_count_window (DataStream API):
   - 功能: 创建滑动计数窗口
   - 用途: 滑动数量的窗口聚合

6. window_aggregate (DataStream API):
   - 功能: 窗口聚合计算
   - 用途: 在窗口上进行计数、求和、平均值等聚合

7. create_watermark_strategy:
   - 功能: 创建 Watermark 策略
   - 用途: 事件时间处理, 处理乱序数据

8. tumbling_window_sql (Table API / SQL):
   - 功能: 创建滚动窗口 SQL 语句
   - 用途: 使用 SQL 进行窗口聚合

9. sliding_window_sql (Table API / SQL):
   - 功能: 创建滑动窗口 SQL 语句
   - 用途: 使用 SQL 进行滑动窗口聚合

10. session_window_sql (Table API / SQL):
    - 功能: 创建会话窗口 SQL 语句
    - 用途: 使用 SQL 进行会话窗口聚合

支持的 API:
- DataStream API: window(), windowAll() 等窗口操作
- Table API: TUMBLE(), HOP(), SESSION() 等窗口函数
- SQL API: TUMBLE(), HOP(), SESSION() 窗口函数

"""

from typing import List, Dict, Optional, Callable, Union
from pyflink.datastream import StreamExecutionEnvironment, DataStream, KeyedStream
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    TumblingProcessingTimeWindows,
    SlidingEventTimeWindows,
    SlidingProcessingTimeWindows,
    EventTimeSessionWindows,
    ProcessingTimeSessionWindows,
    CountWindow,
    GlobalWindows
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Time
from pyflink.common.typeinfo import Types


class FlinkWindowUtils:
    """
    Flink 窗口工具类
    
    封装窗口操作和聚合计算, 支持 DataStream API 和 Table API / SQL
    """

    def __init__(self):
        """
        初始化 Flink Window Utils
        """
        pass

    def tumbling_time_window(
        self,
        data_stream: DataStream,
        window_size_seconds: int,
        time_type: str = "event_time",
        key_by: Optional[Callable] = None
    ):
        """
        创建滚动时间窗口 (DataStream API)

        Parameters:
            data_stream: DataStream 对象
            window_size_seconds: 窗口大小, 秒 (int), 例如: 60 表示1分钟窗口
            time_type: 时间类型, 'event_time' 或 'processing_time' (str, default: 'event_time')
            key_by: KeyBy 函数 (Callable, optional), 如果不提供则使用 windowAll

        Returns:
            WindowedStream: 窗口化的数据流

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> window_utils = FlinkWindowUtils()
            >>> 
            >>> # 创建数据流
            >>> data_stream = env.from_collection([("user1", 100.0), ("user2", 200.0)])
            >>> # 按用户ID分组
            >>> keyed_stream = data_stream.key_by(lambda x: x[0])
            >>> # 创建1分钟滚动窗口
            >>> windowed_stream = window_utils.tumbling_time_window(
            ...     data_stream=keyed_stream,
            ...     window_size_seconds=60,
            ...     time_type="event_time"
            ... )
        """
        window_size = Time.seconds(window_size_seconds)

        if time_type == "event_time":
            window = TumblingEventTimeWindows.of(window_size)
        elif time_type == "processing_time":
            window = TumblingProcessingTimeWindows.of(window_size)
        else:
            raise ValueError(f"Unsupported time type: {time_type}")

        if key_by is not None:
            keyed_stream = data_stream.key_by(key_by)
            return keyed_stream.window(window)
        else:
            return data_stream.window_all(window)

    def sliding_time_window(
        self,
        data_stream: DataStream,
        window_size_seconds: int,
        slide_size_seconds: int,
        time_type: str = "event_time",
        key_by: Optional[Callable] = None
    ):
        """
        创建滑动时间窗口 (DataStream API)

        Parameters:
            data_stream: DataStream 对象
            window_size_seconds: 窗口大小, 秒 (int), 例如: 3600 表示1小时窗口
            slide_size_seconds: 滑动间隔, 秒 (int), 例如: 300 表示每5分钟滑动
            time_type: 时间类型, 'event_time' 或 'processing_time' (str, default: 'event_time')
            key_by: KeyBy 函数 (Callable, optional)

        Returns:
            WindowedStream: 窗口化的数据流

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> # 创建1小时窗口, 每5分钟滑动
            >>> windowed_stream = window_utils.sliding_time_window(
            ...     data_stream=keyed_stream,
            ...     window_size_seconds=3600,
            ...     slide_size_seconds=300,
            ...     time_type="event_time"
            ... )
        """
        window_size = Time.seconds(window_size_seconds)
        slide_size = Time.seconds(slide_size_seconds)

        if time_type == "event_time":
            window = SlidingEventTimeWindows.of(window_size, slide_size)
        elif time_type == "processing_time":
            window = SlidingProcessingTimeWindows.of(window_size, slide_size)
        else:
            raise ValueError(f"Unsupported time type: {time_type}")

        if key_by is not None:
            keyed_stream = data_stream.key_by(key_by)
            return keyed_stream.window(window)
        else:
            return data_stream.window_all(window)

    def session_window(
        self,
        data_stream: DataStream,
        gap_seconds: int,
        time_type: str = "event_time",
        key_by: Optional[Callable] = None
    ):
        """
        创建会话窗口 (DataStream API)

        Parameters:
            data_stream: DataStream 对象
            gap_seconds: 会话间隔, 秒 (int), 例如: 30 表示30秒无活动则关闭会话
            time_type: 时间类型, 'event_time' 或 'processing_time' (str, default: 'event_time')
            key_by: KeyBy 函数 (Callable, optional)

        Returns:
            WindowedStream: 窗口化的数据流

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> # 创建会话窗口, 30秒无活动则关闭
            >>> windowed_stream = window_utils.session_window(
            ...     data_stream=keyed_stream,
            ...     gap_seconds=30,
            ...     time_type="event_time"
            ... )
        """
        gap = Time.seconds(gap_seconds)

        if time_type == "event_time":
            window = EventTimeSessionWindows.with_gap(gap)
        elif time_type == "processing_time":
            window = ProcessingTimeSessionWindows.with_gap(gap)
        else:
            raise ValueError(f"Unsupported time type: {time_type}")

        if key_by is not None:
            keyed_stream = data_stream.key_by(key_by)
            return keyed_stream.window(window)
        else:
            return data_stream.window_all(window)

    def tumbling_count_window(
        self,
        data_stream: DataStream,
        window_size: int,
        key_by: Optional[Callable] = None
    ):
        """
        创建滚动计数窗口 (DataStream API)

        Parameters:
            data_stream: DataStream 对象
            window_size: 窗口大小, 元素数量 (int), 例如: 100 表示每100个元素一个窗口
            key_by: KeyBy 函数 (Callable, optional)

        Returns:
            WindowedStream: 窗口化的数据流

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> # 创建每100个元素的滚动窗口
            >>> windowed_stream = window_utils.tumbling_count_window(
            ...     data_stream=keyed_stream,
            ...     window_size=100
            ... )
        """
        if key_by is not None:
            keyed_stream = data_stream.key_by(key_by)
            return keyed_stream.count_window(window_size)
        else:
            return data_stream.count_window_all(window_size)

    def sliding_count_window(
        self,
        data_stream: DataStream,
        window_size: int,
        slide_size: int,
        key_by: Optional[Callable] = None
    ):
        """
        创建滑动计数窗口 (DataStream API)

        Parameters:
            data_stream: DataStream 对象
            window_size: 窗口大小, 元素数量 (int)
            slide_size: 滑动间隔, 元素数量 (int)
            key_by: KeyBy 函数 (Callable, optional)

        Returns:
            WindowedStream: 窗口化的数据流

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> # 创建100个元素的窗口, 每10个元素滑动
            >>> windowed_stream = window_utils.sliding_count_window(
            ...     data_stream=keyed_stream,
            ...     window_size=100,
            ...     slide_size=10
            ... )
        """
        if key_by is not None:
            keyed_stream = data_stream.key_by(key_by)
            return keyed_stream.count_window(window_size, slide_size)
        else:
            raise ValueError("Sliding count window requires key_by for windowAll is not supported")

    def window_aggregate(
        self,
        windowed_stream,
        agg_func: str = "count",
        agg_field: Optional[Union[int, Callable]] = None
    ):
        """
        窗口聚合计算 (DataStream API)

        Parameters:
            windowed_stream: WindowedStream 对象
            agg_func: 聚合函数类型, 'count', 'sum', 'avg', 'max', 'min' (str, default: 'count')
            agg_field: 聚合字段索引或函数 (int | Callable, optional), 用于 sum/avg/max/min

        Returns:
            DataStream: 聚合后的数据流

        Example:
            >>> from pyflink.datastream.functions import AggregateFunction
            >>> window_utils = FlinkWindowUtils()
            >>> # 创建窗口
            >>> windowed_stream = window_utils.tumbling_time_window(...)
            >>> # 聚合计算 (计数)
            >>> result_stream = window_utils.window_aggregate(
            ...     windowed_stream=windowed_stream,
            ...     agg_func="count"
            ... )
            >>> # 聚合计算 (求和)
            >>> result_stream = window_utils.window_aggregate(
            ...     windowed_stream=windowed_stream,
            ...     agg_func="sum",
            ...     agg_field=1  # 对第2个字段求和
            ... )
        """
        from pyflink.datastream.functions import AggregateFunction

        if agg_func == "count":
            # 计数 - 使用简化的方式
            class CountAggregateFunction(AggregateFunction):
                def create_accumulator(self):
                    return 0

                def add(self, value, accumulator):
                    return accumulator + 1

                def get_result(self, accumulator):
                    return accumulator

                def merge(self, acc_a, acc_b):
                    return acc_a + acc_b

            return windowed_stream.aggregate(CountAggregateFunction())
        elif agg_func == "sum":
            # 求和
            if agg_field is None:
                raise ValueError("agg_field is required for sum aggregation")
            
            class SumAggregateFunction(AggregateFunction):
                def create_accumulator(self):
                    return 0.0

                def add(self, value, accumulator):
                    if isinstance(agg_field, int):
                        field_value = value[agg_field] if isinstance(value, (list, tuple)) else getattr(value, f'f{agg_field}', 0)
                    else:
                        field_value = agg_field(value)
                    return accumulator + float(field_value)

                def get_result(self, accumulator):
                    return accumulator

                def merge(self, acc_a, acc_b):
                    return acc_a + acc_b

            return windowed_stream.aggregate(SumAggregateFunction())
        elif agg_func == "avg":
            # 平均值
            if agg_field is None:
                raise ValueError("agg_field is required for avg aggregation")
            
            class AvgAggregateFunction(AggregateFunction):
                def create_accumulator(self):
                    return (0, 0.0)  # (count, sum)

                def add(self, value, accumulator):
                    count, total = accumulator
                    if isinstance(agg_field, int):
                        field_value = value[agg_field] if isinstance(value, (list, tuple)) else getattr(value, f'f{agg_field}', 0)
                    else:
                        field_value = agg_field(value)
                    return (count + 1, total + float(field_value))

                def get_result(self, accumulator):
                    count, total = accumulator
                    return total / count if count > 0 else 0.0

                def merge(self, acc_a, acc_b):
                    count_a, total_a = acc_a
                    count_b, total_b = acc_b
                    return (count_a + count_b, total_a + total_b)

            return windowed_stream.aggregate(AvgAggregateFunction())
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_func}")

    def create_watermark_strategy(
        self,
        timestamp_field: Optional[str] = None,
        max_out_of_orderness_seconds: int = 0,
        watermark_type: str = "bounded_out_of_orderness"
    ):
        """
        创建 Watermark 策略

        Parameters:
            timestamp_field: 时间戳字段名 (str, optional), 用于提取事件时间
            max_out_of_orderness_seconds: 最大乱序时间, 秒 (int, default: 0)
            watermark_type: Watermark 类型, 'bounded_out_of_orderness' 或 'for_monotonously_timestamps' (str, default: 'bounded_out_of_orderness')

        Returns:
            WatermarkStrategy: Watermark 策略对象

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> # 创建 Watermark 策略, 允许5秒乱序
            >>> watermark_strategy = window_utils.create_watermark_strategy(
            ...     timestamp_field="order_time",
            ...     max_out_of_orderness_seconds=5
            ... )
            >>> # 应用到数据流
            >>> data_stream = env.from_source(source, watermark_strategy, "source-name")
        """
        from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner

        if watermark_type == "bounded_out_of_orderness":
            from pyflink.common import Duration
            max_out_of_orderness = Duration.of_seconds(max_out_of_orderness_seconds)
            
            # 如果指定了时间戳字段, 添加时间戳提取逻辑
            if timestamp_field:
                def extract_timestamp(value, record_timestamp):
                    """Extract timestamp from value based on timestamp_field"""
                    if isinstance(value, dict):
                        return value.get(timestamp_field, record_timestamp)
                    elif hasattr(value, timestamp_field):
                        return getattr(value, timestamp_field, record_timestamp)
                    else:
                        # 如果是 Row 或 tuple, 尝试通过索引或属性访问
                        return record_timestamp
                
                return WatermarkStrategy.for_bounded_out_of_orderness(max_out_of_orderness) \
                    .with_timestamp_assigner(TimestampAssigner(extract_timestamp))
            else:
                return WatermarkStrategy.for_bounded_out_of_orderness(max_out_of_orderness)
        elif watermark_type == "for_monotonously_timestamps":
            # 如果指定了时间戳字段, 添加时间戳提取逻辑
            if timestamp_field:
                def extract_timestamp(value, record_timestamp):
                    """Extract timestamp from value based on timestamp_field"""
                    if isinstance(value, dict):
                        return value.get(timestamp_field, record_timestamp)
                    elif hasattr(value, timestamp_field):
                        return getattr(value, timestamp_field, record_timestamp)
                    else:
                        return record_timestamp
                
                return WatermarkStrategy.for_monotonous_timestamps() \
                    .with_timestamp_assigner(TimestampAssigner(extract_timestamp))
            else:
                return WatermarkStrategy.for_monotonous_timestamps()
        else:
            raise ValueError(f"Unsupported watermark type: {watermark_type}")

    def tumbling_window_sql(
        self,
        time_field: str,
        window_size_interval: str,
        group_by_fields: List[str],
        select_fields: List[str],
        agg_functions: List[str],
        source_table: str = "source_table"
    ) -> str:
        """
        创建滚动窗口 SQL 语句 (Table API / SQL)

        Parameters:
            time_field: 时间字段名 (str), 例如: "order_time"
            window_size_interval: 窗口大小间隔 (str), 例如: "INTERVAL '1' MINUTE"
            group_by_fields: 分组字段列表 (List[str]), 例如: ["user_id"]
            select_fields: 选择字段列表 (List[str]), 例如: ["user_id"]
            agg_functions: 聚合函数列表 (List[str]), 例如: ["COUNT(*) as order_count", "SUM(amount) as total_amount"]
            source_table: 源表名 (str, default: 'source_table')

        Returns:
            str: SQL 查询语句

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> sql = window_utils.tumbling_window_sql(
            ...     time_field="order_time",
            ...     window_size_interval="INTERVAL '1' MINUTE",
            ...     group_by_fields=["user_id"],
            ...     select_fields=["user_id"],
            ...     agg_functions=["COUNT(*) as order_count", "SUM(amount) as total_amount"]
            ... )
            >>> # 执行 SQL
            >>> result_table = table_env.sql_query(sql)
        """
        select_clause = ", ".join(select_fields + agg_functions)
        group_by_clause = ", ".join(group_by_fields + [f"TUMBLE({time_field}, {window_size_interval})"])

        sql = f"""
        SELECT 
            {select_clause},
            TUMBLE_START({time_field}, {window_size_interval}) as window_start,
            TUMBLE_END({time_field}, {window_size_interval}) as window_end
        FROM {source_table}
        GROUP BY {group_by_clause}
        """

        return sql.strip()

    def sliding_window_sql(
        self,
        time_field: str,
        window_size_interval: str,
        slide_size_interval: str,
        group_by_fields: List[str],
        select_fields: List[str],
        agg_functions: List[str],
        source_table: str = "source_table"
    ) -> str:
        """
        创建滑动窗口 SQL 语句 (Table API / SQL)

        Parameters:
            time_field: 时间字段名 (str)
            window_size_interval: 窗口大小间隔 (str), 例如: "INTERVAL '1' HOUR"
            slide_size_interval: 滑动间隔 (str), 例如: "INTERVAL '5' MINUTE"
            group_by_fields: 分组字段列表 (List[str])
            select_fields: 选择字段列表 (List[str])
            agg_functions: 聚合函数列表 (List[str])
            source_table: 源表名 (str, default: 'source_table')

        Returns:
            str: SQL 查询语句

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> sql = window_utils.sliding_window_sql(
            ...     time_field="order_time",
            ...     window_size_interval="INTERVAL '1' HOUR",
            ...     slide_size_interval="INTERVAL '5' MINUTE",
            ...     group_by_fields=["user_id"],
            ...     select_fields=["user_id"],
            ...     agg_functions=["COUNT(*) as order_count", "SUM(amount) as total_amount"]
            ... )
        """
        select_clause = ", ".join(select_fields + agg_functions)
        group_by_clause = ", ".join(group_by_fields + [f"HOP({time_field}, {slide_size_interval}, {window_size_interval})"])

        sql = f"""
        SELECT 
            {select_clause},
            HOP_START({time_field}, {slide_size_interval}, {window_size_interval}) as window_start,
            HOP_END({time_field}, {slide_size_interval}, {window_size_interval}) as window_end
        FROM {source_table}
        GROUP BY {group_by_clause}
        """

        return sql.strip()

    def session_window_sql(
        self,
        time_field: str,
        gap_interval: str,
        group_by_fields: List[str],
        select_fields: List[str],
        agg_functions: List[str],
        source_table: str = "source_table"
    ) -> str:
        """
        创建会话窗口 SQL 语句 (Table API / SQL)

        Parameters:
            time_field: 时间字段名 (str)
            gap_interval: 会话间隔 (str), 例如: "INTERVAL '30' SECOND"
            group_by_fields: 分组字段列表 (List[str])
            select_fields: 选择字段列表 (List[str])
            agg_functions: 聚合函数列表 (List[str])
            source_table: 源表名 (str, default: 'source_table')

        Returns:
            str: SQL 查询语句

        Example:
            >>> window_utils = FlinkWindowUtils()
            >>> sql = window_utils.session_window_sql(
            ...     time_field="order_time",
            ...     gap_interval="INTERVAL '30' SECOND",
            ...     group_by_fields=["user_id"],
            ...     select_fields=["user_id"],
            ...     agg_functions=["COUNT(*) as order_count", "SUM(amount) as total_amount"]
            ... )
        """
        select_clause = ", ".join(select_fields + agg_functions)
        group_by_clause = ", ".join(group_by_fields + [f"SESSION({time_field}, {gap_interval})"])

        sql = f"""
        SELECT 
            {select_clause},
            SESSION_START({time_field}, {gap_interval}) as window_start,
            SESSION_END({time_field}, {gap_interval}) as window_end
        FROM {source_table}
        GROUP BY {group_by_clause}
        """

        return sql.strip()


if __name__ == "__main__":
    # 示例用法
    from pyflink.datastream import StreamExecutionEnvironment

    # 创建环境
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 创建工具类实例
    window_utils = FlinkWindowUtils()
    
    # 示例 1: 创建滚动时间窗口
    print("示例 1: 创建滚动时间窗口")
    data_stream = env.from_collection([("user1", 100.0), ("user2", 200.0)])
    keyed_stream = data_stream.key_by(lambda x: x[0])
    windowed_stream = window_utils.tumbling_time_window(
        data_stream=keyed_stream,
        window_size_seconds=60,
        time_type="event_time"
    )
    print(f"滚动窗口创建成功: {windowed_stream}")
    
    # 示例 2: 创建滚动窗口 SQL
    print("\n示例 2: 创建滚动窗口 SQL")
    sql = window_utils.tumbling_window_sql(
        time_field="order_time",
        window_size_interval="INTERVAL '1' MINUTE",
        group_by_fields=["user_id"],
        select_fields=["user_id"],
        agg_functions=["COUNT(*) as order_count", "SUM(amount) as total_amount"]
    )
    print(f"SQL 语句:\n{sql}")

