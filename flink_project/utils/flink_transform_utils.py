#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
apache-flink 1.17.0+

Flink 数据转换工具类 - Transform Utils

功能说明:
数据转换操作方法:

1. map_transform (DataStream API):
   - 功能: Map 转换 (字段映射、类型转换)
   - 用途: 一对一的数据转换

2. filter_data (DataStream API):
   - 功能: Filter 过滤 (条件过滤、数据清洗)
   - 用途: 过滤不符合条件的数据

3. flatmap_transform (DataStream API):
   - 功能: FlatMap 展开 (一对多转换)
   - 用途: 将一条数据转换为多条数据

4. key_by_field (DataStream API):
   - 功能: KeyBy 分组 (按字段分组)
   - 用途: 按指定字段对数据进行分组

5. filter_null_values (DataStream API):
   - 功能: 去除空值
   - 用途: 过滤掉包含 None 或空值的数据

6. validate_data_format (DataStream API):
   - 功能: 格式验证
   - 用途: 验证数据格式是否符合要求

7. handle_errors_with_side_output (DataStream API):
   - 功能: 异常数据处理 (侧输出流)
   - 用途: 将错误数据输出到侧输出流, 不影响主流程

8. select_fields_table_api (Table API):
   - 功能: 选择字段 (Table API)
   - 用途: 使用 Table API 选择需要的字段

9. filter_table_api (Table API):
   - 功能: 过滤数据 (Table API)
   - 用途: 使用 Table API 进行条件过滤

10. join_streams (DataStream API):
    - 功能: 流与流 Join
    - 用途: 关联两个数据流

11. join_with_lookup_table (Table API):
    - 功能: 流与表 Join (Lookup Join)
    - 用途: 关联数据流和维表

12. convert_datastream_to_table:
    - 功能: DataStream 转 Table
    - 用途: DataStream API 与 Table API 互转

13. convert_table_to_datastream:
    - 功能: Table 转 DataStream
    - 用途: Table API 与 DataStream API 互转

支持的 API:
- DataStream API: map, filter, flatMap, keyBy 等操作
- Table API: select, where, groupBy, join 等操作
- 支持两种 API 互转

"""

from typing import List, Dict, Optional, Callable, Tuple
from pyflink.datastream import StreamExecutionEnvironment, DataStream, KeyedStream
from pyflink.datastream.functions import MapFunction, FilterFunction, FlatMapFunction
from pyflink.table import TableEnvironment, StreamTableEnvironment, Table
from pyflink.common.typeinfo import TypeInformation, Types
from pyflink.common import Row


class FlinkTransformUtils:
    """
    Flink 数据转换工具类
    
    封装常用数据转换操作, 支持 DataStream API 和 Table API
    """

    def __init__(self):
        """
        初始化 Flink Transform Utils
        """
        pass

    def map_transform(
        self,
        data_stream: DataStream,
        map_func: Callable
    ) -> DataStream:
        """
        Map 转换 (字段映射、类型转换)

        Parameters:
            data_stream: DataStream 对象
            map_func: 映射函数 (Callable), 接收一个元素, 返回转换后的元素

        Returns:
            DataStream: 转换后的数据流

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> transform_utils = FlinkTransformUtils()
            >>> 
            >>> # 创建数据流
            >>> data_stream = env.from_collection([("order1", 100.0), ("order2", 200.0)])
            >>> # Map 转换: 提取金额并转换为元
            >>> result_stream = transform_utils.map_transform(
            ...     data_stream=data_stream,
            ...     map_func=lambda x: (x[0], x[1] / 100.0)  # 转换为元
            ... )
        """
        return data_stream.map(map_func)

    def filter_data(
        self,
        data_stream: DataStream,
        filter_func: Callable
    ) -> DataStream:
        """
        Filter 过滤 (条件过滤、数据清洗)

        Parameters:
            data_stream: DataStream 对象
            filter_func: 过滤函数 (Callable), 返回 True 保留, False 过滤

        Returns:
            DataStream: 过滤后的数据流

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 过滤金额大于100的订单
            >>> filtered_stream = transform_utils.filter_data(
            ...     data_stream=data_stream,
            ...     filter_func=lambda x: x[1] > 100.0
            ... )
        """
        return data_stream.filter(filter_func)

    def flatmap_transform(
        self,
        data_stream: DataStream,
        flatmap_func: Callable
    ) -> DataStream:
        """
        FlatMap 展开 (一对多转换)

        Parameters:
            data_stream: DataStream 对象
            flatmap_func: FlatMap 函数 (Callable), 接收一个元素, 返回可迭代对象

        Returns:
            DataStream: 展开后的数据流

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 将订单拆分为订单明细
            >>> def split_order(order):
            ...     order_id, items = order
            ...     for item in items:
            ...         yield (order_id, item)
            >>> 
            >>> expanded_stream = transform_utils.flatmap_transform(
            ...     data_stream=data_stream,
            ...     flatmap_func=split_order
            ... )
        """
        return data_stream.flat_map(flatmap_func)

    def key_by_field(
        self,
        data_stream: DataStream,
        key_func: Callable
    ) -> KeyedStream:
        """
        KeyBy 分组 (按字段分组)

        Parameters:
            data_stream: DataStream 对象
            key_func: Key 提取函数 (Callable), 从元素中提取 key

        Returns:
            KeyedStream: 分组后的数据流

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 按用户ID分组
            >>> keyed_stream = transform_utils.key_by_field(
            ...     data_stream=data_stream,
            ...     key_func=lambda x: x[1]  # 假设第2个字段是user_id
            ... )
        """
        return data_stream.key_by(key_func)

    def filter_null_values(
        self,
        data_stream: DataStream
    ) -> DataStream:
        """
        去除空值

        Parameters:
            data_stream: DataStream 对象

        Returns:
            DataStream: 过滤空值后的数据流

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 过滤掉包含 None 的数据
            >>> cleaned_stream = transform_utils.filter_null_values(data_stream)
        """
        def is_not_null(value):
            if value is None:
                return False
            if isinstance(value, (list, tuple)):
                return all(v is not None for v in value)
            return True

        return data_stream.filter(is_not_null)

    def validate_data_format(
        self,
        data_stream: DataStream,
        validator_func: Callable
    ) -> DataStream:
        """
        格式验证

        Parameters:
            data_stream: DataStream 对象
            validator_func: 验证函数 (Callable), 返回 True 表示格式正确, False 表示格式错误

        Returns:
            DataStream: 验证通过的数据流

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 验证订单数据格式
            >>> def validate_order(order):
            ...     if not isinstance(order, (list, tuple)):
            ...         return False
            ...     if len(order) < 3:
            ...         return False
            ...     return order[2] > 0  # 金额必须大于0
            >>> 
            >>> validated_stream = transform_utils.validate_data_format(
            ...     data_stream=data_stream,
            ...     validator_func=validate_order
            ... )
        """
        return data_stream.filter(validator_func)

    def handle_errors_with_side_output(
        self,
        data_stream: DataStream,
        process_func: Callable,
        error_output_tag: Optional[str] = None
    ) -> Tuple[DataStream, DataStream]:
        """
        异常数据处理 (侧输出流)

        Parameters:
            data_stream: DataStream 对象
            process_func: 处理函数 (Callable), 可能抛出异常
            error_output_tag: 错误输出标签 (str, optional)

        Returns:
            Tuple[DataStream, DataStream]: (主数据流, 错误数据流)

        Example:
            >>> from pyflink.common import OutputTag
            >>> transform_utils = FlinkTransformUtils()
            >>> 
            >>> # 定义错误输出标签
            >>> error_tag = OutputTag("errors")
            >>> 
            >>> # 处理数据, 错误数据输出到侧输出流
            >>> def process_with_error_handling(value):
            ...     try:
            ...         # 处理逻辑
            ...         return process_value(value)
            ...     except Exception as e:
            ...         # 错误数据输出到侧输出流
            ...         ctx.output(error_tag, (value, str(e)))
            ...         return None
            >>> 
            >>> main_stream, error_stream = transform_utils.handle_errors_with_side_output(
            ...     data_stream=data_stream,
            ...     process_func=process_with_error_handling
            ... )
        """
        from pyflink.common import OutputTag
        from pyflink.datastream.functions import ProcessFunction

        if error_output_tag is None:
            error_output_tag = "errors"

        error_tag = OutputTag(error_output_tag)

        class ErrorHandlingProcessFunction(ProcessFunction):
            def process_element(self, value, ctx, out):
                try:
                    result = process_func(value)
                    if result is not None:
                        out.collect(result)
                except Exception as e:
                    ctx.output(error_tag, (value, str(e)))

        processed_stream = data_stream.process(ErrorHandlingProcessFunction())
        error_stream = processed_stream.get_side_output(error_tag)

        return processed_stream, error_stream

    def select_fields_table_api(
        self,
        table: Table,
        fields: List[str]
    ) -> Table:
        """
        选择字段 (Table API)

        Parameters:
            table: Table 对象
            fields: 字段列表 (List[str]), 例如: ["order_id", "user_id", "amount"]

        Returns:
            Table: 选择字段后的 Table

        Example:
            >>> from pyflink.table import TableEnvironment
            >>> table_env = TableEnvironment.create(...)
            >>> transform_utils = FlinkTransformUtils()
            >>> 
            >>> # 从表中选择字段
            >>> source_table = table_env.from_path("order_source")
            >>> selected_table = transform_utils.select_fields_table_api(
            ...     table=source_table,
            ...     fields=["order_id", "user_id", "amount"]
            ... )
        """
        fields_str = ", ".join(fields)
        return table.select(fields_str)

    def filter_table_api(
        self,
        table: Table,
        condition: str
    ) -> Table:
        """
        过滤数据 (Table API)

        Parameters:
            table: Table 对象
            condition: 过滤条件 (str), 例如: "amount > 100 AND user_id IS NOT NULL"

        Returns:
            Table: 过滤后的 Table

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 过滤金额大于100的订单
            >>> filtered_table = transform_utils.filter_table_api(
            ...     table=source_table,
            ...     condition="amount > 100"
            ... )
        """
        return table.where(condition)

    def join_streams(
        self,
        stream1: DataStream,
        stream2: DataStream,
        key_func1: Callable,
        key_func2: Callable,
        window_size_seconds: int = 60
    ) -> DataStream:
        """
        流与流 Join (时间窗口 Join)

        Parameters:
            stream1: 第一个数据流 (DataStream)
            stream2: 第二个数据流 (DataStream)
            key_func1: 第一个流的 Key 提取函数 (Callable)
            key_func2: 第二个流的 Key 提取函数 (Callable)
            window_size_seconds: 窗口大小, 秒 (int, default: 60)

        Returns:
            DataStream: Join 后的数据流

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 关联订单流和用户流
            >>> joined_stream = transform_utils.join_streams(
            ...     stream1=order_stream,
            ...     stream2=user_stream,
            ...     key_func1=lambda x: x[1],  # 订单流的user_id
            ...     key_func2=lambda x: x[0],  # 用户流的user_id
            ...     window_size_seconds=60
            ... )
        """
        from pyflink.common import Time
        from pyflink.datastream.window import TumblingEventTimeWindows
        from pyflink.datastream.functions import JoinFunction

        keyed_stream1 = stream1.key_by(key_func1)
        keyed_stream2 = stream2.key_by(key_func2)

        window = TumblingEventTimeWindows.of(Time.seconds(window_size_seconds))

        # 使用 join with join function
        # 注意: Flink 的 join 需要两个流有相同的 key 类型, 并且需要提供 join function
        # 由于两个流已经通过 key_by 进行了分组, 这里直接使用 window join
        class DefaultJoinFunction(JoinFunction):
            """Default join function to combine values from two streams"""
            def join(self, value1, value2):
                # 默认实现: 返回元组, 实际使用时需要根据业务需求调整
                return (value1, value2)

        # Flink join API: 两个 keyed stream 在相同 key 上 join, 然后应用窗口和 join function
        return keyed_stream1.join(keyed_stream2).window(window).apply(DefaultJoinFunction())

    def join_with_lookup_table_sql(
        self,
        table_env: TableEnvironment,
        stream_table: str,
        lookup_table: str,
        join_condition: str,
        select_fields: List[str]
    ) -> str:
        """
        流与表 Join (Lookup Join) SQL 语句

        Parameters:
            table_env: TableEnvironment 对象
            stream_table: 流表名 (str)
            lookup_table: 维表名 (str)
            join_condition: Join 条件 (str), 例如: "stream_table.user_id = lookup_table.user_id"
            select_fields: 选择字段列表 (List[str])

        Returns:
            str: SQL 查询语句

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> sql = transform_utils.join_with_lookup_table_sql(
            ...     table_env=table_env,
            ...     stream_table="order_source",
            ...     lookup_table="user_info",
            ...     join_condition="order_source.user_id = user_info.user_id",
            ...     select_fields=["order_source.order_id", "order_source.amount", "user_info.user_name"]
            ... )
            >>> result_table = table_env.sql_query(sql)
        """
        select_clause = ", ".join(select_fields)
        sql = f"""
        SELECT {select_clause}
        FROM {stream_table}
        LEFT JOIN {lookup_table} FOR SYSTEM_TIME AS OF {stream_table}.proctime AS lookup
        ON {join_condition}
        """

        return sql.strip()

    def convert_datastream_to_table(
        self,
        table_env: StreamTableEnvironment,
        data_stream: DataStream,
        schema: Optional[List[str]] = None
    ) -> Table:
        """
        DataStream 转 Table

        Parameters:
            table_env: StreamTableEnvironment 对象
            data_stream: DataStream 对象
            schema: 表结构定义 (List[str], optional), 字段名列表

        Returns:
            Table: Table 对象

        Example:
            >>> from pyflink.table import StreamTableEnvironment
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> table_env = StreamTableEnvironment.create(env)
            >>> transform_utils = FlinkTransformUtils()
            >>> 
            >>> # 从 DataStream 创建 Table
            >>> data_stream = env.from_collection([("order1", "user1", 100.0)])
            >>> table = transform_utils.convert_datastream_to_table(
            ...     table_env=table_env,
            ...     data_stream=data_stream,
            ...     schema=["order_id", "user_id", "amount"]
            ... )
        """
        if schema is None:
            return table_env.from_data_stream(data_stream)
        else:
            return table_env.from_data_stream(data_stream, schema)

    def convert_table_to_datastream(
        self,
        table_env: StreamTableEnvironment,
        table: Table,
        type_info: Optional[TypeInformation] = None
    ) -> DataStream:
        """
        Table 转 DataStream

        Parameters:
            table_env: StreamTableEnvironment 对象
            table: Table 对象
            type_info: 类型信息 (TypeInformation, optional)

        Returns:
            DataStream: DataStream 对象

        Example:
            >>> transform_utils = FlinkTransformUtils()
            >>> # 从 Table 创建 DataStream
            >>> result_table = table_env.sql_query("SELECT * FROM order_source WHERE amount > 100")
            >>> data_stream = transform_utils.convert_table_to_datastream(
            ...     table_env=table_env,
            ...     table=result_table
            ... )
        """
        if type_info is None:
            return table_env.to_data_stream(table)
        else:
            return table_env.to_data_stream(table, type_info)


if __name__ == "__main__":
    # 示例用法
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    # 创建环境
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)
    
    # 创建工具类实例
    transform_utils = FlinkTransformUtils()
    
    # 示例 1: Map 转换
    print("示例 1: Map 转换")
    data_stream = env.from_collection([("order1", 100.0), ("order2", 200.0)])
    result_stream = transform_utils.map_transform(
        data_stream=data_stream,
        map_func=lambda x: (x[0], x[1] / 100.0)
    )
    print(f"Map 转换成功: {result_stream}")
    
    # 示例 2: Filter 过滤
    print("\n示例 2: Filter 过滤")
    filtered_stream = transform_utils.filter_data(
        data_stream=data_stream,
        filter_func=lambda x: x[1] > 100.0
    )
    print(f"Filter 过滤成功: {filtered_stream}")

