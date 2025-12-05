#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
apache-flink 1.17.0+

Flink Table API 工具类 - Table API Utils

功能说明:
Table API / SQL 操作方法:

1. create_table_environment:
   - 功能: 创建 Table API 环境 (流处理或批处理)
   - 用途: 初始化 Table API 执行环境

2. create_table_from_source:
   - 功能: 从 Source 创建 Table
   - 用途: 将数据源转换为 Table

3. create_table_from_datastream:
   - 功能: 从 DataStream 创建 Table
   - 用途: DataStream API 与 Table API 互转

4. execute_sql_query:
   - 功能: 执行 SQL 查询
   - 用途: 使用 SQL 进行数据查询和转换

5. execute_sql_insert:
   - 功能: 执行 SQL 插入
   - 用途: 将查询结果写入目标表

6. convert_table_to_datastream:
   - 功能: 将 Table 转换为 DataStream
   - 用途: Table API 与 DataStream API 互转

7. register_temporary_table:
   - 功能: 注册临时表
   - 用途: 在 SQL 中使用临时表

支持的 API:
- Table API: 声明式 API, 类似关系型数据库
- SQL API: 标准 SQL 查询

"""

from typing import List, Dict, Optional, Union
from pyflink.table import (
    TableEnvironment,
    EnvironmentSettings,
    StreamTableEnvironment,
    Table
)
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.common.typeinfo import TypeInformation


class FlinkTableApiUtils:
    """
    Flink Table API 工具类
    
    封装 Table API / SQL 操作, 支持流处理和批处理统一
    """

    def __init__(self):
        """
        初始化 Flink Table API Utils
        """
        pass

    def create_table_environment(
        self,
        execution_mode: str = "streaming",
        **kwargs
    ) -> TableEnvironment:
        """
        创建 Table API 环境

        Parameters:
            execution_mode: 执行模式, 'streaming' 或 'batch' (str, default: 'streaming')
            **kwargs: 其他环境配置参数

        Returns:
            TableEnvironment: Table API 环境对象

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> # 创建流处理环境
            >>> table_env = table_utils.create_table_environment(execution_mode="streaming")
            >>> # 创建批处理环境
            >>> batch_env = table_utils.create_table_environment(execution_mode="batch")
        """
        if execution_mode == "streaming":
            env_settings = EnvironmentSettings.in_streaming_mode()
        elif execution_mode == "batch":
            env_settings = EnvironmentSettings.in_batch_mode()
        else:
            raise ValueError(f"Unsupported execution mode: {execution_mode}")

        # 应用额外配置
        for key, value in kwargs.items():
            if hasattr(env_settings, key):
                setattr(env_settings, key, value)

        table_env = TableEnvironment.create(env_settings)
        return table_env

    def create_stream_table_environment(
        self,
        env: StreamExecutionEnvironment
    ) -> StreamTableEnvironment:
        """
        从 DataStream 环境创建 StreamTableEnvironment

        Parameters:
            env: StreamExecutionEnvironment 对象

        Returns:
            StreamTableEnvironment: Stream Table API 环境对象

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_stream_table_environment(env)
        """
        from pyflink.table import StreamTableEnvironment
        return StreamTableEnvironment.create(env)

    def create_table_from_datastream(
        self,
        table_env: StreamTableEnvironment,
        data_stream: DataStream,
        schema: Union[List[str], str, None] = None,
        **kwargs
    ) -> Table:
        """
        从 DataStream 创建 Table

        Parameters:
            table_env: StreamTableEnvironment 对象
            data_stream: DataStream 对象
            schema: 表结构定义, 可以是字段名列表或 DDL 字符串 (List[str] | str | None, optional)
            **kwargs: 其他转换配置参数

        Returns:
            Table: Table 对象

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_stream_table_environment(env)
            >>> # 从 DataStream 创建 Table
            >>> data_stream = env.from_collection([("order1", "user1", 100.0)])
            >>> table = table_utils.create_table_from_datastream(
            ...     table_env=table_env,
            ...     data_stream=data_stream,
            ...     schema=["order_id", "user_id", "amount"]
            ... )
        """
        if schema is None:
            # 如果没有指定 schema, 使用默认转换
            table = table_env.from_data_stream(data_stream)
        elif isinstance(schema, list):
            # 字段名列表
            table = table_env.from_data_stream(data_stream, schema)
        elif isinstance(schema, str):
            # DDL 字符串, 需要先注册临时视图
            table_env.create_temporary_view("temp_stream", data_stream)
            table = table_env.from_path("temp_stream")
        else:
            table = table_env.from_data_stream(data_stream)

        return table

    def convert_table_to_datastream(
        self,
        table_env: StreamTableEnvironment,
        table: Table,
        type_info: Optional[TypeInformation] = None
    ) -> DataStream:
        """
        将 Table 转换为 DataStream

        Parameters:
            table_env: StreamTableEnvironment 对象
            table: Table 对象
            type_info: 类型信息 (TypeInformation, optional)

        Returns:
            DataStream: DataStream 对象

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_table_environment()
            >>> # 执行查询得到 Table
            >>> result_table = table_env.sql_query("SELECT * FROM order_source WHERE amount > 100")
            >>> # 转换为 DataStream
            >>> result_stream = table_utils.convert_table_to_datastream(
            ...     table_env=table_env,
            ...     table=result_table
            ... )
        """
        if type_info is None:
            return table_env.to_data_stream(table)
        else:
            return table_env.to_data_stream(table, type_info)

    def execute_sql_query(
        self,
        table_env: TableEnvironment,
        sql: str
    ) -> Table:
        """
        执行 SQL 查询

        Parameters:
            table_env: TableEnvironment 对象
            sql: SQL 查询语句 (str)

        Returns:
            Table: 查询结果 Table 对象

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_table_environment()
            >>> # 执行 SQL 查询
            >>> result_table = table_utils.execute_sql_query(
            ...     table_env=table_env,
            ...     sql="SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount FROM order_source GROUP BY user_id"
            ... )
        """
        return table_env.sql_query(sql)

    def execute_sql_ddl(
        self,
        table_env: TableEnvironment,
        ddl: str
    ):
        '''
        执行 SQL DDL 语句

        Parameters:
            table_env: TableEnvironment 对象
            ddl: DDL 语句 (str), 例如 CREATE TABLE, CREATE VIEW 等

        Returns:
            None

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_table_environment()
            >>> # 执行 DDL 创建表
            >>> table_utils.execute_sql_ddl(
            ...     table_env=table_env,
            ...     ddl="""
            ...     CREATE TABLE order_source (
            ...         order_id STRING,
            ...         user_id STRING,
            ...         amount DECIMAL(10,2)
            ...     ) WITH (
            ...         'connector' = 'kafka',
            ...         'topic' = 'order-raw',
            ...         'properties.bootstrap.servers' = 'localhost:29092',
            ...         'format' = 'json'
            ...     )
            ...     """
            ... )
        '''
        table_env.execute_sql(ddl)

    def execute_sql_insert(
        self,
        table_env: TableEnvironment,
        insert_sql: str
    ):
        """
        执行 SQL INSERT 语句

        Parameters:
            table_env: TableEnvironment 对象
            insert_sql: INSERT SQL 语句 (str)

        Returns:
            JobClient: 作业客户端对象 (用于流处理) 或 None (用于批处理)

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_table_environment()
            >>> # 执行 INSERT 语句
            >>> table_utils.execute_sql_insert(
            ...     table_env=table_env,
            ...     insert_sql="INSERT INTO order_sink SELECT * FROM order_source WHERE amount > 100"
            ... )
        """
        return table_env.execute_sql(insert_sql)

    def register_temporary_table(
        self,
        table_env: TableEnvironment,
        table_name: str,
        table: Table
    ):
        """
        注册临时表

        Parameters:
            table_env: TableEnvironment 对象
            table_name: 临时表名称 (str)
            table: Table 对象

        Returns:
            None

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_table_environment()
            >>> # 从 DataStream 创建 Table
            >>> table = table_utils.create_table_from_datastream(...)
            >>> # 注册为临时表
            >>> table_utils.register_temporary_table(
            ...     table_env=table_env,
            ...     table_name="temp_order_table",
            ...     table=table
            ... )
            >>> # 在 SQL 中使用
            >>> result = table_env.sql_query("SELECT * FROM temp_order_table")
        """
        table_env.create_temporary_view(table_name, table)

    def create_table_from_path(
        self,
        table_env: TableEnvironment,
        table_path: str
    ) -> Table:
        """
        从已注册的表路径创建 Table

        Parameters:
            table_env: TableEnvironment 对象
            table_path: 表路径名称 (str), 例如已注册的表名或视图名

        Returns:
            Table: Table 对象

        Example:
            >>> table_utils = FlinkTableApiUtils()
            >>> table_env = table_utils.create_table_environment()
            >>> # 从已注册的表创建 Table
            >>> table = table_utils.create_table_from_path(
            ...     table_env=table_env,
            ...     table_path="order_source"
            ... )
        """
        return table_env.from_path(table_path)


if __name__ == "__main__":
    # 示例用法
    from pyflink.datastream import StreamExecutionEnvironment

    # 创建环境
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 创建工具类实例
    table_utils = FlinkTableApiUtils()
    
    # 示例 1: 创建 Table API 环境
    print("示例 1: 创建 Table API 环境")
    table_env = table_utils.create_table_environment(execution_mode="streaming")
    print(f"Table Environment 创建成功: {table_env}")
    
    # 示例 2: 从 DataStream 创建 Table
    print("\n示例 2: 从 DataStream 创建 Table")
    stream_table_env = table_utils.create_stream_table_environment(env)
    data_stream = env.from_collection([("order1", "user1", 100.0)])
    table = table_utils.create_table_from_datastream(
        table_env=stream_table_env,
        data_stream=data_stream,
        schema=["order_id", "user_id", "amount"]
    )
    print(f"Table 创建成功: {table}")

