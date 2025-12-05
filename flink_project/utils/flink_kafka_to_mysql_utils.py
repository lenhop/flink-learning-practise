#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
apache-flink 1.17.0+

Flink Kafka 到 MySQL 数据管道工具类 - Kafka to MySQL Utils

功能说明:
Kafka → Flink → MySQL 数据管道方法:

1. create_etl_pipeline_datastream:
   - 功能: 创建 DataStream API 的 ETL 管道
   - 用途: 从 Kafka 读取数据, 转换后写入 MySQL

2. create_etl_pipeline_table_api:
   - 功能: 创建 Table API / SQL 的 ETL 管道
   - 用途: 使用 SQL 方式实现 ETL 流程

3. create_etl_pipeline_with_transform:
   - 功能: 创建带数据转换的 ETL 管道
   - 用途: 支持自定义数据转换逻辑

4. create_etl_pipeline_batch:
   - 功能: 创建批量处理 ETL 管道
   - 用途: 批量数据迁移场景

5. create_etl_pipeline_with_error_handling:
   - 功能: 创建带错误处理的 ETL 管道
   - 用途: 生产环境, 需要错误处理和重试

支持的 API:
- DataStream API: 灵活的数据处理流程
- Table API: SQL 化的 ETL 流程

"""

import json
from typing import List, Dict, Optional, Callable
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import TableEnvironment, EnvironmentSettings

# 导入其他工具类
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from flink_source_utils import FlinkSourceUtils
from flink_sink_utils import FlinkSinkUtils
from flink_table_api_utils import FlinkTableApiUtils


class FlinkKafkaToMysqlUtils:
    """
    Flink Kafka 到 MySQL 数据管道工具类
    
    封装 Kafka → Flink → MySQL 的完整 ETL 流程
    """

    def __init__(self):
        """
        初始化 Flink Kafka to MySQL Utils
        """
        self.source_utils = FlinkSourceUtils()
        self.sink_utils = FlinkSinkUtils()
        self.table_utils = FlinkTableApiUtils()

    def create_etl_pipeline_datastream(
        self,
        env: StreamExecutionEnvironment,
        kafka_topic: str,
        kafka_bootstrap_servers: List[str],
        mysql_url: str,
        mysql_driver: str,
        mysql_username: str,
        mysql_password: str,
        mysql_table: str,
        mysql_insert_sql: str,
        transform_func: Optional[Callable] = None,
        **kwargs
    ):
        """
        创建 DataStream API 的 ETL 管道

        Parameters:
            env: StreamExecutionEnvironment 对象
            kafka_topic: Kafka topic 名称 (str)
            kafka_bootstrap_servers: Kafka broker 地址列表 (List[str])
            mysql_url: MySQL JDBC URL (str), 例如: "jdbc:mysql://localhost:3306/testdb"
            mysql_driver: MySQL JDBC 驱动类名 (str), 例如: "com.mysql.cj.jdbc.Driver"
            mysql_username: MySQL 用户名 (str)
            mysql_password: MySQL 密码 (str)
            mysql_table: MySQL 表名 (str)
            mysql_insert_sql: INSERT SQL 语句 (str), 例如: "INSERT INTO order_header (order_id, user_id, amount) VALUES (?, ?, ?)"
            transform_func: 数据转换函数 (Callable, optional), 接收消息字符串, 返回转换后的数据
            **kwargs: 其他配置参数

        Returns:
            None (直接执行作业)

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> pipeline_utils = FlinkKafkaToMysqlUtils()
            >>> 
            >>> def transform_order(message_str):
            ...     data = json.loads(message_str)
            ...     return (data['order_id'], data['user_id'], data['amount'])
            >>> 
            >>> pipeline_utils.create_etl_pipeline_datastream(
            ...     env=env,
            ...     kafka_topic="order-raw",
            ...     kafka_bootstrap_servers=["localhost:29092"],
            ...     mysql_url="jdbc:mysql://localhost:3306/testdb",
            ...     mysql_driver="com.mysql.cj.jdbc.Driver",
            ...     mysql_username="root",
            ...     mysql_password="password",
            ...     mysql_table="order_header",
            ...     mysql_insert_sql="INSERT INTO order_header (order_id, user_id, amount) VALUES (?, ?, ?)",
            ...     transform_func=transform_order
            ... )
            >>> env.execute("Kafka to MySQL ETL")
        """
        # 创建 Kafka Source
        kafka_source = self.source_utils.create_kafka_source(
            env=env,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            offset=kwargs.get('kafka_offset', 'earliest')
        )

        # 从 Kafka Source 创建数据流
        data_stream = env.from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            "kafka-source"
        )

        # 数据转换
        if transform_func:
            data_stream = data_stream.map(transform_func)
        else:
            # 默认转换: JSON 字符串解析
            def default_transform(message_str):
                data = json.loads(message_str)
                return (data.get('order_id'), data.get('user_id'), data.get('amount', 0.0))

            data_stream = data_stream.map(default_transform)

        # 创建 MySQL Sink
        mysql_sink = self.sink_utils.create_mysql_sink(
            url=mysql_url,
            driver=mysql_driver,
            username=mysql_username,
            password=mysql_password,
            table_name=mysql_table,
            sql=mysql_insert_sql,
            batch_size=kwargs.get('batch_size', 1000),
            batch_interval_ms=kwargs.get('batch_interval_ms', 2000),
            max_retries=kwargs.get('max_retries', 3)
        )

        # 添加 Sink
        data_stream.add_sink(mysql_sink)

    def create_etl_pipeline_table_api(
        self,
        kafka_topic: str,
        kafka_bootstrap_servers: List[str],
        kafka_schema: str,
        mysql_url: str,
        mysql_driver: str,
        mysql_username: str,
        mysql_password: str,
        mysql_table: str,
        mysql_schema: str,
        transform_sql: Optional[str] = None,
        execution_mode: str = "streaming",
        **kwargs
    ):
        """
        创建 Table API / SQL 的 ETL 管道

        Parameters:
            kafka_topic: Kafka topic 名称 (str)
            kafka_bootstrap_servers: Kafka broker 地址列表 (List[str])
            kafka_schema: Kafka 表结构定义 (str), 例如: "order_id STRING, user_id STRING, amount DECIMAL(10,2)"
            mysql_url: MySQL JDBC URL (str)
            mysql_driver: MySQL JDBC 驱动类名 (str)
            mysql_username: MySQL 用户名 (str)
            mysql_password: MySQL 密码 (str)
            mysql_table: MySQL 表名 (str)
            mysql_schema: MySQL 表结构定义 (str), 例如: "order_id STRING, user_id STRING, amount DECIMAL(10,2), PRIMARY KEY (order_id) NOT ENFORCED"
            transform_sql: 数据转换 SQL (str, optional), 例如: "SELECT order_id, user_id, amount FROM order_source WHERE amount > 0"
            execution_mode: 执行模式, 'streaming' 或 'batch' (str, default: 'streaming')
            **kwargs: 其他配置参数

        Returns:
            TableEnvironment: Table API 环境对象

        Example:
            >>> pipeline_utils = FlinkKafkaToMysqlUtils()
            >>> table_env = pipeline_utils.create_etl_pipeline_table_api(
            ...     kafka_topic="order-raw",
            ...     kafka_bootstrap_servers=["localhost:29092"],
            ...     kafka_schema="order_id STRING, user_id STRING, amount DECIMAL(10,2), order_time TIMESTAMP(3)",
            ...     mysql_url="jdbc:mysql://localhost:3306/testdb",
            ...     mysql_driver="com.mysql.cj.jdbc.Driver",
            ...     mysql_username="root",
            ...     mysql_password="password",
            ...     mysql_table="order_header",
            ...     mysql_schema="order_id STRING, user_id STRING, amount DECIMAL(10,2), PRIMARY KEY (order_id) NOT ENFORCED",
            ...     transform_sql="SELECT order_id, user_id, amount FROM order_source WHERE amount > 0"
            ... )
            >>> # 执行插入
            >>> table_env.execute_sql("INSERT INTO order_sink SELECT * FROM order_cleaned")
        """
        # 创建 Table API 环境
        table_env = self.table_utils.create_table_environment(execution_mode=execution_mode)

        # 创建 Kafka Source Table
        kafka_source_ddl = self.source_utils.create_kafka_table_source_ddl(
            table_name="order_source",
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            schema=kafka_schema,
            format_type=kwargs.get('kafka_format', 'json')
        )
        table_env.execute_sql(kafka_source_ddl)

        # 创建 MySQL Sink Table
        mysql_sink_ddl = self.sink_utils.create_mysql_table_sink_ddl(
            table_name="order_sink",
            url=mysql_url,
            driver=mysql_driver,
            username=mysql_username,
            password=mysql_password,
            db_table=mysql_table,
            schema=mysql_schema
        )
        table_env.execute_sql(mysql_sink_ddl)

        # 数据转换
        if transform_sql:
            # 创建转换后的临时表
            cleaned_table_ddl = f"""
            CREATE TEMPORARY VIEW order_cleaned AS
            {transform_sql}
            """
            table_env.execute_sql(cleaned_table_ddl)
            insert_sql = "INSERT INTO order_sink SELECT * FROM order_cleaned"
        else:
            insert_sql = "INSERT INTO order_sink SELECT * FROM order_source"

        # 执行插入 (返回 JobClient, 可以用于监控)
        return table_env, insert_sql

    def create_etl_pipeline_with_transform(
        self,
        env: StreamExecutionEnvironment,
        kafka_topic: str,
        kafka_bootstrap_servers: List[str],
        mysql_url: str,
        mysql_driver: str,
        mysql_username: str,
        mysql_password: str,
        mysql_table: str,
        mysql_insert_sql: str,
        filter_func: Optional[Callable] = None,
        map_func: Optional[Callable] = None,
        **kwargs
    ):
        """
        创建带数据转换的 ETL 管道

        Parameters:
            env: StreamExecutionEnvironment 对象
            kafka_topic: Kafka topic 名称 (str)
            kafka_bootstrap_servers: Kafka broker 地址列表 (List[str])
            mysql_url: MySQL JDBC URL (str)
            mysql_driver: MySQL JDBC 驱动类名 (str)
            mysql_username: MySQL 用户名 (str)
            mysql_password: MySQL 密码 (str)
            mysql_table: MySQL 表名 (str)
            mysql_insert_sql: INSERT SQL 语句 (str)
            filter_func: 数据过滤函数 (Callable, optional), 返回 True 保留, False 过滤
            map_func: 数据映射函数 (Callable, optional), 接收消息, 返回转换后的数据
            **kwargs: 其他配置参数

        Returns:
            None (直接执行作业)

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> pipeline_utils = FlinkKafkaToMysqlUtils()
            >>> 
            >>> def filter_valid_order(message_str):
            ...     try:
            ...         data = json.loads(message_str)
            ...         return data.get('amount', 0) > 0
            ...     except:
            ...         return False
            >>> 
            >>> def map_order(message_str):
            ...     data = json.loads(message_str)
            ...     return (data['order_id'], data['user_id'], data['amount'])
            >>> 
            >>> pipeline_utils.create_etl_pipeline_with_transform(
            ...     env=env,
            ...     kafka_topic="order-raw",
            ...     kafka_bootstrap_servers=["localhost:29092"],
            ...     mysql_url="jdbc:mysql://localhost:3306/testdb",
            ...     mysql_driver="com.mysql.cj.jdbc.Driver",
            ...     mysql_username="root",
            ...     mysql_password="password",
            ...     mysql_table="order_header",
            ...     mysql_insert_sql="INSERT INTO order_header (order_id, user_id, amount) VALUES (?, ?, ?)",
            ...     filter_func=filter_valid_order,
            ...     map_func=map_order
            ... )
            >>> env.execute("Kafka to MySQL ETL with Transform")
        """
        # 创建 Kafka Source
        kafka_source = self.source_utils.create_kafka_source(
            env=env,
            topic=kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            offset=kwargs.get('kafka_offset', 'earliest')
        )

        # 从 Kafka Source 创建数据流
        data_stream = env.from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            "kafka-source"
        )

        # 数据过滤
        if filter_func:
            data_stream = data_stream.filter(filter_func)

        # 数据映射
        if map_func:
            data_stream = data_stream.map(map_func)
        else:
            # 默认映射: JSON 字符串解析
            def default_map(message_str):
                data = json.loads(message_str)
                return (data.get('order_id'), data.get('user_id'), data.get('amount', 0.0))

            data_stream = data_stream.map(default_map)

        # 创建 MySQL Sink
        mysql_sink = self.sink_utils.create_mysql_sink(
            url=mysql_url,
            driver=mysql_driver,
            username=mysql_username,
            password=mysql_password,
            table_name=mysql_table,
            sql=mysql_insert_sql,
            batch_size=kwargs.get('batch_size', 1000),
            batch_interval_ms=kwargs.get('batch_interval_ms', 2000),
            max_retries=kwargs.get('max_retries', 3)
        )

        # 添加 Sink
        data_stream.add_sink(mysql_sink)


if __name__ == "__main__":
    # 示例用法
    from pyflink.datastream import StreamExecutionEnvironment

    # 创建环境
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 创建工具类实例
    pipeline_utils = FlinkKafkaToMysqlUtils()
    
    # 示例 1: DataStream API ETL 管道
    print("示例 1: 创建 DataStream API ETL 管道")
    
    def transform_order(message_str):
        data = json.loads(message_str)
        return (data.get('order_id'), data.get('user_id'), data.get('amount', 0.0))
    
    pipeline_utils.create_etl_pipeline_datastream(
        env=env,
        kafka_topic="order-raw",
        kafka_bootstrap_servers=["localhost:29092"],
        mysql_url="jdbc:mysql://localhost:3306/testdb",
        mysql_driver="com.mysql.cj.jdbc.Driver",
        mysql_username="root",
        mysql_password="password",
        mysql_table="order_header",
        mysql_insert_sql="INSERT INTO order_header (order_id, user_id, amount) VALUES (?, ?, ?)",
        transform_func=transform_order
    )
    
    print("ETL 管道创建成功, 执行: env.execute('Kafka to MySQL ETL')")

