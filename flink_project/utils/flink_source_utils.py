#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
apache-flink 1.17.0+

Flink 数据源工具类 - Source Utils

功能说明:
数据源创建方法:

1. create_kafka_source (DataStream API):
   - 功能: 创建 Kafka Source 用于 DataStream API
   - 用途: 从 Kafka 读取数据流, 支持多种消费策略

2. create_kafka_table_source (Table API):
   - 功能: 创建 Kafka Table Source 用于 Table API / SQL
   - 用途: 通过 DDL 方式创建 Kafka 表源

3. create_file_source:
   - 功能: 创建文件 Source (文本/CSV/JSON)
   - 用途: 从文件系统读取数据

4. create_jdbc_source:
   - 功能: 创建 JDBC Source (MySQL/PostgreSQL)
   - 用途: 从数据库读取数据

5. create_socket_source:
   - 功能: 创建 Socket Source
   - 用途: 测试和开发场景

6. create_custom_source:
   - 功能: 创建自定义 Source
   - 用途: 封装 SourceFunction 实现自定义数据生成

支持的 API:
- DataStream API: 通过 StreamExecutionEnvironment 创建 Source
- Table API: 通过 TableEnvironment 创建 Table Source

"""

import json
from typing import List, Dict, Optional, Callable
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import TableEnvironment, EnvironmentSettings


class FlinkSourceUtils:
    """
    Flink 数据源工具类
    
    封装常见数据源的创建和配置, 支持 DataStream API 和 Table API
    """

    def __init__(self):
        """
        初始化 Flink Source Utils
        """
        pass

    def create_kafka_source(
        self,
        env: StreamExecutionEnvironment,
        topic: str,
        bootstrap_servers: List[str],
        group_id: Optional[str] = None,
        offset: str = "earliest",
        deserializer: Optional[Callable] = None,
        **kwargs
    ):
        """
        创建 Kafka Source (DataStream API)

        Parameters:
            env: StreamExecutionEnvironment 对象
            topic: Kafka topic 名称 (str)
            bootstrap_servers: Kafka broker 地址列表 (List[str])
            group_id: Consumer group ID (str, optional)
            offset: 消费策略, 'earliest' 或 'latest' (str, default: 'earliest')
            deserializer: 自定义反序列化器 (Callable, optional)
            **kwargs: 其他 Kafka Source 配置参数

        Returns:
            KafkaSource: Kafka Source 对象

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> source_utils = FlinkSourceUtils()
            >>> kafka_source = source_utils.create_kafka_source(
            ...     env=env,
            ...     topic="order-raw",
            ...     bootstrap_servers=["localhost:29092"],
            ...     offset="earliest"
            ... )
            >>> data_stream = env.from_source(
            ...     kafka_source,
            ...     WatermarkStrategy.no_watermarks(),
            ...     "kafka-source"
            ... )
        """
        if not isinstance(bootstrap_servers, list):
            bootstrap_servers = [bootstrap_servers]

        # 配置 Kafka Source 属性
        kafka_props = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'group.id': group_id or f"flink-consumer-{topic}",
        }
        kafka_props.update(kwargs)

        # 配置 offset 策略
        if offset == "earliest":
            offset_initializer = KafkaOffsetsInitializer.earliest()
        elif offset == "latest":
            offset_initializer = KafkaOffsetsInitializer.latest()
        else:
            offset_initializer = KafkaOffsetsInitializer.earliest()

        # 配置反序列化器
        if deserializer is None:
            deserializer = SimpleStringSchema()

        # 创建 Kafka Source
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(','.join(bootstrap_servers)) \
            .set_topics(topic) \
            .set_value_only_deserializer(deserializer) \
            .set_starting_offsets(offset_initializer) \
            .set_properties(kafka_props) \
            .build()

        return kafka_source

    def create_kafka_table_source_ddl(
        self,
        table_name: str,
        topic: str,
        bootstrap_servers: List[str],
        schema: str,
        format_type: str = "json",
        **kwargs
    ) -> str:
        """
        创建 Kafka Table Source DDL 语句 (Table API / SQL)

        Parameters:
            table_name: 表名称 (str)
            topic: Kafka topic 名称 (str)
            bootstrap_servers: Kafka broker 地址列表 (List[str])
            schema: 表结构定义 (str), 例如: "order_id STRING, user_id STRING, amount DECIMAL(10,2)"
            format_type: 数据格式类型, 'json' 或 'csv' (str, default: 'json')
            **kwargs: 其他 Kafka connector 配置参数

        Returns:
            str: CREATE TABLE DDL 语句

        Example:
            >>> source_utils = FlinkSourceUtils()
            >>> ddl = source_utils.create_kafka_table_source_ddl(
            ...     table_name="order_source",
            ...     topic="order-raw",
            ...     bootstrap_servers=["localhost:29092"],
            ...     schema="order_id STRING, user_id STRING, amount DECIMAL(10,2), order_time TIMESTAMP(3)"
            ... )
            >>> table_env.execute_sql(ddl)
        """
        if not isinstance(bootstrap_servers, list):
            bootstrap_servers = [bootstrap_servers]

        bootstrap_servers_str = ','.join(bootstrap_servers)

        # 构建 DDL 语句
        ddl = f"""
        CREATE TABLE {table_name} (
            {schema}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers_str}',
            'format' = '{format_type}'
        )
        """

        # 添加额外配置
        for key, value in kwargs.items():
            ddl = ddl.rstrip() + f",\n            '{key}' = '{value}'\n        )"

        return ddl.strip()

    def create_file_source(
        self,
        env: StreamExecutionEnvironment,
        file_path: str,
        file_format: str = "text",
        **kwargs
    ):
        """
        创建文件 Source

        Parameters:
            env: StreamExecutionEnvironment 对象
            file_path: 文件路径 (str)
            file_format: 文件格式, 'text', 'csv', 'json' (str, default: 'text')
            **kwargs: 其他文件 Source 配置参数

        Returns:
            FileSource: 文件 Source 对象

        Example:
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> source_utils = FlinkSourceUtils()
            >>> file_source = source_utils.create_file_source(
            ...     env=env,
            ...     file_path="file:///path/to/data.txt",
            ...     file_format="text"
            ... )
            >>> data_stream = env.from_source(
            ...     file_source,
            ...     WatermarkStrategy.no_watermarks(),
            ...     "file-source"
            ... )
        """
        from pyflink.datastream.connectors.file_system import FileSource
        from pyflink.common.serialization import Encoder

        if file_format == "text":
            # 文本文件
            file_source = FileSource.for_record_stream_format(
                Encoder.simple_string_encoder(),
                file_path
            ).build()
        elif file_format == "csv":
            # CSV 文件 (需要指定 schema)
            from pyflink.datastream.formats.csv import CsvReaderFormat
            from pyflink.common import Types
            
            # 默认 CSV schema, 可通过 kwargs 自定义
            row_type = kwargs.get('row_type', Types.STRING())
            csv_format = CsvReaderFormat.for_row_type(row_type=row_type)
            
            file_source = FileSource.for_stream_format(
                csv_format,
                file_path
            ).build()
        elif file_format == "json":
            # JSON 文件 (需要指定 schema)
            from pyflink.datastream.formats.json import JsonRowDeserializationSchema
            from pyflink.common import Types
            
            # 默认 JSON schema, 可通过 kwargs 自定义
            row_type = kwargs.get('row_type', Types.STRING())
            json_deserializer = JsonRowDeserializationSchema.Builder() \
                .type_info(row_type) \
                .build()
            
            file_source = FileSource.for_record_stream_format(
                json_deserializer,
                file_path
            ).build()
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        return file_source

    def create_jdbc_table_source_ddl(
        self,
        table_name: str,
        url: str,
        driver: str,
        username: str,
        password: str,
        table: str,
        schema: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        创建 JDBC Table Source DDL 语句 (Table API / SQL)

        Parameters:
            table_name: 表名称 (str)
            url: JDBC URL (str), 例如: "jdbc:mysql://localhost:3306/testdb"
            driver: JDBC 驱动类名 (str), 例如: "com.mysql.cj.jdbc.Driver"
            username: 数据库用户名 (str)
            password: 数据库密码 (str)
            table: 数据库表名 (str)
            schema: 表结构定义 (str, optional), 如果不提供则从数据库读取
            **kwargs: 其他 JDBC connector 配置参数

        Returns:
            str: CREATE TABLE DDL 语句

        Example:
            >>> source_utils = FlinkSourceUtils()
            >>> ddl = source_utils.create_jdbc_table_source_ddl(
            ...     table_name="order_source",
            ...     url="jdbc:mysql://localhost:3306/testdb",
            ...     driver="com.mysql.cj.jdbc.Driver",
            ...     username="root",
            ...     password="password",
            ...     table="order_header",
            ...     schema="order_id STRING, user_id STRING, amount DECIMAL(10,2)"
            ... )
            >>> table_env.execute_sql(ddl)
        """
        # 构建 DDL 语句
        if schema:
            ddl = f"""
            CREATE TABLE {table_name} (
                {schema}
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{url}',
                'driver' = '{driver}',
                'table-name' = '{table}',
                'username' = '{username}',
                'password' = '{password}'
            )
            """
        else:
            ddl = f"""
            CREATE TABLE {table_name} WITH (
                'connector' = 'jdbc',
                'url' = '{url}',
                'driver' = '{driver}',
                'table-name' = '{table}',
                'username' = '{username}',
                'password' = '{password}'
            )
            """

        # 添加额外配置
        for key, value in kwargs.items():
            ddl = ddl.rstrip() + f",\n                '{key}' = '{value}'\n            )"

        return ddl.strip()

    def create_socket_source(
        self,
        env: StreamExecutionEnvironment,
        host: str = "localhost",
        port: int = 9999
    ):
        """
        创建 Socket Source

        Parameters:
            env: StreamExecutionEnvironment 对象
            host: Socket 主机地址 (str, default: 'localhost')
            port: Socket 端口号 (int, default: 9999)

        Returns:
            DataStream: Socket 数据流

        Example:
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> source_utils = FlinkSourceUtils()
            >>> socket_stream = source_utils.create_socket_source(
            ...     env=env,
            ...     host="localhost",
            ...     port=9999
            ... )
        """
        return env.socket_text_stream(host, port)

    def create_custom_source(
        self,
        env: StreamExecutionEnvironment,
        source_function,
        source_name: str = "custom-source"
    ):
        """
        创建自定义 Source

        Parameters:
            env: StreamExecutionEnvironment 对象
            source_function: SourceFunction 实例
            source_name: Source 名称 (str, default: 'custom-source')

        Returns:
            DataStream: 自定义数据流

        Example:
            >>> from pyflink.datastream import SourceFunction
            >>> class MySourceFunction(SourceFunction):
            ...     def run(self, ctx):
            ...         for i in range(10):
            ...             ctx.collect(f"message-{i}")
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> source_utils = FlinkSourceUtils()
            >>> custom_stream = source_utils.create_custom_source(
            ...     env=env,
            ...     source_function=MySourceFunction()
            ... )
        """
        # 验证 source_function 是否为有效的 SourceFunction
        from pyflink.datastream.functions import SourceFunction
        
        if source_function is None:
            raise ValueError("source_function cannot be None")
        
        # 检查是否为 SourceFunction 实例 (允许子类)
        if not isinstance(source_function, SourceFunction):
            # 尝试检查是否有必要的方法
            if not (hasattr(source_function, 'run') and callable(getattr(source_function, 'run'))):
                raise ValueError(
                    f"source_function must be an instance of SourceFunction or have a 'run' method. "
                    f"Got: {type(source_function)}"
                )
        
        return env.add_source(source_function, source_name)


if __name__ == "__main__":
    # 示例用法
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.common.watermark_strategy import WatermarkStrategy

    # 创建环境
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 创建工具类实例
    source_utils = FlinkSourceUtils()
    
    # 示例 1: 创建 Kafka Source (DataStream API)
    print("示例 1: 创建 Kafka Source")
    kafka_source = source_utils.create_kafka_source(
        env=env,
        topic="test-topic",
        bootstrap_servers=["localhost:29092"],
        offset="earliest"
    )
    print(f"Kafka Source 创建成功: {kafka_source}")
    
    # 示例 2: 创建 Kafka Table Source DDL
    print("\n示例 2: 创建 Kafka Table Source DDL")
    ddl = source_utils.create_kafka_table_source_ddl(
        table_name="order_source",
        topic="order-raw",
        bootstrap_servers=["localhost:29092"],
        schema="order_id STRING, user_id STRING, amount DECIMAL(10,2)"
    )
    print(f"DDL 语句:\n{ddl}")

