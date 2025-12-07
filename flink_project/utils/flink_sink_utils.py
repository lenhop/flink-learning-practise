#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
apache-flink 1.17.0+

Flink 数据输出工具类 - Sink Utils

功能说明:
数据输出创建方法:

1. create_kafka_sink (DataStream API):
   - 功能: 创建 Kafka Sink 用于 DataStream API
   - 用途: 将数据流写入 Kafka Topic

2. create_kafka_table_sink_ddl (Table API):
   - 功能: 创建 Kafka Table Sink DDL 语句
   - 用途: 通过 DDL 方式创建 Kafka 表输出

3. create_mysql_sink (DataStream API):
   - 功能: 创建 MySQL JDBC Sink
   - 用途: 将数据流写入 MySQL 数据库

4. create_mysql_table_sink_ddl (Table API):
   - 功能: 创建 MySQL Table Sink DDL 语句
   - 用途: 通过 DDL 方式创建 MySQL 表输出

5. create_file_sink:
   - 功能: 创建文件 Sink (文本/CSV/JSON)
   - 用途: 将数据流写入文件系统

6. create_print_sink:
   - 功能: 创建打印 Sink
   - 用途: 调试和测试场景

支持的 API:
- DataStream API: 通过 DataStream.add_sink() 添加 Sink
- Table API: 通过 Table.execute_insert() 或 TableEnvironment.execute_sql() 写入

"""

from typing import List, Dict, Optional, Callable
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.typeinfo import Types


class FlinkSinkUtils:
    """
    Flink 数据输出工具类
    
    封装常见数据输出的创建和配置, 支持 DataStream API 和 Table API
    """

    def __init__(self):
        """
        初始化 Flink Sink Utils
        """
        pass

    def create_kafka_sink(
        self,
        topic: str,
        bootstrap_servers: List[str],
        serializer: Optional[Callable] = None,
        **kwargs
    ):
        """
        创建 Kafka Sink (DataStream API)

        Parameters:
            topic: Kafka topic 名称 (str)
            bootstrap_servers: Kafka broker 地址列表 (List[str])
            serializer: 自定义序列化器 (Callable, optional)
            **kwargs: 其他 Kafka Sink 配置参数

        Returns:
            KafkaSink: Kafka Sink 对象

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> sink_utils = FlinkSinkUtils()
            >>> kafka_sink = sink_utils.create_kafka_sink(
            ...     topic="order-processed",
            ...     bootstrap_servers=["localhost:29092"]
            ... )
            >>> data_stream.add_sink(kafka_sink)
        """
        if not isinstance(bootstrap_servers, list):
            bootstrap_servers = [bootstrap_servers]

        # 配置序列化器
        if serializer is None:
            serializer = SimpleStringSchema()

        # 配置 Kafka Sink 属性
        kafka_props = {
            'bootstrap.servers': ','.join(bootstrap_servers),
        }
        kafka_props.update(kwargs)

        # 创建 Kafka Sink
        # 注意: KafkaSink 在 apache-flink 2.2.0 中需要使用 KafkaRecordSerializationSchema
        try:
            from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
            
            # 创建序列化 schema
            kafka_record_serialization = KafkaRecordSerializationSchema.builder() \
                .set_topic(topic) \
                .set_value_serialization_schema(serializer) \
                .build()
            
            # 创建 Kafka Sink
            kafka_sink = KafkaSink.builder() \
                .set_bootstrap_servers(','.join(bootstrap_servers)) \
                .set_record_serializer(kafka_record_serialization) \
                .set_properties(kafka_props) \
                .build()
        except ImportError:
            # 如果 KafkaRecordSerializationSchema 不存在, 使用简化方式
            # 注意: 这可能需要根据实际 Flink 版本调整
            kafka_sink = KafkaSink.builder() \
                .set_bootstrap_servers(','.join(bootstrap_servers)) \
                .set_record_serializer(serializer) \
                .set_properties(kafka_props) \
                .build()

        return kafka_sink

    def create_kafka_table_sink_ddl(
        self,
        table_name: str,
        topic: str,
        bootstrap_servers: List[str],
        schema: str,
        format_type: str = "json",
        **kwargs
    ) -> str:
        """
        创建 Kafka Table Sink DDL 语句 (Table API / SQL)

        Parameters:
            table_name: 表名称 (str)
            topic: Kafka topic 名称 (str)
            bootstrap_servers: Kafka broker 地址列表 (List[str])
            schema: 表结构定义 (str)
            format_type: 数据格式类型, 'json' 或 'csv' (str, default: 'json')
            **kwargs: 其他 Kafka connector 配置参数

        Returns:
            str: CREATE TABLE DDL 语句

        Example:
            >>> sink_utils = FlinkSinkUtils()
            >>> ddl = sink_utils.create_kafka_table_sink_ddl(
            ...     table_name="order_sink",
            ...     topic="order-processed",
            ...     bootstrap_servers=["localhost:29092"],
            ...     schema="order_id STRING, user_id STRING, amount DECIMAL(10,2)"
            ... )
            >>> table_env.execute_sql(ddl)
            >>> table_env.execute_sql("INSERT INTO order_sink SELECT * FROM order_source")
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

    def create_mysql_sink(
        self,
        url: str,
        driver: str,
        username: str,
        password: str,
        table_name: str,
        sql: str,
        type_info=None,
        batch_size: int = 1000,
        batch_interval_ms: int = 2000,
        max_retries: int = 3
    ):
        """
        创建 MySQL JDBC Sink (DataStream API)

        Parameters:
            url: JDBC URL (str), 例如: "jdbc:mysql://localhost:3306/testdb"
            driver: JDBC 驱动类名 (str), 例如: "com.mysql.cj.jdbc.Driver"
            username: 数据库用户名 (str)
            password: 数据库密码 (str)
            table_name: 数据库表名 (str)
            sql: INSERT SQL 语句 (str), 例如: "INSERT INTO order_header (order_id, user_id) VALUES (?, ?)"
            type_info: 数据类型信息 (TypeInformation, optional), 如果不提供则使用默认类型
            batch_size: 批量插入大小 (int, default: 1000)
            batch_interval_ms: 批量插入间隔时间, 毫秒 (int, default: 2000)
            max_retries: 最大重试次数 (int, default: 3)

        Returns:
            JdbcSink: MySQL JDBC Sink 对象

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> sink_utils = FlinkSinkUtils()
            >>> mysql_sink = sink_utils.create_mysql_sink(
            ...     url="jdbc:mysql://localhost:3306/testdb",
            ...     driver="com.mysql.cj.jdbc.Driver",
            ...     username="root",
            ...     password="password",
            ...     table_name="order_header",
            ...     sql="INSERT INTO order_header (order_id, user_id, amount) VALUES (?, ?, ?)"
            ... )
            >>> data_stream.add_sink(mysql_sink)
        """
        try:
            # 如果未提供 type_info, 使用默认类型
            if type_info is None:
                placeholder_count = sql.count('?')
                if placeholder_count == 0:
                    raise ValueError("SQL statement must contain at least one placeholder (?)")
                type_info = Types.ROW([Types.STRING()] * placeholder_count)
            
            # 尝试使用标准JDBC Sink创建方法
            try:
                mysql_sink = JdbcSink.sink(
                    sql,
                    type_info,
                    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .with_url(url)
                        .with_driver_name(driver)
                        .with_user_name(username)
                        .with_password(password)
                        .build(),
                    JdbcExecutionOptions.builder()
                        .with_batch_size(batch_size)
                        .with_batch_interval_ms(batch_interval_ms)
                        .with_max_retries(max_retries)
                        .build()
                )
                return mysql_sink
            except Exception as e1:
                print(f"Standard JDBC sink creation failed: {str(e1)}")
                
                # 尝试使用更简单的方法
                try:
                    mysql_sink = JdbcSink.sink(
                        sql,
                        type_info,
                        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .with_url(url)
                            .with_driver_name(driver)
                            .with_user_name(username)
                            .with_password(password)
                            .build()
                    )
                    return mysql_sink
                except Exception as e2:
                    print(f"Simplified JDBC sink creation failed: {str(e2)}")
                    
                    # 使用自定义Python MySQL Sink
                    print("Using custom Python MySQL Sink...")
                    return self._create_python_mysql_sink(url, username, password, table_name, sql)
                    
        except Exception as e:
            print(f"Error creating JDBC sink: {str(e)}")
            raise
    
    def _create_python_mysql_sink(self, url, username, password, table_name, sql):
        """
        创建基于Python原生mysql.connector的MySQL Sink函数
        
        当JDBC连接器不可用时使用此方法
        """
        try:
            import mysql.connector
            from mysql.connector import Error
            
            def mysql_sink_func(value):
                """MySQL Sink函数"""
                try:
                    # 解析URL
                    if "jdbc:mysql://" in url:
                        # JDBC格式: jdbc:mysql://host:port/database
                        url_parts = url.replace("jdbc:mysql://", "").split("/")
                        host_port = url_parts[0].split(":")
                        host = host_port[0]
                        port = int(host_port[1]) if len(host_port) > 1 else 3306
                        database = url_parts[1] if len(url_parts) > 1 else ""
                    else:
                        # 直接MySQL格式: host:port/database
                        url_parts = url.split("/")
                        host_port = url_parts[0].split(":")
                        host = host_port[0]
                        port = int(host_port[1]) if len(host_port) > 1 else 3306
                        database = url_parts[1] if len(url_parts) > 1 else ""
                    
                    # 连接数据库
                    connection = mysql.connector.connect(
                        host=host,
                        port=port,
                        database=database,
                        user=username,
                        password=password,
                        autocommit=True
                    )
                    cursor = connection.cursor()
                    
                    # 将数据转换为适合SQL的参数格式
                    if hasattr(value, '__iter__') and not isinstance(value, str):
                        params = tuple(value)
                    else:
                        params = (value,)
                    
                    # 执行SQL
                    cursor.execute(sql, params)
                    connection.commit()
                    
                    print(f"✓ Inserted record: {params}")
                    
                    # 关闭连接
                    cursor.close()
                    connection.close()
                    
                except Error as e:
                    print(f"✗ MySQL error inserting record: {str(e)}")
                except Exception as e:
                    print(f"✗ Unexpected error inserting record: {str(e)}")
            
            return mysql_sink_func
            
        except ImportError:
            print("✗ mysql-connector-python not installed. Please install: pip install mysql-connector-python")
            raise
        except Exception as e:
            print(f"✗ Error creating Python MySQL Sink: {str(e)}")
            raise

    def create_mysql_table_sink_ddl(
        self,
        table_name: str,
        url: str,
        driver: str,
        username: str,
        password: str,
        db_table: str,
        schema: str,
        **kwargs
    ) -> str:
        """
        创建 MySQL Table Sink DDL 语句 (Table API / SQL)

        Parameters:
            table_name: 表名称 (str)
            url: JDBC URL (str), 例如: "jdbc:mysql://localhost:3306/testdb"
            driver: JDBC 驱动类名 (str), 例如: "com.mysql.cj.jdbc.Driver"
            username: 数据库用户名 (str)
            password: 数据库密码 (str)
            db_table: 数据库表名 (str)
            schema: 表结构定义 (str)
            **kwargs: 其他 JDBC connector 配置参数

        Returns:
            str: CREATE TABLE DDL 语句

        Example:
            >>> sink_utils = FlinkSinkUtils()
            >>> ddl = sink_utils.create_mysql_table_sink_ddl(
            ...     table_name="order_sink",
            ...     url="jdbc:mysql://localhost:3306/testdb",
            ...     driver="com.mysql.cj.jdbc.Driver",
            ...     username="root",
            ...     password="password",
            ...     db_table="order_header",
            ...     schema="order_id STRING, user_id STRING, amount DECIMAL(10,2), PRIMARY KEY (order_id) NOT ENFORCED"
            ... )
            >>> table_env.execute_sql(ddl)
            >>> table_env.execute_sql("INSERT INTO order_sink SELECT * FROM order_source")
        """
        # 构建 DDL 语句
        # 转义单引号, 防止 SQL 注入
        def escape_sql_string(s):
            """Escape single quotes in SQL string"""
            return s.replace("'", "''")
        
        escaped_url = escape_sql_string(url)
        escaped_driver = escape_sql_string(driver)
        escaped_db_table = escape_sql_string(db_table)
        escaped_username = escape_sql_string(username)
        escaped_password = escape_sql_string(password)
        
        # 构建基础 DDL
        ddl_parts = [
            f"CREATE TABLE {table_name} (",
            f"    {schema}",
            ") WITH (",
            f"    'connector' = 'jdbc',",
            f"    'url' = '{escaped_url}',",
            f"    'driver' = '{escaped_driver}',",
            f"    'table-name' = '{escaped_db_table}',",
            f"    'username' = '{escaped_username}',",
            f"    'password' = '{escaped_password}'"
        ]
        
        # 添加额外配置
        for key, value in kwargs.items():
            escaped_key = escape_sql_string(str(key))
            escaped_value = escape_sql_string(str(value))
            ddl_parts.append(f"    '{escaped_key}' = '{escaped_value}'")
        
        ddl_parts.append(")")
        
        return "\n".join(ddl_parts)

    def create_file_sink(
        self,
        file_path: str,
        file_format: str = "text",
        **kwargs
    ):
        """
        创建文件 Sink

        Parameters:
            file_path: 文件路径 (str)
            file_format: 文件格式, 'text', 'csv', 'json' (str, default: 'text')
            **kwargs: 其他文件 Sink 配置参数

        Returns:
            FileSink: 文件 Sink 对象

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> sink_utils = FlinkSinkUtils()
            >>> file_sink = sink_utils.create_file_sink(
            ...     file_path="file:///path/to/output.txt",
            ...     file_format="text"
            ... )
            >>> data_stream.add_sink(file_sink)
        """
        from pyflink.datastream.connectors.file_system import FileSink
        from pyflink.common.serialization import Encoder

        if file_format == "text":
            # 文本文件
            file_sink = FileSink.for_row_format(
                file_path,
                Encoder.simple_string_encoder()
            ).build()
        elif file_format == "csv":
            # CSV 文件
            from pyflink.datastream.formats.csv import CsvWriterFormat
            from pyflink.common import Types
            
            row_type = kwargs.get('row_type', Types.STRING())
            csv_format = CsvWriterFormat.for_row_type(row_type=row_type)
            
            file_sink = FileSink.for_row_format(
                file_path,
                csv_format
            ).build()
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        return file_sink

    def create_print_sink(self, sink_name: str = "print-sink"):
        """
        创建打印 Sink (用于调试)

        Parameters:
            sink_name: Sink 名称 (str, default: 'print-sink')

        Returns:
            str: Sink 名称标识

        Example:
            >>> from pyflink.datastream import StreamExecutionEnvironment
            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> sink_utils = FlinkSinkUtils()
            >>> data_stream.print(sink_utils.create_print_sink())
        """
        return sink_name


if __name__ == "__main__":
    # 示例用法
    from pyflink.datastream import StreamExecutionEnvironment

    # 创建环境
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 创建工具类实例
    sink_utils = FlinkSinkUtils()
    
    # 示例 1: 创建 Kafka Sink
    print("示例 1: 创建 Kafka Sink")
    kafka_sink = sink_utils.create_kafka_sink(
        topic="test-topic",
        bootstrap_servers=["localhost:29092"]
    )
    print(f"Kafka Sink 创建成功: {kafka_sink}")
    
    # 示例 2: 创建 MySQL Table Sink DDL
    print("\n示例 2: 创建 MySQL Table Sink DDL")
    ddl = sink_utils.create_mysql_table_sink_ddl(
        table_name="order_sink",
        url="jdbc:mysql://localhost:3306/testdb",
        driver="com.mysql.cj.jdbc.Driver",
        username="root",
        password="password",
        db_table="order_header",
        schema="order_id STRING, user_id STRING, amount DECIMAL(10,2), PRIMARY KEY (order_id) NOT ENFORCED"
    )
    print(f"DDL 语句:\n{ddl}")

