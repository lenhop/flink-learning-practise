#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
kafka-python 2.2.11

Kafka 数据加载到 MySQL 方法说明:

1. load_single_message:
   - 功能: 消费单条消息并立即插入 MySQL
   - 用途: 实时处理, 需要立即落库的场景

2. load_batch_messages:
   - 功能: 批量消费消息并批量插入 MySQL
   - 用途: 批量数据处理, 提高写入效率

3. load_with_custom_sql:
   - 功能: 使用自定义 SQL 模板进行插入
   - 用途: 复杂业务逻辑, 需要自定义 SQL 的场景

4. load_with_transaction:
   - 功能: 在事务中执行批量插入
   - 用途: 需要保证数据一致性的场景

5. load_with_upsert:
   - 功能: 使用 INSERT ... ON DUPLICATE KEY UPDATE
   - 用途: 需要更新已存在记录的场景

6. load_with_buffer:
   - 功能: 累积消息到缓冲区, 达到阈值或超时后批量刷新
   - 用途: 实时数据流, 平衡延迟和吞吐量
   - 支持: 可指定起始偏移量(offset)和分区(partition), 灵活控制从哪条消息开始入库

性能对比: load_with_buffer > load_batch_messages > \
    load_with_upsert > load_single_message
可靠性对比: load_with_transaction > load_with_upsert > \
    load_batch_messages > load_single_message

"""

import os
import sys
import time
import json
import threading

current_file_path = os.path.abspath(__file__)
current_directory_path = os.path.dirname(current_file_path)
sys.path.append(current_directory_path)

from kafka_consumer_utils import KafkaConsumerUtils
from unified_mysql import MysqlDB


class KafkaLoadToMysql:

    """
    Kafka Load to MySQL Utils

    Attributes:
        bootstrap_servers: Kafka broker addresses, can be string or list
        topic: Kafka topic name to consume from
        mysql_db: MySQL connection object (MysqlDB instance), optional
        mysql_config: MySQL configuration dict (host, port, user,
                        password, database, etc.), optional
        group_id: Consumer group ID (optional, auto-generated if None)
    """

    def __init__(self, bootstrap_servers, topic, mysql_db=None,
                 mysql_config=None, group_id=None):
        """
        Initialize Kafka Load to MySQL Utils

        Parameters:
            bootstrap_servers: Kafka broker addresses, can be string or list
            topic: Kafka topic name to consume from
            mysql_db: MySQL connection object (MysqlDB instance), optional
            mysql_config: MySQL configuration dict (host, port, user,
                        password, database, etc.), optional
            group_id: Consumer group ID (optional, auto-generated if None)

        Note:
            Either mysql_db or mysql_config must be provided.
            If both are provided, mysql_db takes precedence.
        """
        if isinstance(bootstrap_servers, list):
            self.bootstrap_servers = bootstrap_servers
        else:
            self.bootstrap_servers = [bootstrap_servers]
        self.topic = topic
        self.group_id = group_id or f"kafka-mysql-loader-{int(time.time())}"
        self.mysql_db = mysql_db
        self.mysql_config = mysql_config
        self.consumer_utils = None

        if self.mysql_db is None and self.mysql_config is None:
            msg = "Either mysql_db or mysql_config must be provided"
            raise ValueError(msg)

    def connect_mysql(self):
        """
        Connect to MySQL database

        If mysql_db is already set, return it directly.
        Otherwise, create MysqlDB instance from mysql_config.
        """
        if self.mysql_db is not None:
            return self.mysql_db

        if self.mysql_config is None:
            msg = ("MySQL configuration not found. "
                   "Please provide mysql_db or mysql_config.")
            raise ValueError(msg)

        try:
            self.mysql_db = MysqlDB(**self.mysql_config)
            return self.mysql_db
        except ImportError as exc:
            msg = ("MysqlDB class not found. "
                   "Please ensure unified_tools is available.")
            raise ImportError(msg) from exc
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MySQL: {e}") from e

    def test_connection(self):
        """
        Test MySQL connection

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if self.mysql_db is None:
                self.connect_mysql()
            return self.mysql_db.test_connection()
        except Exception as e:
            print(f"MySQL connection test failed: {e}")
            return False

    def _get_consumer(self, **kwargs):
        """
        Get or create Kafka consumer

        Parameters:
            **kwargs: Additional Kafka consumer configuration

        Returns:
            KafkaConsumerUtils: Consumer instance
        """
        if self.consumer_utils is None:
            self.consumer_utils = KafkaConsumerUtils(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.topic,
                group_id=self.group_id
            )
            self.consumer_utils.connect_kafka(**kwargs)
        return self.consumer_utils

    def transform_message(self, message, transform_func=None):
        """
        Transform message using custom function

        Parameters:
            message: Kafka message object or dict
            transform_func: Optional function to transform message
                          (message) -> dict

        Returns:
            dict: Transformed message data
        """
        if transform_func and callable(transform_func):
            return transform_func(message)

        # If message is already a dict, return it
        if isinstance(message, dict):
            return message

        # If message has value attribute (Kafka message object), extract it
        if hasattr(message, 'value'):
            value = message.value
            if isinstance(value, dict):
                return value
            elif isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return {'raw_data': value}

        # Default: return as dict with raw data
        return {'raw_data': message}

    def build_insert_sql(self, table_name, columns):
        """
        Build INSERT SQL statement

        Parameters:
            table_name: Target table name
            columns: List of column names

        Returns:
            str: INSERT SQL statement with named parameters
        """
        columns_str = ', '.join(columns)
        values_str = ', '.join([f':{col}' for col in columns])
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        return sql

    def build_upsert_sql(self, table_name, columns, update_fields=None):
        """
        Build INSERT ... ON DUPLICATE KEY UPDATE SQL statement

        Parameters:
            table_name: Target table name
            columns: List of column names
            update_fields: List of fields to update on duplicate
                        (if None, update all except first column)

        Returns:
            str: UPSERT SQL statement with named parameters
        """
        columns_str = ', '.join(columns)
        values_str = ', '.join([f':{col}' for col in columns])

        if update_fields is None:
            # Update all fields except the first one (usually primary key)
            update_fields = columns[1:] if len(columns) > 1 else columns

        update_str = ', '.join(
            [f"{field} = VALUES({field})" for field in update_fields]
        )

        sql = (f"INSERT INTO {table_name} ({columns_str}) "
               f"VALUES ({values_str}) ON DUPLICATE KEY UPDATE {update_str}")
        return sql

    def load_single_message(self, message, table_name, transform_func=None,
                            close_after_load=False):
        """
        Load a single message to MySQL table

        Parameters:
            message: Kafka message object or dict
            table_name: Target MySQL table name
            transform_func: Optional function to transform message
                          (message) -> dict
            close_after_load: Whether to close MySQL connection after loading

        Returns:
            int: Number of affected rows
        """
        if self.mysql_db is None:
            self.connect_mysql()

        try:
            # Transform message
            data = self.transform_message(message, transform_func)

            if not isinstance(data, dict):
                raise ValueError("Transformed message must be a dictionary")

            # Build INSERT SQL
            columns = list(data.keys())
            sql = self.build_insert_sql(table_name, columns)

            # Execute insert
            affected_rows = self.mysql_db.execute_batch_dml(sql, [data])

            msg = (f"Inserted 1 record to {table_name}, "
                   f"affected rows: {affected_rows}")
            print(msg)

            return affected_rows

        except Exception as e:
            print(f"Error loading single message: {e}")
            raise
        finally:
            if close_after_load:
                self.close()

    def load_batch_messages(self, messages, table_name, transform_func=None,
                            batch_size=100, close_after_load=False):
        """
        Load batch messages to MySQL table

        Parameters:
            messages: List of Kafka message objects or dicts
            table_name: Target MySQL table name
            transform_func: Optional function to transform message
                          (message) -> dict
            batch_size: Batch size for insertion (default: 100)
            close_after_load: Whether to close MySQL connection after loading

        Returns:
            int: Total number of affected rows
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")

        if len(messages) == 0:
            return 0

        if self.mysql_db is None:
            self.connect_mysql()

        try:
            # Transform all messages
            records = []
            for message in messages:
                data = self.transform_message(message, transform_func)
                if isinstance(data, dict):
                    records.append(data)

            if len(records) == 0:
                print("No valid records to insert")
                return 0

            # Get columns from first record
            columns = list(records[0].keys())

            # Ensure all records have the same columns
            for record in records:
                for col in columns:
                    if col not in record:
                        record[col] = None

            # Build INSERT SQL
            sql = self.build_insert_sql(table_name, columns)

            # Execute batch insert in chunks
            total_affected = 0
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]
                affected_rows = self.mysql_db.execute_batch_dml(sql, chunk)
                total_affected += affected_rows
                batch_num = i // batch_size + 1
                msg = (f"Inserted batch {batch_num} ({len(chunk)} records) "
                       f"to {table_name}, affected rows: {affected_rows}")
                print(msg)

            msg = (f"Batch insert completed, "
                   f"total affected rows: {total_affected}")
            print(msg)
            return total_affected

        except Exception as e:
            print(f"Error loading batch messages: {e}")
            raise
        finally:
            if close_after_load:
                self.close()

    def load_with_custom_sql(self, messages, sql_template, transform_func=None,
                             batch_size=100, close_after_load=False):
        """
        Load messages using custom SQL template

        Parameters:
            messages: List of Kafka message objects or dicts
            sql_template: SQL template with named parameters
                        (e.g., ":param_name")
            transform_func: Optional function to transform message
                          (message) -> dict
            batch_size: Batch size for insertion (default: 100)
            close_after_load: Whether to close MySQL connection after loading

        Returns:
            int: Total number of affected rows
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")

        if len(messages) == 0:
            return 0

        if self.mysql_db is None:
            self.connect_mysql()

        try:
            # Transform all messages
            records = []
            for message in messages:
                data = self.transform_message(message, transform_func)
                if isinstance(data, dict):
                    records.append(data)

            if len(records) == 0:
                print("No valid records to insert")
                return 0

            # Execute batch insert in chunks
            total_affected = 0
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]
                affected_rows = self.mysql_db.execute_batch_dml(
                    sql_template, chunk
                )
                total_affected += affected_rows
                batch_num = i // batch_size + 1
                msg = (f"Executed batch {batch_num} ({len(chunk)} records), "
                       f"affected rows: {affected_rows}")
                print(msg)

            msg = (f"Custom SQL execution completed, "
                   f"total affected rows: {total_affected}")
            print(msg)
            return total_affected

        except Exception as e:
            print(f"Error loading with custom SQL: {e}")
            raise
        finally:
            if close_after_load:
                self.close()

    def load_with_transaction(self, messages, table_name, transform_func=None,
                              batch_size=100, auto_commit=True,
                              close_after_load=False):
        """
        Load messages within a transaction

        Parameters:
            messages: List of Kafka message objects or dicts
            table_name: Target MySQL table name
            transform_func: Optional function to transform message
                          (message) -> dict
            batch_size: Batch size for insertion (default: 100)
            auto_commit: Whether to auto-commit transaction (default: True)
            close_after_load: Whether to close MySQL connection after loading

        Returns:
            int: Total number of affected rows
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")

        if len(messages) == 0:
            return 0

        if self.mysql_db is None:
            self.connect_mysql()

        try:
            # Transform all messages
            records = []
            for message in messages:
                data = self.transform_message(message, transform_func)
                if isinstance(data, dict):
                    records.append(data)

            if len(records) == 0:
                print("No valid records to insert")
                return 0

            # Get columns from first record
            columns = list(records[0].keys())

            # Ensure all records have the same columns
            for record in records:
                for col in columns:
                    if col not in record:
                        record[col] = None

            # Build INSERT SQL
            sql = self.build_insert_sql(table_name, columns)

            # Begin transaction
            if hasattr(self.mysql_db, 'begin'):
                self.mysql_db.begin()
            elif (hasattr(self.mysql_db, 'connection') and
                  hasattr(self.mysql_db.connection, 'begin')):
                self.mysql_db.connection.begin()

            total_affected = 0
            try:
                # Execute batch insert in chunks
                for i in range(0, len(records), batch_size):
                    chunk = records[i:i + batch_size]
                    affected_rows = self.mysql_db.execute_batch_dml(sql, chunk)
                    total_affected += affected_rows
                    batch_num = i // batch_size + 1
                    msg = (f"Transaction batch {batch_num} "
                           f"({len(chunk)} records) to {table_name}, "
                           f"affected rows: {affected_rows}")
                    print(msg)

                # Commit transaction
                if auto_commit:
                    if hasattr(self.mysql_db, 'commit'):
                        self.mysql_db.commit()
                    elif (hasattr(self.mysql_db, 'connection') and
                          hasattr(self.mysql_db.connection, 'commit')):
                        self.mysql_db.connection.commit()
                    msg = (f"Transaction committed, "
                           f"total affected rows: {total_affected}")
                    print(msg)

            except Exception as e:
                # Rollback on error
                if hasattr(self.mysql_db, 'rollback'):
                    self.mysql_db.rollback()
                elif (hasattr(self.mysql_db, 'connection') and
                      hasattr(self.mysql_db.connection, 'rollback')):
                    self.mysql_db.connection.rollback()
                print(f"Transaction rolled back due to error: {e}")
                raise

            return total_affected

        except Exception as e:
            print(f"Error loading with transaction: {e}")
            raise
        finally:
            if close_after_load:
                self.close()

    def load_with_upsert(self, messages, table_name, transform_func=None,
                         batch_size=100, update_fields=None,
                         close_after_load=False):
        """
        Load messages with upsert (INSERT ... ON DUPLICATE KEY UPDATE)

        Parameters:
            messages: List of Kafka message objects or dicts
            table_name: Target MySQL table name
            transform_func: Optional function to transform message
                          (message) -> dict
            batch_size: Batch size for insertion (default: 100)
            update_fields: List of fields to update on duplicate
                        (if None, update all except first column)
            close_after_load: Whether to close MySQL connection after loading

        Returns:
            int: Total number of affected rows
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")

        if len(messages) == 0:
            return 0

        if self.mysql_db is None:
            self.connect_mysql()

        try:
            # Transform all messages
            records = []
            for message in messages:
                data = self.transform_message(message, transform_func)
                if isinstance(data, dict):
                    records.append(data)

            if len(records) == 0:
                print("No valid records to insert")
                return 0

            # Get columns from first record
            columns = list(records[0].keys())

            # Ensure all records have the same columns
            for record in records:
                for col in columns:
                    if col not in record:
                        record[col] = None

            # Build UPSERT SQL
            sql = self.build_upsert_sql(table_name, columns, update_fields)

            # Execute batch upsert in chunks
            total_affected = 0
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]
                affected_rows = self.mysql_db.execute_batch_dml(sql, chunk)
                total_affected += affected_rows
                batch_num = i // batch_size + 1
                msg = (f"Upsert batch {batch_num} ({len(chunk)} records) "
                       f"to {table_name}, affected rows: {affected_rows}")
                print(msg)

            print(f"Upsert completed, total affected rows: {total_affected}")
            return total_affected

        except Exception as e:
            print(f"Error loading with upsert: {e}")
            raise
        finally:
            if close_after_load:
                self.close()

    def load_with_buffer(self, table_name, transform_func=None,
                         buffer_size=100, flush_interval=5,
                         max_messages=None, timeout_seconds=None,
                         start_offset=None, partition=None,
                         close_after_load=False, **consumer_kwargs):
        """
        Load messages with buffer, flush when buffer is full or timeout

        Parameters:
            table_name: Target MySQL table name
            transform_func: Optional function to transform message
                          (message) -> dict
            buffer_size: Buffer size threshold to trigger flush
                        (default: 100)
            flush_interval: Time interval in seconds to flush buffer
                          (default: 5)
            max_messages: Maximum number of messages to consume (optional)
            timeout_seconds: Timeout in seconds (optional)
            start_offset: Starting offset to begin consumption (optional)
                         If specified, must also provide partition
            partition: Partition number to consume from (optional)
                      If specified with start_offset, will seek to that offset
            close_after_load: Whether to close connections after loading
            **consumer_kwargs: Additional Kafka consumer configuration

        Returns:
            dict: Statistics about loading process
        """
        if self.mysql_db is None:
            self.connect_mysql()

        consumer = self._get_consumer(**consumer_kwargs)

        # Seek to specific offset if provided
        if start_offset is not None:
            if partition is None:
                raise ValueError(
                    "partition must be specified when start_offset is provided"
                )
            if not consumer.consumer:
                consumer.connect_kafka(**consumer_kwargs)

            from kafka import TopicPartition

            # Unsubscribe from current subscription if exists
            if consumer.consumer.subscription():
                consumer.consumer.unsubscribe()

            # Assign partition and seek to offset
            topic_partition = TopicPartition(self.topic, partition)
            consumer.consumer.assign([topic_partition])
            consumer.consumer.seek(topic_partition, start_offset)
            print(f"Seeked to partition {partition}, offset {start_offset}")

        buffer = []
        buffer_lock = threading.Lock()
        stats = {
            'total_messages': 0,
            'inserted': 0,
            'errors': 0,
            'flush_count': 0,
            'start_time': time.time(),
            'end_time': None,
            'start_offset': start_offset,
            'partition': partition
        }
        shutdown_flag = False

        def flush_buffer():
            """Flush buffer to MySQL"""
            nonlocal buffer, stats

            with buffer_lock:
                if len(buffer) == 0:
                    return
                records_to_insert = buffer.copy()
                buffer.clear()

            if len(records_to_insert) == 0:
                return

            try:
                # Get columns from first record
                columns = list(records_to_insert[0].keys())

                # Ensure all records have the same columns
                for record in records_to_insert:
                    for col in columns:
                        if col not in record:
                            record[col] = None

                # Build INSERT SQL
                sql = self.build_insert_sql(table_name, columns)

                # Execute batch insert
                affected_rows = self.mysql_db.execute_batch_dml(
                    sql, records_to_insert
                )
                stats['inserted'] += affected_rows
                stats['flush_count'] += 1
                msg = (f"Flushed {len(records_to_insert)} records "
                       f"to {table_name}, affected rows: {affected_rows}")
                print(msg)

            except Exception as e:
                stats['errors'] += len(records_to_insert)
                print(f"Error flushing buffer: {e}")
                import traceback
                traceback.print_exc()

        def flush_timer():
            """Timer thread to flush buffer periodically"""
            nonlocal shutdown_flag

            while not shutdown_flag:
                time.sleep(flush_interval)
                if shutdown_flag:
                    break
                flush_buffer()

        def process_message(message):
            """Process a single message"""
            nonlocal buffer, stats, shutdown_flag

            if shutdown_flag:
                return False

            try:
                # Transform message
                data = self.transform_message(message, transform_func)

                if not isinstance(data, dict):
                    print("Warning: Transformed message is not a dict, "
                          "skipping")
                    return True

                # Add to buffer
                with buffer_lock:
                    buffer.append(data)
                    stats['total_messages'] += 1

                # Check if buffer is full
                if len(buffer) >= buffer_size:
                    flush_buffer()

                return True

            except Exception as e:
                stats['errors'] += 1
                print(f"Error processing message: {e}")
                return True

        try:
            # Start flush timer thread
            timer_thread = threading.Thread(target=flush_timer, daemon=True)
            timer_thread.start()
            msg = (f"Buffer flush timer started "
                   f"(interval: {flush_interval}s)")
            print(msg)

            # Consume messages with callback
            msg = (f"Starting to consume messages with buffer "
                   f"(size: {buffer_size}, flush_interval: {flush_interval}s)")
            if start_offset is not None:
                msg += f", from offset {start_offset} (partition {partition})"
            print(msg + "...")
            consumer.consume_messages_with_callback(
                message_processor=process_message,
                max_messages=max_messages,
                timeout_seconds=timeout_seconds,
                close_after_consume=False,
                shutdown_check=lambda: shutdown_flag
            )

        except KeyboardInterrupt:
            print("\nInterrupted by user")
        except Exception as e:
            print(f"Error in load_with_buffer: {e}")
            raise
        finally:
            # Set shutdown flag
            shutdown_flag = True

            # Wait a bit for timer thread
            time.sleep(1)

            # Flush remaining buffer
            print("Flushing remaining buffer...")
            flush_buffer()

            stats['end_time'] = time.time()
            stats['duration'] = stats['end_time'] - stats['start_time']

            print("\nBuffer loading completed:")
            print(f"  Total messages: {stats['total_messages']}")
            print(f"  Inserted: {stats['inserted']}")
            print(f"  Errors: {stats['errors']}")
            print(f"  Flush count: {stats['flush_count']}")
            print(f"  Duration: {stats['duration']:.2f} seconds")
            if stats.get('start_offset') is not None:
                print(f"  Started from offset: {stats['start_offset']} "
                      f"(partition: {stats.get('partition')})")

            if close_after_load:
                self.close()

        return stats

    def close(self):
        """Close MySQL and Kafka connections"""
        if self.consumer_utils:
            try:
                self.consumer_utils.close()
                self.consumer_utils = None
            except Exception as e:
                print(f"Error closing Kafka consumer: {e}")

        # Note: MysqlDB connection pool is typically managed by
        # the MysqlDB instance itself. We don't need to explicitly
        # close it here unless there's a close method
        if self.mysql_db and hasattr(self.mysql_db, 'close'):
            try:
                self.mysql_db.close()
            except Exception as e:
                print(f"Error closing MySQL connection: {e}")


if __name__ == "__main__":
    # Example usage
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'password',
        'database': 'testdb',
        'charset': 'utf8mb4'
    }

    loader = KafkaLoadToMysql(
        bootstrap_servers="localhost:9092",
        topic="test-topic",
        mysql_config=mysql_config
    )

    # Test connection
    if loader.test_connection():
        print("MySQL connection successful")

    # Example: Load single message
    # message = {"id": 1, "name": "test"}
    # loader.load_single_message(message, "test_table")

    # Example: Load with buffer
    # loader.load_with_buffer("test_table", buffer_size=50, flush_interval=3)

    loader.close()
