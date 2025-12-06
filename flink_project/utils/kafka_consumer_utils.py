#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
kafka-python 2.2.11

消息消费方法说明:

1. consume_messages_once:
   - 功能: 一次性消费指定数量的消息, 消费完即停止
   - 用途: 测试场景, 快速验证消息内容

2. consume_messages_with_timeout:
   - 功能: 在指定时间内消费消息, 超时自动停止
   - 用途: 定时任务, 批量处理场景

3. consume_messages_continuous:
   - 功能: 持续消费消息, 手动停止或异常中断
   - 用途: 实时数据处理, 长期运行的消费者

4. consume_messages_with_callback:
   - 功能: 消费消息并通过回调函数处理, 支持自定义处理逻辑
   - 用途: 复杂业务处理, 消息转换和路由

5. consume_from_specific_offset:
   - 功能: 从指定offset开始消费消息
   - 用途: 数据重放, 错误恢复, 历史数据处理

6. consume_latest_messages:
   - 功能: 只消费最新的消息, 跳过历史数据
   - 用途: 实时监控, 状态更新

性能对比: consume_messages_continuous > consume_messages_with_callback > consume_messages_with_timeout > consume_messages_once
灵活性对比: consume_messages_with_callback > consume_from_specific_offset > consume_messages_continuous > consume_latest_messages

"""

import time
import json
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAuthorizationFailedError


class KafkaConsumerUtils:

    def __init__(self, bootstrap_servers, topic, group_id=None):
        """
        Initialize Kafka Consumer Utils

        Parameters:
            bootstrap_servers: Kafka broker addresses, can be string or list
            topic: Topic name to consume from
            group_id: Consumer group ID (optional, auto-generated if None)
        """
        self.bootstrap_servers = bootstrap_servers if isinstance(bootstrap_servers, list) else [bootstrap_servers]
        self.topic = topic
        self.group_id = group_id or f"consumer-group-{int(time.time())}"
        self.consumer = None

    def connect_kafka(self, **kwargs):
        """
        Common KafkaConsumer parameters:
            group_id='default-group',                 # 消费者组ID
            auto_offset_reset='earliest',             # 偏移量重置策略: 'earliest', 'latest', 'none'
            enable_auto_commit=True,                  # 是否自动提交偏移量
            auto_commit_interval_ms=5000,             # 自动提交偏移量间隔, 毫秒
            session_timeout_ms=30000,                 # 会话超时时间, 毫秒 (默认30秒)
            heartbeat_interval_ms=3000,               # 心跳间隔, 毫秒 (默认3秒)
            max_poll_records=500,                     # 单次poll最大记录数 (默认500)
            max_poll_interval_ms=300000,              # 两次poll间最大间隔, 毫秒 (默认5分钟)
            fetch_min_bytes=1,                        # 每次fetch最小字节数 (默认1字节)
            fetch_max_wait_ms=500,                    # fetch最大等待时间, 毫秒 (默认500ms)
            fetch_max_bytes=52428800,                 # 每次fetch最大字节数 (默认50MB)
            max_partition_fetch_bytes=1048576,        # 每个分区fetch最大字节数 (默认1MB)
            consumer_timeout_ms=None,                 # 消费者超时时间, 毫秒 (None表示无限等待)
            client_id='',                             # 客户端标识字符串
            check_crcs=True,                          # 是否检查CRC校验
            metadata_max_age_ms=300000,               # 元数据最大缓存时间, 毫秒 (默认5分钟)
            partition_assignment_strategy=['RoundRobinPartitionAssignor'],  # 分区分配策略
            security_protocol='PLAINTEXT',            # 安全协议
            api_version=(0, 10, 1),                   # Kafka API版本
        """
        default_config = {
            'group_id': self.group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            # x 为KafkaConsumer收到的消息value（二进制），自动传给value_deserializer
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
            # x 为接收到的消息 key（二进制字节串），自动传给 key_deserializer
            'key_deserializer': lambda x: x.decode('utf-8') if x else None,
        }

        params = {**default_config, **{k:v for k,v in kwargs.items() if k not in default_config}}
        # Merge user config with defaults
        config = {**params}

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            **config
        )

    def test_connection(self):
        """
        Test connection to Kafka brokers and topic

        Returns:
            dict: Connection test results
        """
        results = {'brokers': {}, 'topic_info': None}

        print("Testing Kafka broker connections...\n" + "=" * 60)

        for server in self.bootstrap_servers:
            try:
                test_consumer = KafkaConsumer(
                    bootstrap_servers=[server],
                    consumer_timeout_ms=5000,
                    auto_offset_reset='earliest'
                )

                metadata = test_consumer.partitions_for_topic(self.topic)
                if metadata:
                    results['brokers'][server] = {
                        'status': 'success',
                        'partitions': len(metadata),
                        'partition_ids': list(metadata)
                    }
                    print(f"Successfully connected to {server}\n   Topic '{self.topic}' has {len(metadata)} partitions")
                else:
                    results['brokers'][server] = {
                        'status': 'topic_not_found',
                        'message': f"Topic '{self.topic}' not found"
                    }
                    print(f"Connected to {server} but topic '{self.topic}' not found")

                test_consumer.close()

            except NoBrokersAvailable:
                results['brokers'][server] = {
                    'status': 'connection_failed',
                    'message': 'No brokers available'
                }
                print(f"Failed to connect to {server} - No brokers available")
            except Exception as e:
                results['brokers'][server] = {
                    'status': 'error',
                    'message': str(e)
                }
                print(f"Failed to connect to {server} - {e}")

        print("=" * 60)
        return results

    def consume_messages_once(self, max_messages=10, timeout_ms=30000, close_after_consume=False):
        """
        Consume a specific number of messages and stop

        Parameters:
            max_messages: Maximum number of messages to consume
            timeout_ms: Timeout in milliseconds
            close_after_consume: Whether to close consumer after consuming

        Returns:
            list: List of consumed messages
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        messages = []
        message_count = 0
        start_time = time.time()
        timeout_seconds = timeout_ms / 1000.0 if timeout_ms else None
        end_time = start_time + timeout_seconds if timeout_seconds else None

        print(f"Consuming up to {max_messages} messages from topic '{self.topic}'...\n" + "=" * 60)

        try:
            # Use poll() instead of iterator to support timeout
            poll_timeout_ms = min(1000, timeout_ms) if timeout_ms else 1000

            while message_count < max_messages:
                # Check timeout
                if end_time and time.time() >= end_time:
                    print(f"Timeout reached after {timeout_ms/1000:.1f}s")
                    break

                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=poll_timeout_ms)

                if not message_batch:
                    # No messages received, check if we should continue
                    if end_time and time.time() >= end_time:
                        print(f"Timeout reached after {timeout_ms/1000:.1f}s (no messages)")
                        break
                    continue

                # Process messages from batch
                for topic_partition, messages_list in message_batch.items():
                    for message in messages_list:
                        message_count += 1
                        elapsed_time = time.time() - start_time

                        message_info = {
                            'topic': message.topic,
                            'partition': message.partition,
                            'offset': message.offset,
                            'key': message.key,
                            'value': message.value,
                            'timestamp': message.timestamp,
                            'headers': message.headers
                        }
                        messages.append(message_info)

                        print(f"Message #{message_count} (after {elapsed_time:.2f}s): Topic={message.topic}, Partition={message.partition}, Offset={message.offset}, Key={message.key}, Value={message.value}\n" + "-" * 40)

                        if message_count >= max_messages:
                            print(f"Reached maximum message count ({max_messages}), stopping...")
                            break

                    if message_count >= max_messages:
                        break

                if message_count >= max_messages:
                    break

        except Exception as e:
            print(f"Error consuming messages: {e}")
            raise
        finally:
            if close_after_consume:
                self.close()

        print(f"Consumed {len(messages)} messages")
        return messages

    def consume_messages_with_timeout(self, timeout_seconds=30, close_after_consume=False):
        """
        Consume messages within a specified time limit

        Parameters:
            timeout_seconds: Timeout in seconds
            close_after_consume: Whether to close consumer after consuming

        Returns:
            list: List of consumed messages
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        messages = []
        message_count = 0
        start_time = time.time()
        end_time = start_time + timeout_seconds

        print(f"Consuming messages for {timeout_seconds} seconds from topic '{self.topic}'...\n" + "=" * 60)

        try:
            # Set a short consumer timeout to check time regularly
            self.consumer._consumer_timeout = 1.0

            while time.time() < end_time:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    for topic_partition, messages_list in message_batch.items():
                        for message in messages_list:
                            message_count += 1
                            elapsed_time = time.time() - start_time

                            message_info = {
                                'topic': message.topic,
                                'partition': message.partition,
                                'offset': message.offset,
                                'key': message.key,
                                'value': message.value,
                                'timestamp': message.timestamp,
                                'headers': message.headers
                            }
                            messages.append(message_info)

                            print(f"Message #{message_count} (after {elapsed_time:.2f}s): Topic={message.topic}, Partition={message.partition}, Offset={message.offset}, Value={message.value}\n" + "-" * 40)

                            if time.time() >= end_time:
                                break
                        if time.time() >= end_time:
                            break

                except Exception as e:
                    if "timeout" not in str(e).lower():
                        print(f"Error during polling: {e}")
                        break

        except KeyboardInterrupt:
            print("Consumption interrupted by user")
        finally:
            if close_after_consume:
                self.close()

        print(f"Consumed {len(messages)} messages in {timeout_seconds} seconds")
        return messages

    def consume_messages_continuous(self, callback=None, close_after_consume=False, timeout_seconds=None):
        """
        Consume messages continuously until interrupted or timeout

        Parameters:
            callback: Optional callback function to process each message
            close_after_consume: Whether to close consumer after consuming
            timeout_seconds: Optional timeout in seconds (None means no timeout)

        Returns:
            int: Total number of messages consumed
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        message_count = 0
        start_time = time.time()
        end_time = start_time + timeout_seconds if timeout_seconds else None
        poll_timeout_ms = 1000  # Poll timeout in milliseconds

        print(f"Starting continuous consumption from topic '{self.topic}'...")
        if timeout_seconds:
            print(f"Will timeout after {timeout_seconds} seconds")
        else:
            print("Press Ctrl+C to stop")
        print("=" * 60)

        try:
            while True:
                # Check timeout
                if end_time and time.time() >= end_time:
                    print(f"Timeout reached after {timeout_seconds}s")
                    break

                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=poll_timeout_ms)

                if not message_batch:
                    # No messages received, check if we should continue
                    if end_time and time.time() >= end_time:
                        print(f"Timeout reached after {timeout_seconds}s (no messages)")
                        break
                    continue

                # Process messages from batch
                for topic_partition, messages_list in message_batch.items():
                    for message in messages_list:
                        message_count += 1
                        elapsed_time = time.time() - start_time

                        callback_result = ""
                        if callback:
                            try:
                                callback(message)
                            except Exception as e:
                                callback_result = f" | Callback error: {e}"

                        print(f"Message #{message_count} (after {elapsed_time:.2f}s): Topic={message.topic}, Partition={message.partition}, Offset={message.offset}, Key={message.key}, Value={message.value}{callback_result}\n" + "-" * 40)

                        # Check timeout after processing
                        if end_time and time.time() >= end_time:
                            break

                    if end_time and time.time() >= end_time:
                        break

                if end_time and time.time() >= end_time:
                    break

        except KeyboardInterrupt:
            print("Consumption stopped by user")
        except Exception as e:
            print(f"Error during continuous consumption: {e}")
            raise
        finally:
            if close_after_consume:
                self.close()

        print(f"Total messages consumed: {message_count}")
        return message_count

    def consume_messages_with_callback(self, message_processor, max_messages=None, timeout_seconds=None, close_after_consume=False, shutdown_check=None):
        """
        Consume messages and process them with a custom callback function

        Parameters:
            message_processor: Function to process each message (message) -> bool
                              Return True to continue, False to stop
            max_messages: Maximum number of messages to consume (optional)
            timeout_seconds: Timeout in seconds (optional)
            close_after_consume: Whether to close consumer after consuming
            shutdown_check: Callable that returns True if should stop (optional)

        Returns:
            dict: Consumption statistics
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        if not callable(message_processor):
            raise ValueError("message_processor must be a callable function")

        stats = {
            'total_messages': 0,
            'processed_successfully': 0,
            'processing_errors': 0,
            'start_time': time.time(),
            'end_time': None
        }

        config_info = f"Consuming messages with callback from topic '{self.topic}'..."
        if max_messages:
            config_info += f"\nMax messages: {max_messages}"
        if timeout_seconds:
            config_info += f"\nTimeout: {timeout_seconds} seconds"
        print(config_info + "\n" + "=" * 60)

        try:
            end_time = time.time() + timeout_seconds if timeout_seconds else None
            poll_timeout_ms = 1000  # Poll timeout in milliseconds
            last_heartbeat_time = stats['start_time']  # Track last heartbeat time

            while True:
                # Check shutdown flag first
                if shutdown_check and shutdown_check():
                    print("Shutdown flag detected, stopping gracefully...")
                    break

                # Check timeout
                if end_time and time.time() >= end_time:
                    print(f"Timeout reached ({timeout_seconds}s), stopping...")
                    break

                # Check max messages limit
                if max_messages and stats['total_messages'] >= max_messages:
                    print(f"Reached maximum message count ({max_messages}), stopping...")
                    break

                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=poll_timeout_ms)

                if not message_batch:
                    # Check shutdown flag again when no messages
                    if shutdown_check and shutdown_check():
                        print("Shutdown flag detected, stopping gracefully...")
                        break
                    # No messages received, check if we should continue
                    if end_time and time.time() >= end_time:
                        print(f"Timeout reached ({timeout_seconds}s) (no messages), stopping...")
                        break
                    # Print heartbeat every 30 seconds when waiting for messages
                    current_time = time.time()
                    elapsed = current_time - stats['start_time']
                    # Check if 30 seconds have passed since last heartbeat
                    if timeout_seconds is None and (current_time - last_heartbeat_time) >= 30:
                        print(f"[Heartbeat] Still waiting for messages... (running for {int(elapsed)}s)")
                        last_heartbeat_time = current_time
                    continue

                # Process messages from batch
                for topic_partition, messages_list in message_batch.items():
                    for message in messages_list:
                        stats['total_messages'] += 1
                        elapsed_time = time.time() - stats['start_time']

                        print(f"Message #{stats['total_messages']} (after {elapsed_time:.2f}s): Topic={message.topic}, Partition={message.partition}, Offset={message.offset}")

                        # Process message with callback
                        try:
                            print(message.value)
                            should_continue = message_processor(message)
                            stats['processed_successfully'] += 1

                            if should_continue is False:
                                print("Callback returned False, stopping consumption...")
                                break

                        except Exception as e:
                            stats['processing_errors'] += 1
                            print(f"Processing error: {e}")

                        print("-" * 40)

                        # Check shutdown flag after processing
                        if shutdown_check and shutdown_check():
                            print("Shutdown flag detected, stopping gracefully...")
                            break

                        # Check limits after processing
                        if max_messages and stats['total_messages'] >= max_messages:
                            print(f"Reached maximum message count ({max_messages}), stopping...")
                            break

                        if end_time and time.time() >= end_time:
                            print(f"Timeout reached ({timeout_seconds}s), stopping...")
                            break

                    # Break outer loop if needed
                    if shutdown_check and shutdown_check():
                        break
                    if max_messages and stats['total_messages'] >= max_messages:
                        break
                    if end_time and time.time() >= end_time:
                        break

                # Break main loop if needed
                if shutdown_check and shutdown_check():
                    break
                if max_messages and stats['total_messages'] >= max_messages:
                    break
                if end_time and time.time() >= end_time:
                    break

        except KeyboardInterrupt:
            print("Consumption interrupted by user")
        except Exception as e:
            print(f"Error during callback consumption: {e}")
            raise
        finally:
            stats['end_time'] = time.time()
            if close_after_consume:
                self.close()

        stats['duration'] = stats['end_time'] - stats['start_time']
        print(f"Consumption completed:\n  Total messages: {stats['total_messages']}\n  Successfully processed: {stats['processed_successfully']}\n  Processing errors: {stats['processing_errors']}\n  Duration: {stats['duration']:.2f} seconds")

        return stats

    def consume_from_specific_offset(self, partition, offset, max_messages=10, timeout_ms=30000, close_after_consume=False):
        """
        Consume messages starting from a specific offset

        Parameters:
            partition: 分区号。对于 test-topic-loop 这个只有一个分区的 topic,这里应该填 0(即 partition=0)。
            offset: 起始 offset。
            max_messages: 最多消费的消息数。
            timeout_ms: 消费超时时间（毫秒）。
            close_after_consume: 消费结束后是否关闭 consumer。

        Returns:
            list: 消费到的消息列表。
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        messages = []
        message_count = 0
        start_time = time.time()
        timeout_seconds = timeout_ms / 1000.0 if timeout_ms else None
        end_time = start_time + timeout_seconds if timeout_seconds else None

        print(f"Consuming from topic '{self.topic}', partition {partition}, starting at offset {offset}...\n" + "=" * 60)

        try:
            # Unsubscribe from current subscription before assigning specific partition
            # Kafka consumer cannot use both subscribe() and assign() at the same time
            if self.consumer.subscription():
                self.consumer.unsubscribe()

            # Create TopicPartition and seek to specific offset
            topic_partition = TopicPartition(self.topic, partition)
            self.consumer.assign([topic_partition])
            self.consumer.seek(topic_partition, offset)

            print(f"Seeked to partition {partition}, offset {offset}")

            # Use poll() instead of iterator to support timeout
            poll_timeout_ms = min(1000, timeout_ms) if timeout_ms else 1000

            while message_count < max_messages:
                # Check timeout
                if end_time and time.time() >= end_time:
                    print(f"Timeout reached after {timeout_ms/1000:.1f}s")
                    break

                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=poll_timeout_ms)

                if not message_batch:
                    # No messages received, check if we should continue
                    if end_time and time.time() >= end_time:
                        print(f"Timeout reached after {timeout_ms/1000:.1f}s (no messages)")
                        break
                    continue

                # Process messages from batch
                for topic_part, messages_list in message_batch.items():
                    for message in messages_list:
                        message_count += 1

                        message_info = {
                            'topic': message.topic,
                            'partition': message.partition,
                            'offset': message.offset,
                            'key': message.key,
                            'value': message.value,
                            'timestamp': message.timestamp,
                            'headers': message.headers
                        }
                        messages.append(message_info)

                        print(f"Message #{message_count}: Partition={message.partition}, Offset={message.offset}, Key={message.key}, Value={message.value}\n" + "-" * 40)

                        if message_count >= max_messages:
                            print(f"Reached maximum message count ({max_messages}), stopping...")
                            break

                    if message_count >= max_messages:
                        break

                if message_count >= max_messages:
                    break

        except Exception as e:
            print(f"Error consuming from specific offset: {e}")
            raise
        finally:
            if close_after_consume:
                self.close()

        print(f"Consumed {len(messages)} messages from offset {offset}")
        return messages

    def consume_latest_messages(self, max_messages=10, timeout_seconds=30, close_after_consume=False):
        """
        Consume only the latest messages, skipping historical data

        Parameters:
            max_messages: Maximum number of messages to consume
            timeout_seconds: Timeout in seconds
            close_after_consume: Whether to close consumer after consuming

        Returns:
            list: List of consumed messages
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        messages = []
        message_count = 0
        start_time = time.time()
        end_time = start_time + timeout_seconds

        print(f"Consuming latest messages from topic '{self.topic}'...\n" + "=" * 60)

        try:
            # Unsubscribe from current subscription before assigning specific partitions
            # Kafka consumer cannot use both subscribe() and assign() at the same time
            if self.consumer.subscription():
                self.consumer.unsubscribe()

            # Seek to end of all partitions
            partitions = self.consumer.partitions_for_topic(self.topic)
            if partitions:
                topic_partitions = [TopicPartition(self.topic, p) for p in partitions]
                self.consumer.assign(topic_partitions)
                self.consumer.seek_to_end()
                print(f"Seeked to end of all partitions: {list(partitions)}")

            # Set consumer timeout
            self.consumer._consumer_timeout = 1.0

            while time.time() < end_time and message_count < max_messages:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    for topic_partition, messages_list in message_batch.items():
                        for message in messages_list:
                            message_count += 1
                            elapsed_time = time.time() - start_time

                            message_info = {
                                'topic': message.topic,
                                'partition': message.partition,
                                'offset': message.offset,
                                'key': message.key,
                                'value': message.value,
                                'timestamp': message.timestamp,
                                'headers': message.headers
                            }
                            messages.append(message_info)

                            print(f"Latest Message #{message_count} (after {elapsed_time:.2f}s): Partition={message.partition}, Offset={message.offset}, Value={message.value}\n" + "-" * 40)

                            if message_count >= max_messages:
                                break
                        if message_count >= max_messages:
                            break

                except Exception as e:
                    if "timeout" not in str(e).lower():
                        print(f"Error during polling: {e}")
                        break

        except Exception as e:
            print(f"Error consuming latest messages: {e}")
            raise
        finally:
            if close_after_consume:
                self.close()

        print(f"Consumed {len(messages)} latest messages")
        return messages

    def get_topic_partitions_info(self):
        """
        Get information about topic partitions

        Returns:
            dict: Partition information
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect_kafka() first.")

        try:
            partitions = self.consumer.partitions_for_topic(self.topic)
            if not partitions:
                return {'error': f"Topic '{self.topic}' not found or has no partitions"}

            partition_info = {}
            topic_partitions = [TopicPartition(self.topic, p) for p in partitions]

            # Get beginning and end offsets
            beginning_offsets = self.consumer.beginning_offsets(topic_partitions)
            end_offsets = self.consumer.end_offsets(topic_partitions)

            for tp in topic_partitions:
                partition_info[tp.partition] = {
                    'beginning_offset': beginning_offsets.get(tp, -1),
                    'end_offset': end_offsets.get(tp, -1),
                    'message_count': end_offsets.get(tp, 0) - beginning_offsets.get(tp, 0)
                }

            return {
                'topic': self.topic,
                'partition_count': len(partitions),
                'partitions': partition_info,
                'total_messages': sum(info['message_count'] for info in partition_info.values())
            }

        except Exception as e:
            return {'error': str(e)}

    def close(self):
        """
        Close the consumer connection
        """
        if self.consumer:
            self.consumer.close()
            print("Kafka consumer connection closed")


if __name__ == "__main__":
    # Example usage
    consumer = KafkaConsumerUtils(
        bootstrap_servers=["localhost:29092", "localhost:39092"],
        topic="test-topic-sync-batch",
        group_id="test-consumer-group"
    )

    # Test connection
    consumer.test_connection()

    # Connect to Kafka
    consumer.connect_kafka()

    # Consume some messages
    messages = consumer.consume_messages_once(max_messages=5)
    print(f"Consumed {len(messages)} messages")

    # Close consumer
    consumer.close()