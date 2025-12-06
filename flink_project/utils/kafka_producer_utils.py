#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
kafka-python 2.2.11

消息发送方法说明:

1. send_batch_messages:
   - 功能: 循环发送, 每条消息立即flush, 间隔0.2秒
   - 用途: 小批量测试, 低吞吐场景

2. send_message_sync:
   - 功能: 同步发送, 阻塞等待确认, 返回RecordMetadata
   - 用途: 需要立刻确认的重要消息, 需获取offset

3. send_messages_sync_wait:
   - 功能: 批量同步发送, 等待所有消息确认, 返回RecordMetadata列表
   - 用途: 批量数据迁移, ETL流程, 需详细追踪消息状态

4. send_messages_with_callback:
   - 功能: 异步发送, 非阻塞, 回调处理结果, 高吞吐
   - 用途: 高吞吐场景, 实时数据流, 生产环境大批量消息

性能对比: send_messages_with_callback > send_messages_sync_wait > send_message_sync > send_batch_messages
可靠性对比: send_message_sync = send_messages_sync_wait > send_batch_messages = send_messages_with_callback

"""

import time
import json
from typing import List, Dict
from kafka import KafkaProducer


class KafkaProducerUtils:

    def __init__(self, bootstrap_servers, topic):

        self.bootstrap_servers = bootstrap_servers if isinstance(bootstrap_servers, list) else [bootstrap_servers]
        self.topic = topic
        self.producer = None

    def connect_kafka(self, **kwargs):
        """
        Common KafkaProducer parameters:
            acks='all',                           # 消息确认机制: 0, 1, 或 'all'/-1
            retries=3,                            # 发生错误时最大重试次数
            batch_size=16384,                     # 批量发送的字节数阈值 (默认 16384, 16KB)
            linger_ms=10,                         # 批量发送最大等待时间, 单位毫秒
            request_timeout_ms=30000,             # 发送请求超时时间, 单位毫秒 (默认30000)
            compression_type='gzip',              # 消息压缩类型, 可选 'none', 'gzip', 'snappy', 'lz4'
            buffer_memory=33554432,               # 发送端用于缓冲消息的总内存字节数 (默认32MB)
            max_in_flight_requests_per_connection=5,  # 每个连接未完成请求的最大数量(默认5)
            client_id='',                         # 客户端标识字符串
            enable_idempotence=False,             # 是否开启幂等, 保证消息“恰好一次”投递
            max_request_size=1048576,             # 单个请求最大字节数 (默认1MB)
            metadata_max_age_ms=300000,           # 元数据刷新间隔, 单位毫秒 (默认5分钟)
            send_buffer_bytes=131072,             # TCP发送缓冲区大小, 字节 (默认128KB)
            receive_buffer_bytes=32768,           # TCP接收缓冲区大小, 字节 (默认32KB)
            max_block_ms=60000,                   # 当缓冲区已满时最大阻塞时间, 毫秒 (默认60秒)
            api_version=(0, 10, 1),               # Kafka API 版本号
            security_protocol='PLAINTEXT',        # 连接安全协议类型
        """
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            # 这里的 x 来自 producer.send(self.topic, message, key=key) 调用时传入的 message 参数。
            # KafkaProducer 调用 value_serializer 时会将 send 传入的 message (即方法参数 message) 赋值给 x。
            value_serializer=lambda x: json.dumps(x).encode('utf-8') if x is not None else None,
            # 这里的 x 来自 producer.send(self.topic, message, key=key) 调用时传入的 key 参数。
            # KafkaProducer 调用 key_serializer 时会将 send 传入的 key (即方法参数 key) 赋值给 x。
            key_serializer=lambda x: x.encode('utf-8') if x is not None else None,
            **kwargs
            )

    def send_single_message(self, message, key=None, close_after_send=False):
        """
        Send a single message to the Kafka topic
        """
        self.producer.send(self.topic, message, key=key)
        self.producer.flush()

        if close_after_send:
            self.producer.close()

    def send_single_message_without_flush(self, message, key=None, close_after_send=False):
        """
        Send a single message to the Kafka topic without flushing
        """
        self.producer.send(self.topic, message, key=key)
        if close_after_send:
            self.producer.close()

    def send_batch_messages(self, messages: List[Dict], key: str = None, key_field: str = None, close_after_send=False, flush_count=10):
        """
        Send a batch of messages to the Kafka topic

        Parameters:
            messages: List of messages to send
            # 要发送的消息列表
            key: Prefix for message key (e.g., "order" will create "order-1", "order-2", etc.)
            # 消息 key 的前缀（例如 "order"，会生成 "order-1", "order-2" 等 key）
            key_field: Field name in message dict to use as key (e.g., "orderId")
            # 用于作为 key 的字段名（假如给定 "orderId"，每条消息的对应字段会当 key 用）
            close_after_send: Whether to close producer after sending
            # 发送完消息后是否关闭 producer
            flush_count: Flush producer every N messages
            # 每 N 条消息后 flush 一次 producer
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list of dictionaries")

        for i, message in enumerate(messages, start=1):

            if key_field and isinstance(message, dict):
                # Safely extract key from message dictionary
                _key = message.get(key_field, i)
            elif key:
                _key = f"{key}-{i}"
            else:
                _key = i

            self.send_single_message(message, key=_key)

            if flush_count:
                quotient, remainder = divmod(i, flush_count)
                if quotient > 0 and remainder == 0:
                    self.producer.flush()

            time.sleep(0.2)

        self.producer.flush()

        print("send batch messages success, total messages:", len(messages))
        if close_after_send:
            self.producer.close()

    def send_message_sync(self, message, key=None, timeout=None, close_after_send=False):
        """
        Send a message synchronously and wait for the result to ensure the message is sent.
        Uses producer.send().get() to block until the message is acknowledged.

        Parameters:
            message: Message to send (dict)
            key: Message key (str, optional)
            timeout: Timeout in seconds for waiting for the result (optional)
            close_after_send: Whether to close producer after sending message

        Returns:
            RecordMetadata: Metadata about the sent message (topic, partition, offset)
        """
        try:
            future = self.producer.send(self.topic, value=message, key=key)
            record_metadata = future.get(timeout=timeout) if timeout else future.get()
            return record_metadata
        except Exception as e:
            print(f"Error sending message synchronously: {e}")
            raise
        finally:
            if close_after_send:
                self.producer.close()

    def send_messages_sync_wait(
        self,
        messages: List[Dict],
        key: str = None,
        key_field: str = None,
        timeout=None,
        close_after_send=False,
        flush_count=10
    ):
        """
        Send multiple messages synchronously and wait for all results.
        Ensures all messages are sent before returning.

        Parameters:
            messages: List of messages to send, each element should be a dict
            key: Prefix for message key (e.g., "order" will create "order-1", "order-2", etc.)
            key_field: Field name in message dict to use as key (e.g., "orderId")
            timeout: Timeout in seconds for waiting for each result (optional)
            close_after_send: Whether to close producer after sending messages
            flush_count: Flush producer every N messages

        Returns:
            list: List of RecordMetadata objects for all sent messages
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list of dictionaries")

        results = []
        try:
            for i, message in enumerate(messages, start=1):
                if key_field and isinstance(message, dict):
                    # Safely extract key from message dictionary
                    message_key = message.get(key_field, i)
                elif key:
                    message_key = f"{key}-{i}"
                else:
                    message_key = None
                future = self.producer.send(
                    self.topic, value=message, key=message_key
                )
                record_metadata = (
                    future.get(timeout=timeout) if timeout else future.get()
                )
                results.append(record_metadata)
                print(
                    f"Message {i} sent successfully: "
                    f"topic={record_metadata.topic} "
                    f"partition={record_metadata.partition} "
                    f"offset={record_metadata.offset}"
                )
                # Flush every flush_count messages if flush_count > 0
                if flush_count:
                    quotient, remainder = divmod(i, flush_count)
                    if quotient > 0 and remainder == 0:
                        self.producer.flush()

                time.sleep(0.2)
        except Exception as e:
            print(f"Error sending messages synchronously: {e}")
            raise
        finally:
            # Final flush after all messages are sent
            self.producer.flush()
            if close_after_send:
                self.producer.close()

        return results


    def send_messages_with_callback(self, messages: List[Dict], key: str = None, key_field: str = None, close_after_send=False, flush_count=10):
        """
        发送带有回调函数的消息队列。
        callback 的作用：
        - 发送消息到 Kafka 时，可以提供一个回调函数 callback, 用于处理消息发送成功或失败的情况。
          当消息被 broker 成功接收（或失败）后会自动异步调用此回调。
        - 通过回调，可以获取到消息的发送结果，执行日志记录、异常处理等操作。

        Parameters:
            messages: List of messages, each element should be a dict
            key: Prefix for message key (e.g., "order" will create "order-1", "order-2", etc.)
            key_field: Field name in message dict to use as key (e.g., "orderId")
            close_after_send: Whether to close producer after sending messages
            flush_count: Flush the producer every flush_count messages
        """
        if not isinstance(messages, list):
            raise ValueError("messages must be a list of dictionaries")

        def on_send_success(record_metadata):
            print(f"Message sent to topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}")

        def on_send_error(excp):
            print(f"Message delivery failed: {excp}")

        for i, message in enumerate(messages, start=1):
            if key_field and isinstance(message, dict):
                # Safely extract key from message dictionary
                message_key = message.get(key_field, i)
            elif key:
                message_key = f"{key}-{i}"
            else:
                message_key = None
            future = self.producer.send(self.topic, value=message, key=message_key)
            # Add callbacks to handle message send success or failure.
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            if flush_count:
                quotient, remainder = divmod(i, flush_count)
                if quotient > 0 and remainder == 0:
                    self.producer.flush()

            time.sleep(0.2)

        self.producer.flush()
        if close_after_send:
            self.producer.close()

    def close(self):
        self.producer.close()


if __name__ == "__main__":
    producer = KafkaProducerUtils(bootstrap_servers="localhost:9092", topic="test-topic")
    producer.connect_kafka()
    producer.send_batch_messages([{"message": "Hello, World!"}], key="test")
    producer.close()
