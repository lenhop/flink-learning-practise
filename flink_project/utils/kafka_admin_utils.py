#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
python 3.11
confluent-kafka 2.12.1

Kafka Admin Utils - Topic management functions

功能说明:
Topic 管理:
1. create_topic: 创建 topic
2. list_topics: 列出所有 topic
3. delete_topic: 删除 topic
4. get_topic_message_count: 检查 topic 的消息数量
5. get_topic_info: 获取 topic 详细信息
6. alter_topic_config: 修改 topic 配置
7. describe_topic_config: 查看 topic 配置
8. increase_partitions: 增加 topic 分区数
9. check_topic_exists: 检查 topic 是否存在

Consumer Group 管理:
10. list_consumer_groups: 列出所有 consumer group
11. describe_consumer_groups: 描述 consumer group 详情
12. get_consumer_group_offsets: 获取 consumer group 的 offset
13. alter_consumer_group_offsets: 修改 consumer group 的 offset
14. get_consumer_lag: 获取 consumer lag

集群管理:
15. describe_cluster: 描述集群信息

"""

import time
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigEntry
from confluent_kafka import TopicPartition


class KafkaAdminUtils:

    def __init__(self, bootstrap_servers):
        """
        Initialize Kafka Admin Client

        Parameters:
            bootstrap_servers: Kafka broker addresses, can be string or list
                              e.g., "localhost:9092" or ["localhost:9092", "localhost:9093"]
        """
        if isinstance(bootstrap_servers, list):
            bootstrap_servers = ','.join(bootstrap_servers)

        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1, config=None, timeout=10):
        """
        Create a new Kafka topic

        Parameters:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions (default: 1)
            replication_factor: Replication factor (default: 1)
            config: Optional topic configuration dict (default: None)
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Result of the operation with topic name and status
        """
        # Convert config values to strings if config is provided
        # NewTopic requires config to be a dict of strings or None
        if config is not None:
            config = {str(k): str(v) for k, v in config.items()}

        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config if config is not None else {}
        )

        futures = self.admin_client.create_topics([new_topic], request_timeout=timeout)

        result = {}
        for topic, future in futures.items():
            try:
                future.result(timeout=timeout)
                result[topic] = {'status': 'success', 'message': f'Topic {topic} created successfully'}
            except Exception as e:
                result[topic] = {'status': 'error', 'message': str(e)}

        return result

    def list_topics(self, timeout=10):
        """
        List all topics in the Kafka cluster

        Parameters:
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Dictionary with topic names as keys and metadata as values
        """
        try:
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)
            topics = {}

            for topic_name, topic_metadata in cluster_metadata.topics.items():
                topics[topic_name] = {
                    'partitions': len(topic_metadata.partitions),
                    'partition_ids': list(topic_metadata.partitions.keys())
                }

            return topics
        except Exception as e:
            return {'error': str(e)}

    def delete_topic(self, topic_name, timeout=10):
        """
        Delete a Kafka topic

        Parameters:
            topic_name: Name of the topic to delete
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Result of the operation with topic name and status
        """
        futures = self.admin_client.delete_topics([topic_name], request_timeout=timeout)

        result = {}
        for topic, future in futures.items():
            try:
                future.result(timeout=timeout)
                result[topic] = {'status': 'success', 'message': f'Topic {topic} deleted successfully'}
            except Exception as e:
                result[topic] = {'status': 'error', 'message': str(e)}

        return result

    def get_topic_message_count(self, topic_name, timeout=10):
        """
        Get the total message count for a specific topic

        Parameters:
            topic_name: Name of the topic to check
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Dictionary with partition information and total message count
        """
        try:
            # Get topic metadata to find partitions
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)

            if topic_name not in cluster_metadata.topics:
                return {'error': f'Topic {topic_name} not found'}

            topic_metadata = cluster_metadata.topics[topic_name]
            partitions = topic_metadata.partitions

            # Create consumer to get offsets
            consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': f'topic_count_checker_{int(time.time())}',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })

            # Get partition assignments
            from confluent_kafka import TopicPartition
            topic_partitions = [TopicPartition(topic_name, p) for p in partitions.keys()]
            consumer.assign(topic_partitions)

            # Get beginning and end offsets for each partition
            partition_info = {}
            total_messages = 0

            for tp in topic_partitions:
                low, high = consumer.get_watermark_offsets(tp, timeout=timeout)
                partition_info[tp.partition] = {
                    'beginning_offset': low,
                    'end_offset': high,
                    'message_count': high - low
                }
                total_messages += (high - low)

            consumer.close()

            return {
                'topic': topic_name,
                'partitions': partition_info,
                'total_messages': total_messages,
                'partition_count': len(partitions)
            }

        except Exception as e:
            return {'error': str(e)}

    def get_topic_info(self, topic_name, timeout=10):
        """
        Get detailed information about a specific topic

        Parameters:
            topic_name: Name of the topic to get info for
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Detailed topic information including partitions, replicas, etc.
        """
        try:
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)

            if topic_name not in cluster_metadata.topics:
                return {'error': f'Topic {topic_name} not found'}

            topic_metadata = cluster_metadata.topics[topic_name]
            partitions = topic_metadata.partitions

            partition_info = {}
            for partition_id, partition_metadata in partitions.items():
                partition_info[partition_id] = {
                    'leader': partition_metadata.leader,
                    'replicas': partition_metadata.replicas,
                    'isrs': partition_metadata.isrs
                }

            # Get message count
            message_count_info = self.get_topic_message_count(topic_name, timeout)

            return {
                'topic': topic_name,
                'partition_count': len(partitions),
                'partitions': partition_info,
                'message_count': message_count_info.get('total_messages', 0),
                'partition_message_counts': message_count_info.get('partitions', {})
            }

        except Exception as e:
            return {'error': str(e)}

    def alter_topic_config(self, topic_name, config, timeout=10):
        """
        Alter topic configuration

        Parameters:
            topic_name: Name of the topic to alter
            config: Dictionary of config key-value pairs to set
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Result of the operation
        """
        try:
            # Convert config values to strings - ConfigResource expects a dict of str, str
            config_dict = {k: str(v) for k, v in config.items()}
            config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name, set_config=config_dict)

            futures = self.admin_client.alter_configs([config_resource], request_timeout=timeout)

            result = {}
            for resource, future in futures.items():
                try:
                    future.result(timeout=timeout)
                    result[resource.name] = {'status': 'success', 'message': f'Topic {resource.name} config updated successfully'}
                except Exception as e:
                    result[resource.name] = {'status': 'error', 'message': str(e)}

            return result
        except Exception as e:
            return {'error': str(e)}

    def describe_topic_config(self, topic_name, timeout=10):
        """
        Describe topic configuration

        Parameters:
            topic_name: Name of the topic to describe
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Topic configuration details
        """
        try:
            config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
            futures = self.admin_client.describe_configs([config_resource], request_timeout=timeout)

            result = {}
            for resource, future in futures.items():
                try:
                    config = future.result(timeout=timeout)
                    config_dict = {}
                    for entry in config.values():
                        config_dict[entry.name] = {
                            'value': entry.value,
                            'is_default': entry.is_default,
                            'is_read_only': entry.is_read_only,
                            'is_sensitive': entry.is_sensitive
                        }
                    result[resource.name] = config_dict
                except Exception as e:
                    result[resource.name] = {'error': str(e)}

            return result
        except Exception as e:
            return {'error': str(e)}

    def increase_partitions(self, topic_name, new_total_partitions, timeout=10):
        """
        Increase the number of partitions for a topic

        Parameters:
            topic_name: Name of the topic
            new_total_partitions: New total number of partitions (must be greater than current)
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Result of the operation
        """
        try:
            # Get current partition count
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)
            if topic_name not in cluster_metadata.topics:
                return {'error': f'Topic {topic_name} not found'}

            current_partitions = len(cluster_metadata.topics[topic_name].partitions)
            if new_total_partitions <= current_partitions:
                return {'error': f'New partition count ({new_total_partitions}) must be greater than current ({current_partitions})'}

            # Create new partitions - use new_total_count instead of new_total_partitions
            new_partitions = NewPartitions(topic_name, new_total_count=new_total_partitions)
            futures = self.admin_client.create_partitions([new_partitions], request_timeout=timeout)

            result = {}
            for topic, future in futures.items():
                try:
                    future.result(timeout=timeout)
                    result[topic] = {'status': 'success', 'message': f'Topic {topic} partitions increased to {new_total_partitions}'}
                except Exception as e:
                    result[topic] = {'status': 'error', 'message': str(e)}

            return result
        except Exception as e:
            return {'error': str(e)}

    def check_topic_exists(self, topic_name, timeout=10):
        """
        Check if a topic exists

        Parameters:
            topic_name: Name of the topic to check
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            bool: True if topic exists, False otherwise
        """
        try:
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)
            return topic_name in cluster_metadata.topics
        except Exception as e:
            return False

    def list_consumer_groups(self, timeout=10):
        """
        List all consumer groups in the cluster

        Parameters:
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Dictionary with consumer group information
        """
        try:
            # Note: confluent-kafka AdminClient doesn't have direct list_consumer_groups
            # We need to use Consumer to get consumer groups
            # This is a workaround using Consumer API
            consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': f'admin_list_groups_{int(time.time())}',
                'enable.auto.commit': False
            })

            # Get cluster metadata which includes consumer groups info
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)

            # Note: confluent-kafka AdminClient doesn't directly support listing consumer groups
            # This would typically require using kafka-python or Kafka's native admin tools
            # For now, return a message indicating this limitation
            consumer.close()
            return {'note': 'Consumer group listing requires additional API support. Consider using kafka-python AdminClient or Kafka CLI tools.'}
        except Exception as e:
            return {'error': str(e)}

    def describe_consumer_groups(self, group_ids, timeout=10):
        """
        Describe consumer groups

        Parameters:
            group_ids: List of consumer group IDs to describe
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Consumer group details
        """
        try:
            # Note: confluent-kafka AdminClient doesn't have direct describe_consumer_groups
            # This would require using Consumer API or kafka-python
            return {'note': 'Consumer group description requires additional API support. Consider using kafka-python AdminClient or Kafka CLI tools.'}
        except Exception as e:
            return {'error': str(e)}

    def get_consumer_group_offsets(self, group_id, timeout=10):
        """
        Get consumer group offsets for all topics

        Parameters:
            group_id: Consumer group ID
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Consumer group offsets information
        """
        try:
            consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'enable.auto.commit': False
            })

            # Get committed offsets
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)
            offsets = {}

            for topic_name in cluster_metadata.topics.keys():
                try:
                    partitions = [TopicPartition(topic_name, p) for p in cluster_metadata.topics[topic_name].partitions.keys()]
                    committed = consumer.committed(partitions, timeout=timeout)

                    topic_offsets = {}
                    for tp in committed:
                        if tp.offset >= 0:
                            topic_offsets[tp.partition] = {
                                'offset': tp.offset,
                                'metadata': tp.metadata
                            }

                    if topic_offsets:
                        offsets[topic_name] = topic_offsets
                except Exception:
                    continue

            consumer.close()
            return {'group_id': group_id, 'offsets': offsets}
        except Exception as e:
            return {'error': str(e)}

    def alter_consumer_group_offsets(self, group_id, topic_partitions, timeout=10):
        """
        Alter consumer group offsets

        Parameters:
            group_id: Consumer group ID
            topic_partitions: Dictionary with topic names as keys and partition offsets as values
                            e.g., {'topic1': {0: 100, 1: 200}, 'topic2': {0: 50}}
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Result of the operation
        """
        try:
            consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'enable.auto.commit': False
            })

            # Create TopicPartition list with new offsets
            topic_partition_list = []
            for topic_name, partitions in topic_partitions.items():
                for partition_id, offset in partitions.items():
                    tp = TopicPartition(topic_name, partition_id, offset)
                    topic_partition_list.append(tp)

            # Commit new offsets
            consumer.commit(offsets=topic_partition_list, asynchronous=False)

            consumer.close()
            return {'status': 'success', 'message': f'Consumer group {group_id} offsets updated successfully'}
        except Exception as e:
            return {'error': str(e)}

    def get_consumer_lag(self, group_id, topic_name, timeout=10):
        """
        Get consumer lag for a specific topic

        Parameters:
            group_id: Consumer group ID
            topic_name: Name of the topic
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Consumer lag information
        """
        try:
            consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'enable.auto.commit': False
            })

            # Get topic partitions
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)
            if topic_name not in cluster_metadata.topics:
                consumer.close()
                return {'error': f'Topic {topic_name} not found'}

            partitions = cluster_metadata.topics[topic_name].partitions
            topic_partitions = [TopicPartition(topic_name, p) for p in partitions.keys()]
            consumer.assign(topic_partitions)

            # Get committed offsets and end offsets
            committed = consumer.committed(topic_partitions, timeout=timeout)
            lag_info = {}
            total_lag = 0

            for tp in topic_partitions:
                # Get committed offset
                committed_tp = next((ctp for ctp in committed if ctp.partition == tp.partition), None)
                committed_offset = committed_tp.offset if committed_tp and committed_tp.offset >= 0 else 0

                # Get end offset
                low, high = consumer.get_watermark_offsets(tp, timeout=timeout)
                lag = high - committed_offset

                lag_info[tp.partition] = {
                    'committed_offset': committed_offset,
                    'end_offset': high,
                    'lag': lag
                }
                total_lag += lag

            consumer.close()

            return {
                'group_id': group_id,
                'topic': topic_name,
                'partitions': lag_info,
                'total_lag': total_lag
            }
        except Exception as e:
            return {'error': str(e)}

    def describe_cluster(self, timeout=10):
        """
        Describe cluster information

        Parameters:
            timeout: Timeout in seconds for the operation (default: 10)

        Returns:
            dict: Cluster information including brokers, controller, etc.
        """
        try:
            cluster_metadata = self.admin_client.list_topics(timeout=timeout)

            # Get broker information from cluster metadata
            brokers = {}
            for broker_id, broker_metadata in cluster_metadata.brokers.items():
                brokers[broker_id] = {
                    'id': broker_id,
                    'host': broker_metadata.host,
                    'port': broker_metadata.port,
                    'rack': broker_metadata.rack if hasattr(broker_metadata, 'rack') else None
                }

            return {
                'cluster_id': cluster_metadata.cluster_id if hasattr(cluster_metadata, 'cluster_id') else None,
                'controller_id': cluster_metadata.controller_id if hasattr(cluster_metadata, 'controller_id') else None,
                'brokers': brokers,
                'broker_count': len(brokers),
                'topic_count': len(cluster_metadata.topics)
            }
        except Exception as e:
            return {'error': str(e)}

    def close(self):
        """
        Close the admin client connection
        Note: AdminClient doesn't have an explicit close method in confluent-kafka
        This method is provided for consistency with other utils classes
        """
        pass


if __name__ == "__main__":
    # Example usage
    admin = KafkaAdminUtils(bootstrap_servers=["localhost:29092", "localhost:39092"])

    # List all topics
    print("=== List All Topics ===")
    topics = admin.list_topics()
    print(topics)

    # Create a new topic
    print("\n=== Create Topic ===")
    result = admin.create_topic("test-admin-topic", num_partitions=1, replication_factor=1)
    print(result)

    # Get topic message count
    print("\n=== Get Topic Message Count ===")
    count_info = admin.get_topic_message_count("test-topic-sync-batch")
    print(count_info)

    # Get topic info
    print("\n=== Get Topic Info ===")
    topic_info = admin.get_topic_info("test-topic-sync-batch")
    print(topic_info)

