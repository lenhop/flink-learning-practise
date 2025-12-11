#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 2: Prepare and add connector JARs to Flink environment.

Split from flink_walmart_order_pipeline.py to keep steps clear:
1) Collect required JAR paths (Kafka, optional MySQL)
2) Add JARs to an existing StreamExecutionEnvironment

Usage:
    from flink2_add_jar_to_flink import FlinkJarManager
    jar_files = FlinkJarManager.prepare_all_jars(flink_project_path, logger=logger)
    FlinkJarManager.add_jars_to_env(env, jar_files, logger=logger)
"""

import os

from pyflink.datastream import StreamExecutionEnvironment

# logger will be passed in by caller (reuse pipeline logger to keep log format consistent)


class FlinkJarManager:
    """Manage connector JARs for Flink (Kafka mandatory, MySQL optional)."""

    @classmethod
    def _ensure_jar(cls, jar_path, logger=None, mandatory=True):
        """Return file:// URI if exists; raise if mandatory and missing."""
        if os.path.exists(jar_path):
            uri = f"file://{os.path.abspath(jar_path)}"
            if logger:
                logger.info(f"Found JAR: {os.path.basename(jar_path)}")
            return uri
        if mandatory:
            if logger:
                logger.error(f"JAR file not found: {jar_path}")
            raise FileNotFoundError(f"JAR file not found: {jar_path}")
        if logger:
            logger.warning(f"Optional JAR not found, skip: {jar_path}")
        return None

    @classmethod
    def add_kafka_jars(cls, env: StreamExecutionEnvironment, jar_dir, jar_name=None, clients_jar_name=None, logger=None):
        """
        将 Kafka 相关 JAR 直接 add 到 Flink StreamExecutionEnvironment 中
        """
        if jar_name is None:
            jar_name = 'flink-connector-kafka-3.3.0-1.20.jar'
        if clients_jar_name is None:
            clients_jar_name = 'kafka-clients-3.4.1.jar'
        kafka_connector_jar = os.path.join(jar_dir, jar_name)
        kafka_clients_jar = os.path.join(jar_dir, clients_jar_name)
        # 检查 JAR 是否存在，并获取URI
        jar_uris = []
        for jar in [kafka_connector_jar, kafka_clients_jar]:
            uri = cls._ensure_jar(jar, logger=logger, mandatory=True)
            if uri:
                jar_uris.append(uri)
        # 直接 add 到 env
        if jar_uris:
            env.add_jars(*jar_uris)
            if logger:
                logger.info(f"Added Kafka JARs to Flink env: {len(jar_uris)}")
                for uri in jar_uris:
                    logger.info(f"  - {uri}")
        return jar_uris

    @classmethod
    def add_mysql_jars(cls, env: StreamExecutionEnvironment, jar_dir, jar_name=None, logger=None):
        """
        将 MySQL 相关 JAR 直接 add 到 Flink StreamExecutionEnvironment 中
        """
        if jar_name is None:
            # 默认使用当前 jar 目录已存在的版本（本项目存在 mysql-connector-java-8.0.28.jar）
            jar_name = 'mysql-connector-java-8.0.28.jar'
        mysql_connector_jar = os.path.join(jar_dir, jar_name)
        uri = cls._ensure_jar(mysql_connector_jar, logger=logger, mandatory=False)
        if uri:
            env.add_jars(uri)
            if logger:
                logger.info(f"Added MySQL JAR to Flink env: {uri}")

    @classmethod
    def add_jars_to_env(cls, env: StreamExecutionEnvironment, jar_dir, jar_name=None, clients_jar_name=None, logger=None):
        """
        将 Kafka 相关 JAR 直接 add 到 Flink StreamExecutionEnvironment 中
        """
        cls.add_kafka_jars(env, jar_dir, jar_name, clients_jar_name, logger)
        cls.add_mysql_jars(env, jar_dir, jar_name, logger)


if __name__ == "__main__":
    # Simple smoke test (no env creation here)
    print("This module prepares and adds connector JARs. Import and use in pipeline.")

