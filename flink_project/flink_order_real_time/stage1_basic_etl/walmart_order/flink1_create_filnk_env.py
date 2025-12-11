#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 1: Create Flink execution environment.
Split from flink_walmart_order_pipeline.py. Only responsible for creating StreamExecutionEnvironment.
"""

import os
import socket
import traceback
import logging

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment

# Simple logger (file handler omitted for this split step)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FlinkEnvironmentSetup:
    """Flink execution environment setup (only creation)."""

    def __init__(self, flink_project_path, jobmanager_host='localhost', jobmanager_port=6123):
        self.flink_project_path = flink_project_path
        self.env = None
        self.jobmanager_host = jobmanager_host
        self.jobmanager_port = jobmanager_port

    def _check_flink_cluster(self):
        """Check if Flink cluster is accessible."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((self.jobmanager_host, self.jobmanager_port))
            sock.close()
            return result == 0
        except Exception as e:
            logger.warning(f"Could not check Flink cluster: {e}")
            return False

    def create_environment(self):
        """
        Create and configure Flink execution environment.
        """
        try:
            logger.info("Creating Flink execution environment...")

            flink_config = Configuration()

            if self._check_flink_cluster():
                logger.info("Flink cluster detected, using remote mode")
                flink_config.set_string("jobmanager.address", self.jobmanager_host)
                flink_config.set_string("jobmanager.rpc.port", str(self.jobmanager_port))
            else:
                # Local mode (default) when remote cluster is not accessible
                logger.info("Flink cluster not accessible, using local mode")

            logger.info("Calling StreamExecutionEnvironment.get_execution_environment()...")
            self.env = StreamExecutionEnvironment.get_execution_environment(flink_config)
            logger.info("✓ StreamExecutionEnvironment created successfully")

            return self.env

        except Exception as e:
            logger.error(f"Failed to create Flink execution environment: {e}")
            logger.error("Trying fallback without explicit config...")
            self._create_environment_fallback()
            return self.env

    def _create_environment_fallback(self):
        """Fallback method to create Flink environment without explicit config."""
        try:
            logger.info("Creating environment with fallback method (no explicit config)...")
            self.env = StreamExecutionEnvironment.get_execution_environment()
            logger.info("✓ Created environment with fallback method")
        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")
            traceback.print_exc()
            raise

    def get_environment(self):
        """Get Flink execution environment"""
        if self.env is None:
            raise RuntimeError("Flink environment not created. Call create_environment() first.")
        return self.env

    def create_and_get_environment(self):
        """
        Convenience helper: create environment then return it.
        Note: adding JARs belongs to step 2, so no classpaths are set here.
        """
        self.create_environment()
        return self.get_environment()


if __name__ == "__main__":
    # Example: set your project path here before running directly
    project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
    env_setup = FlinkEnvironmentSetup(project_path)
    env = env_setup.create_and_get_environment()
    print(env)
