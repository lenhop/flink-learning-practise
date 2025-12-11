#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 3: Configure Flink execution parameters (class-based helper).

Usage:
    from flink3_add_parameter_to_flink import FlinkParameterConfigurator
    FlinkParameterConfigurator.configure_parallelism(env, 1, logger)
    FlinkParameterConfigurator.configure_checkpointing(env, checkpoint_dir, 60000, logger)
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.restart_strategy import RestartStrategies


class FlinkParameterConfigurator:
    """Configure Flink execution parameters."""

    @classmethod
    def configure_parallelism(cls, env: StreamExecutionEnvironment, parallelism=1, logger=None):
        """Set Flink parallelism."""
        if env is None:
            raise RuntimeError("Flink environment not created.")
        env.set_parallelism(parallelism)
        if logger:
            logger.info(f"✓ Parallelism set to {parallelism}")

    @classmethod
    def configure_checkpointing(cls, env: StreamExecutionEnvironment, checkpoint_dir: str, interval_ms=60000, logger=None):
        """Enable checkpointing and set checkpoint storage to persistent path."""
        if env is None:
            raise RuntimeError("Flink environment not created.")

        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir, exist_ok=True)
            if logger:
                logger.info(f"Created checkpoint directory: {checkpoint_dir}")

        if logger:
            logger.info("Enabling checkpointing...")
            logger.info(f"  Checkpoint directory: {checkpoint_dir}")
            logger.info(f"  Checkpoint interval: {interval_ms/1000}s")

        env.enable_checkpointing(interval_ms)

        try:
            checkpoint_config = env.get_checkpoint_config()
            checkpoint_config.set_checkpoint_storage(f"file://{checkpoint_dir}")
            if logger:
                logger.info(f"✓ Checkpoint storage configured: file://{checkpoint_dir}")
        except Exception as e:
            if logger:
                logger.warning(f"Could not configure checkpoint storage: {e}")
                logger.warning("  Checkpoints may be stored in temporary location")
                logger.warning("  Consider using Flink cluster mode for persistent checkpoints")

        if logger:
            logger.info(f"✓ Checkpointing enabled (interval: {interval_ms/1000}s)")

    @classmethod
    def configure_other_common_parameter(cls, env: StreamExecutionEnvironment, logger=None, **kwargs):
        """
        Configure additional parameters via kwargs.
        Example:
            configure_other_common_parameter(env, logger, object_reuse=True)
        """
        if env is None:
            raise RuntimeError("Flink environment not created.")

        # object reuse: reduce allocations; enable only if user functions are side-effect safe
        # 对象复用：减少对象分配；仅在用户函数无副作用时启用
        if kwargs.get("object_reuse"):
            env.get_config().enable_object_reuse()
            if logger:
                logger.info("✓ Enabled object reuse")
        elif kwargs.get("object_reuse") is False:
            env.get_config().disable_object_reuse()
            if logger:
                logger.info("✓ Disabled object reuse")

        # restart strategy: supports fixed_delay {attempts, delay_ms} or no_restart
        restart = kwargs.get("restart_strategy")
        if restart:
            try:
                if isinstance(restart, dict) and restart.get("type") == "fixed_delay":
                    attempts = restart.get("attempts", 3)
                    delay_ms = restart.get("delay_ms", 10000)
                    env.set_restart_strategy(
                        RestartStrategies.fixed_delay_restart(attempts, delay_ms)
                    )
                    if logger:
                        logger.info(f"✓ Restart strategy: fixed_delay attempts={attempts}, delay_ms={delay_ms}")
                elif restart == "no_restart":
                    env.set_restart_strategy(RestartStrategies.no_restart())
                    if logger:
                        logger.info("✓ Restart strategy: no_restart")
                else:
                    if logger:
                        logger.warning(f"Restart strategy not applied (unsupported format): {restart}")
            except Exception as e:
                if logger:
                    logger.warning(f"Could not set restart strategy: {e}")

        # Additional hints can be added here when actual config hooks are implemented


if __name__ == "__main__":
    print("This module configures Flink parameters. Import and use in pipeline.")

