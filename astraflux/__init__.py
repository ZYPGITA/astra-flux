# -*- coding: utf-8 -*-

import os
import importlib

from astraflux.exports import *
from astraflux.config.globals import get_current_dir
from astraflux.config.constructor import ServiceConstructor, WorkerConstructor

__all__ = [
    # Core
    'AstraFlux',
    'get_current_dir',
    'ServiceConstructor',
    'WorkerConstructor',

    # Task management
    'task_submit',
    'task_stop',
    'task_retry',
    'subtasks_create',

    # ID generation
    'snowflake_id',

    # Logging
    'logger',

    # Messaging (RabbitMQ)
    'rabbitmq_send_message',
    'rabbitmq_receive_message',

    # Utilities
    'ipaddr',
    'devices_id',
    'date_time_obj',
    'format_converted_time',
    'converted_time',

    # RPC
    'rpc_decorator',
    'proxy_call',
    'start_consumer',

    # MongoDB task operations
    'mongodb_find_one_and_update_from_task',
    'mongodb_delete_from_task',
    'mongodb_find_from_task',
    'mongodb_find_paginated_from_task',

    # Redis worker registry
    'redis_store_worker_data',
    'redis_get_max_process',
    'redis_update_max_process',
    'redis_increment_max_process',
    'redis_get_run_process_count',
    'redis_get_all_run_process',
    'redis_get_available_slots',
    'redis_get_worker_status',
    'redis_get_full_worker_data',
    'redis_scan_workers_by_service',
    'get_total_available_slots_by_server_name',
    'get_all_service_names',
    'refresh_service_expiry',

    # Scheduling
    'start_scheduler',
    'stop_scheduler',
    'add_scheduled_job',
    'remove_scheduled_job',

    # Launcher
    'launch_register',
    'launch_start',

    # Executors
    'thread_executor',
    'process_executor',

    # Config
    'config_obj',
]


class AstraFlux:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """
        The underlying layer of the intelligent architecture framework implements dependency injection,
        interface generation, function factory initialization, and runtime environment
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, yaml_path: str = None, current_dir: str = None):
        """
        :param yaml_path: yaml file path
        :param current_dir: workspace path
        """
        if self._initialized:
            if yaml_path is not None:
                raise RuntimeError(
                    "AstraFlux is already initialized. "
                    "Use AstraFlux.reconfigure(yaml_path, current_dir) to reinitialize, "
                    "or access the singleton with AstraFlux.instance()"
                )
            return

        if yaml_path is None or current_dir is None:
            raise ValueError("yaml_path and current_dir are required on first initialization")

        self.yaml_path = yaml_path
        self.current_dir = current_dir

        from .config.globals import set_current_dir, set_yaml_path

        set_current_dir(current_dir)
        set_yaml_path(yaml_path)

        from . import providers

        for _ in os.listdir(providers.__path__[0]):

            if _.startswith('__'):
                continue

            if _.startswith('_') and _.endswith('.py'):
                importlib.import_module('astraflux.providers.' + _.strip('.py'))

        self._initialized = True

    @classmethod
    def instance(cls):
        """Get the initialized singleton instance"""
        if cls._instance is None or not cls._instance._initialized:
            raise RuntimeError(
                "AstraFlux not initialized. Call AstraFlux(yaml_path, current_dir) first"
            )
        return cls._instance

    @classmethod
    def reconfigure(cls, yaml_path: str, current_dir: str):
        """Reinitialize with new configuration (clears existing state)"""
        from .core import global_manager
        # Clear all cached fixture results so they re-initialize with new config
        for fixture_def in global_manager._fixtures.values():
            fixture_def.clear_cache()

        cls._instance = None
        cls._initialized = False
        return cls(yaml_path, current_dir)
