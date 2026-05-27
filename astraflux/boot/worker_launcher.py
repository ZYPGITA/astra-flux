# -*- encoding: utf-8 -*-

import os
import sys
import time
import json
import argparse
import multiprocessing

from astraflux import AstraFlux
from astraflux.config.constants import *
from astraflux.boot.component_builder import ComponentBuilder
from astraflux.config.constructor import WorkerConstructor

from astraflux.exports import (
    redis_store_worker_data, rabbitmq_receive_message, redis_add_to_run_process,
    redis_remove_from_run_process, converted_time, redis_get_available_slots,
    mongodb_find_one_and_update_from_task, mongodb_find_from_task
)


class TaskExecutor:
    """
    Executes individual tasks in isolated worker processes.

    Handles task lifecycle including initialization, execution,
    status tracking, and cleanup.
    """

    @staticmethod
    def execute_task(task_data: dict, class_path: str, yaml_config: str, current_dir: str):
        """
        Execute a single task in an isolated worker process.

        Args:
            task_data: Dictionary containing task parameters and data
            class_path: Path to worker class definition
            yaml_config: Path to YAML configuration file
            current_dir: Current working directory
        """

        AstraFlux(yaml_path=yaml_config, current_dir=current_dir)

        worker_process_id = os.getpid()

        # ComponentBuilder worker component for task execution
        worker_builder = ComponentBuilder(
            class_path=class_path,
            component_type='worker',
            constructor=WorkerConstructor
        )
        worker_component = worker_builder.build_component(
            task_id=task_data[TASK.CONFIG.ID.value]
        )

        # Update task status to running
        TaskExecutor._update_task_status(
            worker_component, task_data, worker_process_id,
            STATUS.RUNNING.value
        )

        try:
            # Register worker as running
            redis_add_to_run_process(
                unique_id=worker_component.unique_id,
                process_id=worker_process_id
            )

            # Execute the task
            worker_component().run(task_data)

            # Mark task as successfully completed
            TaskExecutor._update_task_status(
                worker_component, task_data, worker_process_id,
                STATUS.SUCCESS.value
            )

        except Exception as execution_error:
            # Mark task as failed and record error
            TaskExecutor._update_task_status(
                worker_component, task_data, worker_process_id,
                STATUS.FAILED.value
            )
            worker_component.logger.error(
                f"Task execution failed: {execution_error}"
            )
        finally:
            # Cleanup worker registration
            redis_remove_from_run_process(
                unique_id=worker_component.unique_id,
                process_id=worker_process_id
            )

    @staticmethod
    def _update_task_status(worker_component, task_data: dict, worker_pid: int, status):
        """
        Update task status in the task tracking system.

        Args:
            worker_component: Worker component instance
            task_data: Task data dictionary
            worker_pid: Worker process ID
            status: New task status
        """

        current_time = converted_time()
        update_data = {
            BUILD.CONFIG.WORKER_PID.value: worker_pid,
            BUILD.CONFIG.WORKER_IPADDR.value: worker_component.ipaddr,
            TASK.CONFIG.STATUS.value: status,
        }

        # Add timing information based on status
        if status == STATUS.RUNNING.value:
            update_data[TASK.CONFIG.START_TIME.value] = current_time
        else:
            update_data[TASK.CONFIG.END_TIME.value] = current_time

        query = {
            TASK.CONFIG.ID.value: task_data[TASK.CONFIG.ID.value]
        }

        mongodb_find_one_and_update_from_task(
            query=query,
            data=update_data,
            upsert=False
        )


class MessageQueueHandler:
    """
    Handles RabbitMQ message processing and task distribution.

    Listens for incoming task messages, validates them, and
    dispatches to available worker processes with load balancing.
    """

    _STATUS_CACHE_TTL = 5
    _CAPACITY_SYNC_INTERVAL = 30

    def __init__(self, class_path: str, yaml_config: str, current_dir: str, logger, worker_name: str, unique_id: str):
        """
        Initialize message queue handler.

        Args:
            class_path: Path to worker class definition
            yaml_config: Path to YAML configuration file
            current_dir: Current working directory
            logger: Logger instance for message handling
            worker_name: Worker component name
            unique_id: Worker unique identifier
        """
        self.class_path = class_path
        self.yaml_config = yaml_config
        self.current_dir = current_dir
        self.logger = logger
        self.worker_name = worker_name
        self.unique_id = unique_id

        self._task_status_cache = {}
        self._task_status_cache_time = {}

        self._available_slots_cache = None
        self._available_slots_cache_time = 0
        self._AVAILABLE_SLOTS_CACHE_TTL = 1

        self._init_capacity_from_redis()

    def handle_incoming_message(self, channel, method, properties, body):
        """
        Process incoming RabbitMQ messages and dispatch tasks.

        Args:
            channel: RabbitMQ channel object
            method: Delivery method information
            properties: Message properties
            body: Message body containing task data
        """
        delivery_tag = method.delivery_tag

        try:
            task_data = json.loads(body.decode())

            # Validate task data contains required ID
            if TASK.CONFIG.ID.value not in task_data:
                self.logger.error(f'Invalid task data missing ID: {task_data}')
                # ACK invalid messages to remove them from queue (no point retrying)
                channel.basic_ack(delivery_tag=delivery_tag)
                return

            # Check if task should be executed
            if not self._should_execute_task(task_data):
                # Task should not be executed (e.g., status is STOPPED)
                # ACK to remove from queue
                channel.basic_ack(delivery_tag=delivery_tag)
                return

            # Check worker capacity
            if not self._has_available_worker_capacity():
                # This ensures message is not lost if republish fails
                self.logger.debug(f"No available capacity, requeuing task {task_data.get(TASK.CONFIG.ID.value)}")
                # Sleep briefly to prevent tight loop
                time.sleep(0.1)
                # Nack with requeue=True to put message back in queue
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return

            # Dispatch task for execution
            self._execute_task_in_isolated_process(task_data)

            # This ensures message is acknowledged only when we're certain
            # the task has been handed off to a worker process
            channel.basic_ack(delivery_tag=delivery_tag)

        except json.JSONDecodeError as json_error:
            self.logger.error(f'Failed to parse message JSON: {json_error}')
            # ACK malformed messages to remove them from queue
            channel.basic_ack(delivery_tag=delivery_tag)

        except Exception as processing_error:
            self.logger.error(f'Message processing error: {processing_error}')

            # This prevents message loss while allowing retry
            try:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            except Exception as nack_error:
                self.logger.error(f'Failed to NACK message: {nack_error}')
                # If NACK fails, try ACK to prevent message from being stuck
                try:
                    channel.basic_ack(delivery_tag=delivery_tag)
                except Exception as e:
                    self.logger.error(e)

    def _should_execute_task(self, task_data: dict) -> bool:
        """
        Determine if task should be executed based on current status.

        Args:
            task_data: Task data dictionary

        Returns:
            Boolean indicating if task should be executed
        """

        if BUILD.CONFIG.SYSTEM_SERVICE_NAME.value in self.worker_name:
            return True

        task_id = task_data.get(TASK.CONFIG.ID.value)
        if not task_id:
            return True

        cached_status = self._get_cached_task_status(task_id)
        if cached_status is not None:
            return cached_status != STATUS.STOPPED.value

        try:
            tasks = mongodb_find_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                fields={'status': 1}
            )
            if tasks and len(tasks) > 0:
                task_status = tasks[0].get('status')
                self._cache_task_status(task_id, task_status)
                return task_status != STATUS.STOPPED.value
        except Exception as e:
            self.logger.error(f'Failed to check task status: {e}')

        return True

    def _get_cached_task_status(self, task_id: str):
        """
        Get task status from local cache if not expired.

        Args:
            task_id: Task ID

        Returns:
            Task status string or None if not in cache or expired
        """
        if task_id in self._task_status_cache:
            cache_time = self._task_status_cache_time.get(task_id, 0)
            if time.time() - cache_time < self._STATUS_CACHE_TTL:
                return self._task_status_cache[task_id]
            else:
                del self._task_status_cache[task_id]
                del self._task_status_cache_time[task_id]
        return None

    def _cache_task_status(self, task_id: str, status: str):
        """
        Cache task status locally.

        Args:
            task_id: Task ID
            status: Task status string
        """
        self._task_status_cache[task_id] = status
        self._task_status_cache_time[task_id] = time.time()

    def _init_capacity_from_redis(self):
        """Initialize capacity data from Redis on startup."""
        try:
            from astraflux.exports import redis_get_max_process
            self._max_process = redis_get_max_process(unique_id=self.unique_id)
            if self._max_process is None:
                self._max_process = 10
            self.logger.debug(f"Initialized capacity: max_process={self._max_process}")
        except Exception as e:
            self.logger.warning(f"Failed to init capacity from Redis: {e}, using default max_process=10")
            self._max_process = 10

    def _get_cached_available_slots(self):
        """
        Get available slots from cache or Redis.

        Returns:
            Number of available slots
        """
        current_time = time.time()
        if current_time - self._available_slots_cache_time < self._AVAILABLE_SLOTS_CACHE_TTL:
            return self._available_slots_cache

        try:
            self._available_slots_cache = redis_get_available_slots(unique_id=self.unique_id)
            self._available_slots_cache_time = current_time
        except Exception as e:
            self.logger.warning(f"Failed to get available slots from Redis: {e}")
            self._available_slots_cache = 0

        return self._available_slots_cache

    def _has_available_worker_capacity(self) -> bool:
        """
        Check if worker has capacity to handle new tasks using cached Redis query.

        Returns:
            Boolean indicating if worker has available capacity
        """
        available_slot = self._get_cached_available_slots()
        return available_slot > 0

    def _execute_task_in_isolated_process(self, task_data: dict):
        """
        Execute task in a separate isolated process.

        Args:
            task_data: Task data dictionary
        """
        worker_process = multiprocessing.Process(
            target=TaskExecutor.execute_task,
            args=(
                task_data, self.class_path, self.yaml_config,
                self.current_dir
            )
        )
        # worker_process.daemon = True
        worker_process.start()


class WorkerComponentLauncher:
    """
    Launcher for worker components that process tasks from message queue.

    Handles worker registration, message queue listening, and
    task distribution to worker processes.
    """

    def __init__(self, class_path: str, yaml_config: str = '', current_dir: str = ''):
        """
        Initialize worker component launcher.

        Args:
            class_path: Path to worker class definition
            yaml_config: Path to YAML configuration file
            current_dir: Current working directory
        """
        self.class_path = class_path
        self.yaml_config = yaml_config
        self.current_dir = current_dir

    def launch_worker(self):
        """
        Launch and run the worker component.

        This method:
        1. Builds and registers the worker component
        2. Starts listening for messages from RabbitMQ
        3. Handles task distribution with fault tolerance
        """
        # ComponentBuilder worker component
        worker_builder = ComponentBuilder(
            class_path=self.class_path,
            component_type='worker',
            constructor=WorkerConstructor
        )
        worker_component = worker_builder.build_component()

        # Register worker in service discovery
        self._register_worker_component(worker_component)

        # Start message processing loop
        self._start_message_processing(worker_component)

    @staticmethod
    def _register_worker_component(worker_component):
        """
        Register worker component in the service discovery system.

        Args:
            worker_component: Configured worker component instance
        """
        worker_registration_data = {
            BUILD.CONFIG.UNIQUE_ID.value: worker_component.unique_id,
            BUILD.CONFIG.NAME.value: worker_component.name,
            BUILD.CONFIG.WORKER_IPADDR.value: worker_component.ipaddr,
            BUILD.CONFIG.WORKER_NAME.value: worker_component.worker_name,
            BUILD.CONFIG.WORKER_VERSION.value: worker_component.version,
            BUILD.CONFIG.WORKER_PID.value: os.getpid(),
            BUILD.CONFIG.WORKER_FUNCTIONS.value: worker_component.functions,
            BUILD.CONFIG.WORKER_MAX_PROCESS.value: 10,
            BUILD.CONFIG.WORKER_RUN_PROCESS.value: []
        }

        redis_store_worker_data(data=worker_registration_data)

        worker_component.logger.info(f'Worker component started: {worker_registration_data}')

    def _start_message_processing(self, worker_component):
        """
        Start listening for and processing messages from RabbitMQ.

        Args:
            worker_component: Worker component instance
        """
        # Initialize message queue handler
        message_handler = MessageQueueHandler(
            class_path=self.class_path,
            yaml_config=self.yaml_config,
            current_dir=self.current_dir,
            logger=worker_component.logger,
            worker_name=worker_component.worker_name,
            unique_id=worker_component.unique_id
        )

        # Main message processing loop with error handling
        while True:
            try:
                rabbitmq_receive_message(
                    queue=worker_component.worker_name,
                    callback=message_handler.handle_incoming_message
                )
            except Exception as connection_error:
                worker_component.logger.error(
                    f'Worker {worker_component.worker_name} connection error: {connection_error}'
                )
                time.sleep(0.5)  # Prevent tight loop on persistent errors


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Distributed Worker Component Launcher")

    # Define command line arguments
    parser.add_argument("--yaml_file", type=str, required=True,
                        help="Path to YAML configuration file")
    parser.add_argument("--class_path", type=str, required=True,
                        help="Path to service class definition file")
    parser.add_argument("--current_dir", type=str, required=True,
                        help="Current working directory")

    # Parse arguments
    args = parser.parse_args()
    # Add current directory to Python path for module discovery
    sys.path.append(args.current_dir)

    AstraFlux(yaml_path=args.yaml_file, current_dir=args.current_dir)

    # Launch the worker component, passing config paths for message processing
    WorkerComponentLauncher(
        class_path=args.class_path,
        yaml_config=args.yaml_file,
        current_dir=args.current_dir
    ).launch_worker()
