# -*- coding: utf-8 -*-

import time
import dill
import queue
import threading
import traceback
import multiprocessing as mp
from dataclasses import dataclass, asdict
from multiprocessing import Queue, Manager
from typing import Callable, List, Any, Dict, Tuple

from astraflux.core import global_manager
from astraflux.config.constants import *


@dataclass
class ExecutorTask:
    """
    Data class representing a task to be executed by the thread/process pool.

    Attributes:
        id: Unique identifier for the task
        func: Callable function to be executed
        args: Positional arguments for the function
        kwargs: Keyword arguments for the function
        max_retries: Maximum number of retry attempts upon failure
        retry_delay: Base delay time (in seconds) between retries
        status: Current execution status of the task
        retry_count: Number of retry attempts already made
        result: Result returned by the function upon successful execution
        error: Exception information if the task failed
    """
    id: int
    func: Callable
    args: tuple = ()
    kwargs: dict = None
    max_retries: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_RETRIES
    retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY
    status: str = STATUS.PENDING.value
    retry_count: int = 0
    result: Any = None
    error: str = None

    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class RetryableThreadPool:
    """
    A thread pool executor with built-in retry mechanism for failed tasks.

    Features:
    - Automatic retry of failed tasks with exponential backoff
    - Progress tracking and monitoring
    - Batch task submission
    - Thread-safe operations

    Args:
        logger: Logger instance for logging operations (used only in main thread)
        max_workers: Maximum number of worker threads (default: 5)
        retry_delay: Base delay time between retries in seconds (default: 1.0)
    """

    def __init__(self, logger,
                 max_workers: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_WORKERS,
                 retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY,
                 max_queue_size: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_QUEUE_SIZE):
        self.logger = logger
        self.max_workers = max_workers
        self.retry_delay = retry_delay
        self.max_queue_size = max_queue_size

        self.task_queue = queue.Queue(maxsize=max_queue_size)
        self.results = {}
        self.failed_tasks = []
        self.completed_tasks = 0
        self.total_tasks = 0

        self.lock = threading.Lock()
        self.workers = []
        self.is_running = False

    def update(self, max_workers: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_WORKERS,
               retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY):
        self.max_workers = max_workers
        self.retry_delay = retry_delay

    def submit(self, func: Callable, *args, max_retries: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_RETRIES, **kwargs) -> int:

        with self.lock:
            task_id = self.total_tasks
            task = ExecutorTask(
                id=task_id,
                func=func,
                args=args,
                kwargs=kwargs,
                max_retries=max_retries,
                retry_delay=self.retry_delay
            )
            self.results[task_id] = task
            try:
                self.task_queue.put(task, timeout=DEFAULTS.EXECUTOR_TASK_QUEUE_PUT_TIMEOUT)
            except queue.Full:
                raise RuntimeError(
                    f"ThreadPool '{self.name if hasattr(self, 'name') else ''}' queue full "
                    f"({self.max_queue_size})"
                )
            self.total_tasks += 1
            self.results[task_id] = task
            return task_id

    def submit_batch(self, tasks: List[Tuple[Callable, tuple, dict]]) -> List[int]:
        task_ids = []
        for task_info in tasks:
            if len(task_info) == 1:
                func, args, kwargs = task_info[0], (), {}
            elif len(task_info) == 2:
                func, args, kwargs = task_info[0], task_info[1], {}
            else:
                func, args, kwargs = task_info[0], task_info[1], task_info[2]

            task_id = self.submit(func, *args, **kwargs)
            task_ids.append(task_id)
        return task_ids

    def _worker(self):
        while self.is_running:
            try:
                task = self.task_queue.get(timeout=DEFAULTS.EXECUTOR_QUEUE_GET_TIMEOUT)
            except queue.Empty:
                continue

            try:
                with self.lock:
                    task.status = STATUS.RUNNING.value

                self.logger.info(f"Executing task {task.id}: {task.func.__name__}")

                result = task.func(*task.args, **task.kwargs)

                with self.lock:
                    task.status = STATUS.SUCCESS.value
                    task.result = result
                    self.completed_tasks += 1

                self.logger.info(f"Task {task.id} completed successfully")

            except Exception as e:
                with self.lock:
                    task.retry_count += 1
                    task.error = e

                if task.retry_count <= task.max_retries:
                    with self.lock:
                        task.status = STATUS.RETRYING.value

                    self.logger.warning(
                        f"Task {task.id} failed, retry {task.retry_count}/{task.max_retries}. Error: {e}"
                    )

                    time.sleep(task.retry_delay * task.retry_count)
                    self.task_queue.put(task)
                else:
                    with self.lock:
                        task.status = STATUS.FAILED.value
                        self.failed_tasks.append(task)
                        self.completed_tasks += 1

                    self.logger.error(f"Task {task.id} failed after {task.max_retries} retry attempts")

            finally:
                self.task_queue.task_done()

    def start(self):
        if self.is_running:
            self.logger.warning("Executor is already running")
            return

        self.is_running = True
        self.workers = []

        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker,
                name=f"Worker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)

        self.logger.info(f"Started {self.max_workers} worker threads")

    def wait_completion(self, timeout: float = None):
        if not self.is_running:
            if self.logger:
                self.logger.warning("Executor is not running, nothing to wait for")
            return

        self.task_queue.join()
        self.is_running = False

        start_time = time.time()
        for worker in self.workers:
            if timeout:
                remaining = timeout - (time.time() - start_time)
                if remaining <= 0:
                    break
                worker.join(remaining)
            else:
                worker.join()

    def shutdown(self):
        self.is_running = False

        # Drain the task queue so worker threads can exit cleanly
        # (task_done() must still be called for each remaining item to unblock join())
        while True:
            try:
                self.task_queue.get_nowait()
                self.task_queue.task_done()
            except queue.Empty:
                break

        self.logger.info("Executor has been shutdown")

    def get_progress(self) -> Dict[str, Any]:
        with self.lock:
            pending = sum(1 for task in self.results.values() if task.status == STATUS.PENDING.value)
            running = sum(1 for task in self.results.values() if task.status == STATUS.RUNNING.value)
            retrying = sum(1 for task in self.results.values() if task.status == STATUS.RETRYING.value)
            success = sum(1 for task in self.results.values() if task.status == STATUS.SUCCESS.value)
            failed = sum(1 for task in self.results.values() if task.status == STATUS.FAILED.value)

            progress_percentage = (
                (self.completed_tasks / self.total_tasks * 100)
                if self.total_tasks > 0 else 0
            )

            return {
                "total": self.total_tasks,
                "completed": self.completed_tasks,
                "pending": pending,
                "running": running,
                "retrying": retrying,
                "success": success,
                "failed": failed,
                "progress": progress_percentage
            }

    def get_results(self) -> Dict[int, Any]:
        return {
            task_id: task.result
            for task_id, task in self.results.items()
            if task.status == STATUS.SUCCESS.value
        }

    def get_failed_tasks(self) -> List[ExecutorTask]:
        return self.failed_tasks.copy()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


def _worker_process(
        worker_id: int,
        task_queue: Queue,
        results: Dict,
        task_status: Dict,
        failed_tasks: List,
        completed_tasks: mp.Value,
        total_tasks: mp.Value,
        is_running: mp.Value,
        retry_delay: float,
        logger=None
):
    """Worker process function"""
    if logger:
        logger.info(f"Worker process {worker_id} started")

    while is_running.value:
        try:
            task_dict = task_queue.get(timeout=DEFAULTS.EXECUTOR_QUEUE_GET_TIMEOUT)
        except queue.Empty:
            continue

        try:
            task_dict = task_dict.copy()
            task_dict['func'] = dill.loads(task_dict['func'])
            task_dict['args'] = dill.loads(task_dict['args'])
            task_dict['kwargs'] = dill.loads(task_dict['kwargs'])
            task = ExecutorTask(**task_dict)
        except Exception as e:
            if logger:
                logger.error(f"Worker {worker_id} failed to deserialize task: {e}")
            with completed_tasks.get_lock():
                completed_tasks.value += 1
            continue

        task.status = STATUS.RUNNING.value
        results[task.id] = _serialize_task(task)
        task_status[task.id] = task.status

        if logger:
            logger.info(f"Process {worker_id} executing task {task.id}: {task.func.__name__}")

        try:
            result = task.func(*task.args, **task.kwargs)
            task.status = STATUS.SUCCESS.value
            task.result = result
            with completed_tasks.get_lock():
                completed_tasks.value += 1
            if logger:
                logger.info(f"Process {worker_id} completed task {task.id}")

        except Exception as e:
            task.retry_count += 1
            task.error = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

            if task.retry_count <= task.max_retries:
                task.status = STATUS.RETRYING.value
                if logger:
                    logger.warning(
                        f"Process {worker_id} task {task.id} failed, "
                        f"retry {task.retry_count}/{task.max_retries}. Error: {e}"
                    )
                time.sleep(task.retry_delay * task.retry_count)
                task_queue.put(_serialize_task(task))
            else:
                task.status = STATUS.FAILED.value
                failed_tasks.append(_serialize_task(task))
                with completed_tasks.get_lock():
                    completed_tasks.value += 1
                if logger:
                    logger.error(f"Process {worker_id} task {task.id} failed after maximum retries")

        finally:
            results[task.id] = _serialize_task(task)
            task_status[task.id] = task.status

    if logger:
        logger.info(f"Worker process {worker_id} exited")


def _serialize_task(task: ExecutorTask) -> Dict[str, Any]:
    """Serialize task for inter-process communication."""
    task_dict = asdict(task)
    task_dict['func'] = dill.dumps(task.func)
    task_dict['args'] = dill.dumps(task.args)
    task_dict['kwargs'] = dill.dumps(task.kwargs)
    return task_dict


class RetryableProcessPool:
    """
    A process pool executor with built-in retry mechanism for failed tasks.

    Features:
    - Multiprocessing-based execution for CPU-bound tasks
    - Automatic retry with configurable delay
    - Progress tracking across processes
    - Batch task submission
    - Process-safe operations using shared memory

    Args:
        logger: Logger instance for logging operations (used only in main process)
        max_workers: Maximum number of worker processes (default: CPU count)
        retry_delay: Base delay time between retries in seconds (default: 1.0)
    """

    def __init__(self, logger=None, max_workers: int = None,
                 retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY):
        self.logger = logger
        self.max_workers = max_workers or mp.cpu_count()
        self.retry_delay = retry_delay

        # Defer set_start_method to _start_pool to avoid import-time side effects:
        # only set on Windows, and only if not already set
        self._start_method_set = False

        self.manager = Manager()
        self.task_queue = Queue()
        self.results = self.manager.dict()
        self.task_status = self.manager.dict()
        self.failed_tasks = self.manager.list()

        self.completed_tasks = mp.Value('i', 0)
        self.total_tasks = mp.Value('i', 0)
        self.is_running = mp.Value('b', False)

        self.workers = []
        self._task_counter = 0
        self._submit_lock = threading.Lock()

    def _ensure_start_method(self):
        """Set multiprocessing start method to 'spawn' for cross-platform safety.

        On Linux the default start method is 'fork', which inherits the parent
        process's entire memory (including thread locks, open file handles,
        singleton class-level state like AstraFlux._initialized).  Using 'spawn'
        avoids all of these problems and makes behaviour identical across
        Linux, macOS and Windows.

        Note: set_start_method can only be called once per Python process, so
        we catch RuntimeError and continue if it was already set elsewhere.
        """
        if not self._start_method_set:
            try:
                mp.set_start_method('spawn')
            except RuntimeError:
                pass  # Already set
            self._start_method_set = True

    def update(self, max_workers: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_WORKERS,
               retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY):
        self.max_workers = max_workers
        self.retry_delay = retry_delay

    def submit(self, func: Callable, *args, max_retries: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_RETRIES, **kwargs) -> int:
        with self._submit_lock:
            task_id = self._task_counter
            task = ExecutorTask(
                id=task_id,
                func=func,
                args=args,
                kwargs=kwargs,
                max_retries=max_retries,
                retry_delay=self.retry_delay
            )
            task_dict = _serialize_task(task)
            self.results[task_id] = task_dict
            self.task_status[task_id] = task.status
            self.task_queue.put(task_dict)
            self._task_counter += 1
            self.total_tasks.value += 1
            return task_id

    def submit_batch(self, tasks: List[Tuple[Callable, tuple, dict]]) -> List[int]:
        task_ids = []
        for task_info in tasks:
            if len(task_info) == 1:
                func, args, kwargs = task_info[0], (), {}
            elif len(task_info) == 2:
                func, args, kwargs = task_info[0], task_info[1], {}
            else:
                func, args, kwargs = task_info[0], task_info[1], task_info[2]

            task_id = self.submit(func, *args, **kwargs)
            task_ids.append(task_id)
        return task_ids

    def start(self):
        if self.is_running.value:
            if self.logger:
                self.logger.warning("Executor is already running")
            return

        self._ensure_start_method()
        self.is_running.value = True
        self.workers = []

        for i in range(self.max_workers):
            worker = mp.Process(
                target=_worker_process,
                args=(
                    i,
                    self.task_queue,
                    self.results,
                    self.task_status,
                    self.failed_tasks,
                    self.completed_tasks,
                    self.total_tasks,
                    self.is_running,
                    self.retry_delay,
                    None  # logger passed as None to avoid pickling issues
                ),
                name=f"WorkerProcess-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)

        if self.logger:
            self.logger.debug(f"Started {self.max_workers} worker processes")

    def wait_completion(self, timeout: float = None):
        start_time = time.time()
        while self.completed_tasks.value < self.total_tasks.value:
            if timeout and (time.time() - start_time) > timeout:
                if self.logger:
                    self.logger.warning("Timeout waiting for task completion")
                break
            time.sleep(DEFAULTS.EXECUTOR_LOOP_YIELD_INTERVAL)

        self.is_running.value = False

        for worker in self.workers:
            if timeout:
                remaining = timeout - (time.time() - start_time)
                if remaining <= 0:
                    break
                worker.join(remaining)
            else:
                worker.join()

    def shutdown(self, timeout: float = DEFAULTS.EXECUTOR_SHUTDOWN_TIMEOUT):
        self.is_running.value = False

        start_time = time.time()
        for worker in self.workers:
            remaining = timeout - (time.time() - start_time)
            if remaining > 0:
                worker.join(timeout=remaining)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=DEFAULTS.EXECUTOR_SIGTERM_JOIN_TIMEOUT)

        # Only shut down the manager AFTER all workers have exited,
        # otherwise workers still accessing shared memory (results, task_status, etc.)
        # will trigger BrokenPipeError / ConnectionRefusedError
        try:
            self.manager.shutdown()
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error shutting down process pool manager: {e}")

        if self.logger:
            self.logger.info("Executor has been shutdown")

    def get_progress(self) -> Dict[str, Any]:
        total = self.total_tasks.value
        completed = self.completed_tasks.value

        # Count statuses from task_status dict (faster than deserializing)
        status_count = {
            STATUS.PENDING.value: 0,
            STATUS.RUNNING.value: 0,
            STATUS.RETRYING.value: 0,
            STATUS.SUCCESS.value: 0,
            STATUS.FAILED.value: 0
        }

        # Convert manager dict values to list to avoid modification during iteration
        for status in list(self.task_status.values()):
            status_count[status] += 1

        progress_percentage = (completed / total * 100) if total > 0 else 0

        return {
            "total": total,
            "completed": completed,
            "pending": status_count[STATUS.PENDING.value],
            "running": status_count[STATUS.RUNNING.value],
            "retrying": status_count[STATUS.RETRYING.value],
            "success": status_count[STATUS.SUCCESS.value],
            "failed": status_count[STATUS.FAILED.value],
            "progress": progress_percentage
        }

    def get_results(self) -> Dict[int, Any]:
        results = {}
        for task_id in list(self.results.keys()):
            try:
                task_dict = self.results[task_id]
                task = ExecutorTask(
                    id=task_dict['id'],
                    func=dill.loads(task_dict['func']),
                    args=dill.loads(task_dict['args']),
                    kwargs=dill.loads(task_dict['kwargs']),
                    max_retries=task_dict['max_retries'],
                    retry_delay=task_dict['retry_delay'],
                    status=task_dict['status'],
                    retry_count=task_dict['retry_count'],
                    result=task_dict['result'],
                    error=task_dict['error']
                )
                if task.status == STATUS.SUCCESS.value:
                    results[task_id] = task.result
            except Exception as e:
                if self.logger:
                    self.logger.error(e)
                continue
        return results

    def get_failed_tasks(self) -> List[Dict[str, Any]]:
        failed = []
        for task_dict in self.failed_tasks:
            try:
                task = ExecutorTask(
                    id=task_dict['id'],
                    func=dill.loads(task_dict['func']),
                    args=dill.loads(task_dict['args']),
                    kwargs=dill.loads(task_dict['kwargs']),
                    max_retries=task_dict['max_retries'],
                    retry_delay=task_dict['retry_delay'],
                    status=task_dict['status'],
                    retry_count=task_dict['retry_count'],
                    result=task_dict['result'],
                    error=task_dict['error']
                )
                failed.append({
                    'id': task.id,
                    'func': task.func.__name__,
                    'error': task.error,
                    'retry_count': task.retry_count
                })
            except Exception as e:
                if self.logger:
                    self.logger.error(e)
                continue
        return failed

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


@global_manager.register_fixture(name="fixture_thread_executor", scope=Scope.GLOBAL)
def _retryable_thread_pool(fixture_logger):
    """
    Fixture function for creating a global ThreadPoolExecutorWithRetry instance.

    This fixture is registered with global scope, meaning it will create a single
    thread pool executor instance that can be reused across the entire application
    lifecycle. The executor is configured with a dedicated logger for thread execution
    operations and uses default configuration parameters unless overridden.

    Args:
        fixture_logger: Logger factory instance for creating named loggers

    Yields:
        ThreadPoolExecutorWithRetry: A thread pool executor instance with built-in
        retry mechanism for failed tasks
    """
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, 'thread_executor')

    yield RetryableThreadPool(logger=_logger)


@global_manager.register_fixture(name="fixture_process_executor", scope=Scope.GLOBAL)
def _retryable_process_pool(fixture_logger):
    """
    Fixture function for creating a global ProcessPoolExecutorWithRetry instance.

    This fixture is registered with global scope, providing a singleton process pool
    executor instance for the entire application. It's optimized for CPU-bound tasks
    and uses inter-process communication with proper serialization/deserialization.
    The executor is equipped with a dedicated logger for process execution monitoring.

    Args:
        fixture_logger: Logger factory instance for creating named loggers

    Yields:
        ProcessPoolExecutorWithRetry: A process pool executor instance with built-in
        retry mechanism for failed tasks, optimized for CPU-bound operations
    """
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, 'process_executor')

    yield RetryableProcessPool(logger=_logger)
