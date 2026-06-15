# -*- coding: utf-8 -*-

from astraflux.core import global_manager
from astraflux.config.constants import DEFAULTS


def thread_executor(
        max_workers: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_WORKERS,
        retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY):
    """
    Get and configure a global thread pool executor instance with custom parameters.

    This function retrieves the global RetryableThreadPool fixture instance,
    updates its configuration with the specified max worker threads and retry delay,
    and returns the configured executor. It provides a convenient way to customize
    the thread pool settings at runtime while maintaining the global singleton scope.

    Args:
        max_workers: Maximum number of worker threads in the pool (default: 5)
        retry_delay: Base delay time (in seconds) between retry attempts for failed tasks (default: 1.0)

    Returns:
        RetryableThreadPool: Configured global thread pool executor instance
    """

    def _backcall(fixture_thread_executor):
        fixture_thread_executor.update(max_workers=max_workers, retry_delay=retry_delay)
        return fixture_thread_executor

    return global_manager.bind_fixture_func(_backcall)()


def process_executor(
        max_workers: int = DEFAULTS.EXECUTOR_DEFAULT_MAX_WORKERS,
        retry_delay: float = DEFAULTS.EXECUTOR_DEFAULT_RETRY_DELAY):
    """
    Get and configure a global process pool executor instance with custom parameters.

    This function retrieves the global RetryableProcessPool fixture instance,
    updates its configuration with the specified max worker processes and retry delay,
    and returns the configured executor. It maintains the global singleton scope while
    allowing runtime customization of process pool settings for CPU-bound task execution.

    Args:
        max_workers: Maximum number of worker processes in the pool (default: 5;
                     if None, uses CPU core count)
        retry_delay: Base delay time (in seconds) between retry attempts for failed tasks (default: 1.0)

    Returns:
        RetryableProcessPool: Configured global process pool executor instance
    """

    def _backcall(fixture_process_executor):
        fixture_process_executor.update(max_workers=max_workers, retry_delay=retry_delay)
        return fixture_process_executor

    return global_manager.bind_fixture_func(_backcall)()
