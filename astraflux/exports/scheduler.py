# -*- coding: utf-8 -*-
from typing import Callable, Optional, List, Dict

from astraflux.core import global_manager
from astraflux.config.constants import *


def add_scheduled_job(
        job_id: str,
        cron_expression: str,
        function: Callable,
        timezone: str = "UTC",
        arguments: Optional[List] = None,
        keyword_arguments: Optional[Dict] = None,
        allowed_ips: Optional[List[str]] = None,
        execution_type: str = "thread",
        execution_mode: str = ExecutionMode.DISTRIBUTED_UNIQUE.value) -> bool:
    """
    Schedule a job in the distributed scheduler

    Args:
        job_id: Unique identifier for the job
        cron_expression: Cron expression for scheduling
        function: Function to be executed
        timezone: Timezone for schedule calculation
        arguments: Positional arguments for the function
        keyword_arguments: Keyword arguments for the function
        allowed_ips: List of IP addresses allowed to execute this job
        execution_type: 'thread' or 'process'
        execution_mode: 'distributed_unique', 'ip_unique', or 'unrestricted'

    Returns:
        Boolean indicating success
    """

    def _backcall(fixture_schedule):
        return fixture_schedule.add_scheduled_job(
            job_id=job_id,
            cron_expression=cron_expression,
            function=function,
            timezone=timezone,
            arguments=arguments,
            keyword_arguments=keyword_arguments,
            allowed_ips=allowed_ips,
            execution_type=execution_type,
            execution_mode=execution_mode
        )

    return global_manager.bind_fixture_func(_backcall)()


def remove_scheduled_job(job_id: str) -> bool:
    """
    Remove a scheduled job from the distributed scheduler

    Args:
        job_id: ID of the job to remove

    Returns:
        Boolean indicating success
    """

    def _backcall(fixture_schedule):
        return fixture_schedule.remove_scheduled_job(job_id=job_id)

    return global_manager.bind_fixture_func(_backcall)()


def start_scheduler() -> None:
    """Start the distributed scheduler"""

    def _backcall(fixture_schedule):
        return fixture_schedule.start_scheduler()

    return global_manager.bind_fixture_func(_backcall)()


def stop_scheduler() -> None:
    """Stop the distributed scheduler"""

    def _backcall(fixture_schedule):
        return fixture_schedule.stop_scheduler()

    return global_manager.bind_fixture_func(_backcall)()
