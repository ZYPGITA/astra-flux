# -*- coding: utf-8 -*-

from typing import TypeVar, Dict, Any

from astraflux.config.constants import *
from astraflux.exports.other import converted_time
from astraflux.exports.generate_id import snowflake_id
from astraflux.exports.redisdb import redis_scan_workers_by_service
from astraflux.exports.mongodb import mongodb_find_one_and_update_from_task

TaskData = TypeVar("TaskData", bound=Dict[str, Any])


def _ensure_task_id(task_data: TaskData) -> TaskData:
    """
    Ensure a valid task ID exists in the task data (auto-generate if missing).

    Private helper function: Checks for a non-empty task ID in the input data. If missing or empty,
    generates a globally unique ID using the snowflake algorithm.

    Args:
        task_data (TaskData): Raw task data dictionary (business-related fields).

    Returns:
        TaskData: Task data dictionary with a valid task ID (existing or auto-generated).

    Notes:
        - Preserves all original business fields in `task_data`.
        - Uses `snowflake_id()` for ID generation (guarantees global uniqueness and time-ordering).
    """
    task_id = task_data.get(TASK.CONFIG.ID.value, None)
    if task_id is not None and task_id.strip() != "":
        return task_data

    task_data[TASK.CONFIG.ID.value] = snowflake_id()
    return task_data


def _is_service_running(queue_name: str) -> bool:
    """
    Check if the service associated with the specified queue is running.

    Private helper function: Verifies service status by querying the MongoDB service collection.
    A service is considered "running" if it exists in the collection.

    Args:
        queue_name (str): Name of the target queue (maps to a specific service).

    Returns:
        bool: True if the service is running (exists in the collection), False otherwise.

    Notes:
        - Relies on the service collection being updated with active services.
        - Case-sensitive matching for `queue_name` (ensure consistency with service registration).
    """

    return True if redis_scan_workers_by_service(service_name=queue_name) else False


def _build_task_record(
        queue_name: str,
        task_data: TaskData,
        weight: int = TASK.DEFAULT.WEIGHT.value,
        source_id: str = TASK.DEFAULT.SOURCE_ID.value,
        resources: dict = TASK.DEFAULT.RESOURCES.value,
        depends_no: list[str] = TASK.DEFAULT.DEPENDS_ON.value,
) -> TaskData:
    """
    Constructs a complete task data dictionary by combining provided task data with system defaults.

    This internal function validates and enriches the provided task data with system-level metadata
    such as status, creation time, and organizational fields. It ensures the task has a valid ID
    and creates a standardized representation suitable for storage and processing.

    Args:
        queue_name: The name of the queue where the task will be submitted.
        task_data: The core task data containing business logic and configuration.
        weight: Task priority weight (higher values indicate higher priority).
               Defaults to system default weight.
        source_id: Identifier of the parent task if this is a subtask.
                  Empty string indicates no parent task. Defaults to system default.
        resources: Dictionary specifying resource requirements for task execution.
                  Includes keys: 'cpu_num', 'gpu_num', 'memory', 'gpu_memory', 'disk'.
                  All values are in megabytes except count fields. Defaults to system defaults.
        depends_no: List of task IDs that must complete before this task can execute.
                   Defaults to empty list.

    Returns:
        A complete TaskData dictionary containing both user-provided data and system metadata,
        ready for storage and processing.

    Note:
        This is an internal helper function and should not be called directly by application code.
    """

    task_data = _ensure_task_id(task_data)

    full_task_data = {
        TASK.CONFIG.ID.value: task_data[TASK.CONFIG.ID.value],
        TASK.CONFIG.QUEUE_NAME.value: queue_name,
        TASK.CONFIG.BODY.value: task_data,
        TASK.CONFIG.STATUS.value: TASK.DEFAULT.STATUS.value,
        TASK.CONFIG.WEIGHT.value: weight,
        TASK.CONFIG.SOURCE_ID.value: source_id,
        TASK.CONFIG.RESOURCES.value: resources,
        TASK.CONFIG.DEPENDS_ON.value: depends_no,
        TASK.CONFIG.CREATE_TIME.value: converted_time()
    }
    return full_task_data


def task_submit(
        queue_name: str,
        task_data: TaskData,
        weight: int = TASK.DEFAULT.WEIGHT.value,
        resources: dict = TASK.DEFAULT.RESOURCES.value,
        depends_no: list[str] = TASK.DEFAULT.DEPENDS_ON.value,
) -> str:
    """
    Submits a single task to the specified queue for execution.

    Validates queue availability, constructs complete task data with metadata,
    and persists it to the database. Returns the unique identifier of the submitted task.

    Args:
        queue_name: The name of the target queue for task submission.
        task_data: Core task data containing execution logic and parameters.
        weight: Priority weight for task scheduling (higher = higher priority).
               Defaults to system default.
                  Empty string indicates standalone task. Defaults to system default.
        resources: Resource requirements dictionary with keys:
                  'cpu_num' (CPU core count),
                  'gpu_num' (GPU device count),
                  'memory' (system memory in MB),
                  'gpu_memory' (GPU memory in MB),
                  'disk' (storage space in MB).
                  Defaults to system defaults.
        depends_no: List of prerequisite task IDs that must complete successfully
                   before this task can start execution. Defaults to empty list.

    Returns:
        The unique string identifier of the submitted task.

    Raises:
        ValueError: If the specified queue service is not currently running.
    """

    if not _is_service_running(queue_name):
        raise ValueError(f"Service for queue '{queue_name}' is not running")

    full_task_data = _build_task_record(
        queue_name=queue_name,
        task_data=task_data,
        weight=weight, resources=resources, depends_no=depends_no)

    mongodb_find_one_and_update_from_task(
        query={TASK.CONFIG.ID.value: task_data[TASK.CONFIG.ID.value]},
        data=full_task_data,
        upsert=True
    )

    return task_data[TASK.CONFIG.ID.value]


def subtasks_create(
        subtask_queue: str,
        subtask_list: list[TaskData],
        weight: int = TASK.DEFAULT.WEIGHT.value,
        source_id: str = TASK.DEFAULT.SOURCE_ID.value,
        resources: dict = TASK.DEFAULT.RESOURCES.value
) -> list[str]:
    """
    Creates and submits multiple subtasks in batch to the specified queue.

    Processes a list of subtask data, assigns them common properties (weight, parent, resources),
    and submits each to the database. All subtasks inherit the same source_id indicating
    they belong to the same parent task.

    Args:
        subtask_queue: The queue name where all subtasks will be submitted.
        subtask_list: List of dictionaries containing individual subtask data.
        weight: Priority weight applied to all subtasks in the batch.
               Defaults to system default.
        source_id: Identifier of the common parent task for all subtasks.
                  Defaults to system default (typically empty string).
        resources: Resource requirements applied uniformly to all subtasks.
                  Dictionary with keys: 'cpu_num', 'gpu_num', 'memory',
                  'gpu_memory', 'disk'. Defaults to system defaults.

    Returns:
        A list of unique string identifiers for all created subtasks, maintaining
        the same order as the input subtask_list.

    Raises:
        TypeError: If subtask_list is not a list.
        ValueError: If the specified subtask queue service is not running.
    """

    if not isinstance(subtask_list, list):
        raise TypeError(f"Expected 'subtask_list' to be List[Dict], got {type(subtask_list).__name__}")

    if not _is_service_running(subtask_queue):
        raise ValueError(f"Service for subtask queue '{subtask_queue}' is not running")

    subtask_ids = []
    for subtask_data in subtask_list:
        subtask_data = _ensure_task_id(subtask_data)

        full_task_data = _build_task_record(
            queue_name=subtask_queue,
            task_data=subtask_data,
            weight=weight, source_id=source_id, resources=resources)

        subtask_ids.append(subtask_data[TASK.CONFIG.ID.value])
        mongodb_find_one_and_update_from_task(
            query={TASK.CONFIG.ID.value: subtask_data[TASK.CONFIG.ID.value]},
            data=full_task_data,
            upsert=True
        )

    return subtask_ids


def task_stop(task_id: str) -> bool:
    """
    Stops a task by setting its status to 'stopped'.

    Only tasks in non-terminal states (pending, waiting, running, retrying) can be stopped.
    Once stopped, the scheduler will skip this task and its dependents will fail via the
    failure propagation mechanism.

    Args:
        task_id: The unique identifier of the task to stop.

    Returns:
        True if the task was successfully stopped, False if the task was not found
        or was already in a terminal state (success/failed/stopped).

    Notes:
        - This does not cancel an already-executing worker process; it only updates the
          database status. The scheduler's failure propagation handles downstream effects.
        - The worker checks the database status via `_should_execute_task()` before execution,
          so tasks picked up after being stopped will be rejected at the worker level.
    """
    current_time = converted_time()

    result = mongodb_find_one_and_update_from_task(
        query={
            TASK.CONFIG.ID.value: task_id,
            TASK.CONFIG.STATUS.value: {
                '$nin': [STATUS.SUCCESS.value, STATUS.FAILED.value, STATUS.STOPPED.value]
            }
        },
        data={
            TASK.CONFIG.STATUS.value: STATUS.STOPPED.value,
            TASK.CONFIG.END_TIME.value: current_time,
        },
        upsert=False
    )

    return result is not None


def task_retry(task_id: str) -> bool:
    """
    Retries a task by resetting its status back to 'pending'.

    Only tasks in a terminal or stopped state (failed, stopped) can be retried.
    The retry clears the error_message and end_time, resets retry_count, and sets
    the status back to pending so the scheduler picks it up again.

    Args:
        task_id: The unique identifier of the task to retry.

    Returns:
        True if the task was successfully queued for retry, False if the task
        was not found or is already in a non-retryable state (pending/waiting/running/success).

    Notes:
        - The retried task will re-enter the scheduling cycle on the next
          TaskScheduler run (default every 10 seconds).
        - All dependent task statuses must be re-evaluated manually; this function
          does NOT cascade the retry to downstream tasks.
        - If the original queue service is no longer running, the task will remain
          in pending state and fail silently in the scheduler.
    """

    result = mongodb_find_one_and_update_from_task(
        query={
            TASK.CONFIG.ID.value: task_id,
            TASK.CONFIG.STATUS.value: {
                '$in': [STATUS.FAILED.value, STATUS.STOPPED.value]
            }
        },
        data={
            TASK.CONFIG.STATUS.value: STATUS.PENDING.value,
            TASK.CONFIG.ERROR_MESSAGE.value: '',
            TASK.CONFIG.END_TIME.value: '',
        },
        upsert=False
    )

    return result is not None
