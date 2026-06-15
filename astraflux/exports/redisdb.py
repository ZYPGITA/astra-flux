# -*- coding: utf-8 -*-

from astraflux.core import global_manager
from astraflux.config.constants import DEFAULTS


def redis_store_worker_data(data: dict):
    """
    Wrapper function to store the complete metadata of a worker into Redis by binding to the global Redis fixture.
    This function encapsulates the logic of acquiring the WorkerRegistry instance through global_manager
    and invoking its store_worker_data method to persist worker data with transactional consistency.

    Args:
        data: A dictionary containing the full metadata of the worker, which must include the 'unique_id' field
              as the unique identifier of the worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the worker data storage operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.store_worker_data(data=data)

    return global_manager.bind_fixture_func(_backcall)()


def redis_get_max_process(unique_id: str):
    """
    Wrapper function to retrieve the maximum allowed process count of a specific worker from Redis.
    It acquires the WorkerRegistry instance via the global fixture manager and calls the get_max_process method,
    which prioritizes fetching from the dedicated max process key and falls back to the worker's main data key if needed.

    Args:
        unique_id: The unique string identifier of the target worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the max process query operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_max_process(unique_id=unique_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_update_max_process(unique_id: str, new_value: int):
    """
    Wrapper function to update the maximum allowed process count of a specific worker in Redis.
    It obtains the WorkerRegistry instance through the global fixture system and invokes the update_max_process method,
    which synchronizes the new max process value to both the dedicated key and the worker's main data key atomically.

    Args:
        unique_id: The unique string identifier of the target worker
        new_value: The new integer value of the maximum allowed processes for the worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the max process update operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.update_max_process(unique_id=unique_id, new_value=new_value)

    return global_manager.bind_fixture_func(_backcall)()


def redis_increment_max_process(unique_id: str, delta: int):
    """
    Wrapper function to increment the maximum allowed process count of a specific worker by a given delta.
    It acquires the WorkerRegistry instance via the global fixture manager and calls the increment_max_process method,
    which uses Redis's atomic INCRBY operation to avoid race conditions during concurrent updates.

    Args:
        unique_id: The unique string identifier of the target worker
        delta: The integer value to increment the worker's maximum process count by
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the max process increment operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.increment_max_process(unique_id=unique_id, delta=delta)

    return global_manager.bind_fixture_func(_backcall)()


def redis_get_run_process_count(unique_id: str):
    """
    Wrapper function to retrieve the count of currently running processes for a specific worker from Redis.
    It obtains the WorkerRegistry instance through the global fixture system and invokes the get_run_process_count method,
    which counts the members in the worker's running process sorted set using Redis ZCARD operation.

    Args:
        unique_id: The unique string identifier of the target worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the running process count query
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_run_process_count(unique_id=unique_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_add_to_run_process(unique_id: str, process_id: int):
    """
    Wrapper function to add a single process ID to the running process list of a specific worker in Redis.
    It acquires the WorkerRegistry instance via the global fixture manager and calls the add_to_run_process method,
    which inserts the process ID into the worker's running process sorted set with a timestamp-based score.

    Args:
        unique_id: The unique string identifier of the target worker
        process_id: The integer ID of the process to be added to the worker's running process list
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the process ID addition operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.add_to_run_process(unique_id=unique_id, process_id=process_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_remove_from_run_process(unique_id: str, process_id: int):
    """
    Wrapper function to remove a single process ID from the running process list of a specific worker in Redis.
    It obtains the WorkerRegistry instance through the global fixture system and invokes the remove_from_run_process method,
    which deletes the process ID from the worker's running process sorted set using Redis ZREM operation.

    Args:
        unique_id: The unique string identifier of the target worker
        process_id: The integer ID of the process to be removed from the worker's running process list
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the process ID removal operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.remove_from_run_process(unique_id=unique_id, process_id=process_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_get_all_run_process(unique_id: str):
    """
    Wrapper function to retrieve the complete list of currently running process IDs for a specific worker from Redis.
    It acquires the WorkerRegistry instance via the global fixture manager and calls the get_all_run_process method,
    which fetches all members from the worker's running process sorted set and converts them to integer IDs.

    Args:
        unique_id: The unique string identifier of the target worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the full running process list query
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_all_run_process(unique_id=unique_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_get_available_slots(unique_id: str):
    """
    Wrapper function to calculate the number of available process slots for a specific worker in Redis.
    It obtains the WorkerRegistry instance through the global fixture system and invokes the get_available_slots method,
    which computes the difference between max processes and running processes (clamped to non-negative value) atomically.

    Args:
        unique_id: The unique string identifier of the target worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the available slots calculation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_available_slots(unique_id=unique_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_get_worker_status(unique_id: str):
    """
    Wrapper function to retrieve the core status metadata of a specific worker from Redis.
    It acquires the WorkerRegistry instance via the global fixture manager and calls the get_worker_status method,
    which fetches key metrics (max processes, running processes) and identifiers (worker name, service IP) atomically.

    Args:
        unique_id: The unique string identifier of the target worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the worker status query
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_worker_status(unique_id=unique_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_get_full_worker_data(unique_id: str):
    """
    Wrapper function to retrieve the complete metadata of a specific worker from Redis.
    It obtains the WorkerRegistry instance through the global fixture system and invokes the get_full_worker_data method,
    which combines main worker data, max process count and running process list into a structured dictionary.

    Args:
        unique_id: The unique string identifier of the target worker
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the full worker data query
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_full_worker_data(unique_id=unique_id)

    return global_manager.bind_fixture_func(_backcall)()


def redis_scan_workers_by_service(service_name: str):
    """
    Wrapper function to retrieve the list of worker unique IDs associated with a specific service name from Redis.
    It acquires the WorkerRegistry instance via the global fixture manager and calls the scan_workers_by_service method,
    which fetches all members from the service name index set and returns them as decoded string IDs.

    Args:
        service_name: The string name of the service to filter the worker list by
    Returns:
        The result returned by the global_manager.bind_fixture_func method after executing the service worker scan operation
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.scan_workers_by_service(service_name=service_name)

    return global_manager.bind_fixture_func(_backcall)()


def get_total_available_slots_by_server_name(server_name: str):
    """
    Get the total number of available slots for a specific service name from Redis.
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_total_available_slots_by_server_name(server_name=server_name)

    return global_manager.bind_fixture_func(_backcall)()


def get_all_service_names():
    """
    Retrieve the names of all registered services.

    Uses Redis SCAN command to iteratively find all keys matching the pattern
    "index:service_name:*", extracts the service name from each key, and returns
    a list of unique service names. This method is efficient for large key spaces
    as it uses cursor-based iteration without blocking the server.

    Returns:
        A list of service name strings. If an error occurs or no services are
        found, an empty list is returned.
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.get_all_service_names()

    return global_manager.bind_fixture_func(_backcall)()


def refresh_service_expiry(
        server_name: str,
        expire_seconds: int = DEFAULTS.REDIS_DEFAULT_EXPIRE_SECONDS,
        min_remaining_seconds: int = 0):
    """
    Refresh the expiration time for all Redis keys associated with a specific service.

    For the given service, this method renews the TTL (time-to-live) on:
        - Each worker's main data key
        - Each worker's max process key
        - Each worker's running process key
        - The service index key (used for service discovery)

    The operation is performed atomically using a Redis pipeline to ensure
    all keys are updated together efficiently.

    If min_remaining_seconds > 0, the refresh is skipped when all keys already
    have at least that many seconds of TTL remaining, reducing unnecessary writes.

    Args:
        server_name: The name of the service whose keys should be refreshed.
        expire_seconds: New expiration time in seconds (default is 86400 seconds = 24 hours).
        min_remaining_seconds: Minimum remaining TTL threshold; skip refresh if
                              all keys exceed this (default: 0 = always refresh).

    Returns:
        True if all keys were successfully refreshed; False if no workers were
        found for the service or if an error occurred during the operation.
    """

    def _backcall(fixture_redis_client):
        return fixture_redis_client.refresh_service_expiry(
            server_name=server_name,
            expire_seconds=expire_seconds,
            min_remaining_seconds=min_remaining_seconds
        )

    return global_manager.bind_fixture_func(_backcall)()
