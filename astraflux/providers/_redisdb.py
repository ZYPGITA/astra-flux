# -*- encoding: utf-8 -*-

import time
import json
import logging
import threading
from typing import Dict, List, Optional, Any

import redis
from redis import Redis

from astraflux.core import global_manager
from astraflux.config.constants import *

DEFAULT_MAX_PROCESS = 10


class WorkerRegistry:
    """
    A thread-safe Redis client for managing worker-related data operations.
    Handles storage, retrieval, and updates of worker metadata including max process count
    and running process lists. Uses connection pooling and maintains Redis key naming conventions.
    All operations include exception handling and logging for robust error tracking.
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the WorkerRegistry with configuration and logger.

        Args:
            config: Dictionary containing Redis connection configuration
            logger: Logger instance for recording operational logs
        """
        self.logger = logger

        self._host = config[REDIS.CONFIG.HOST.value]
        self._port = config[REDIS.CONFIG.PORT.value]
        self._password = config[REDIS.CONFIG.PASSWORD.value]
        self._db = config[REDIS.CONFIG.DB_INDEX.value]
        self._max_connections = config[REDIS.CONFIG.MAX_CONNECTIONS.value]

        self._connection_pool = redis.ConnectionPool(
            host=self._host,
            port=self._port,
            password=self._password,
            db=self._db,
            max_connections=self._max_connections,
            decode_responses=False,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )

        self._lock = threading.RLock()

    def get_connection(self) -> Redis:
        """
        Retrieve a Redis connection from the connection pool.

        Returns:
            Redis client instance bound to the connection pool
        """
        return redis.Redis(connection_pool=self._connection_pool)

    @staticmethod
    def _get_worker_key(unique_id: str) -> str:
        """
        Generate Redis key for worker metadata.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Redis key string: "worker:{unique_id}"
        """
        return f"worker:{unique_id}"

    @staticmethod
    def _get_run_process_key(unique_id: str) -> str:
        """
        Generate Redis key for running process list (sorted set).

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Redis key string: "worker:{unique_id}:run_process"
        """
        return f"worker:{unique_id}:run_process"

    @staticmethod
    def _get_max_process_key(unique_id: str) -> str:
        """
        Generate Redis key for max process count.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Redis key string: "worker:{unique_id}:max_process"
        """
        return f"worker:{unique_id}:max_process"

    def store_worker_data(self, data: Dict[str, Any]) -> bool:
        """
        Store complete worker metadata in Redis with transaction consistency.
        Separates data into dedicated keys, serializes JSON fields, sets 24-hour expiration,
        and maintains service name index for worker discovery.

        Args:
            data: Worker metadata dictionary containing 'unique_id'

        Returns:
            True if storage succeeded, False otherwise
        """
        if 'unique_id' not in data:
            raise ValueError("Data must contain 'unique_id' field")

        try:
            conn = self.get_connection()
            unique_id = data['unique_id']

            with conn.pipeline(transaction=True) as pipe:

                main_data = data.copy()

                worker_run_process = main_data.pop('worker_run_process', [])

                raw_max_process = main_data.pop('worker_max_process', DEFAULT_MAX_PROCESS)
                worker_max_process = self._validate_max_process(raw_max_process)

                for key in ['service_functions', 'worker_functions']:
                    if key in main_data:
                        main_data[key] = json.dumps(main_data[key])

                main_key = self._get_worker_key(unique_id)
                if main_data:
                    hmset_args = [main_key]
                    for field, value in main_data.items():
                        hmset_args.append(field)
                        hmset_args.append(value)
                    pipe.execute_command('HMSET', *hmset_args)

                max_process_key = self._get_max_process_key(unique_id)
                pipe.set(max_process_key, str(worker_max_process))

                run_process_key = self._get_run_process_key(unique_id)
                pipe.delete(run_process_key)

                if worker_run_process:
                    current_time = time.time()
                    zadd_args = []
                    for i, process_id in enumerate(worker_run_process):
                        score = current_time + (i * 0.000001)
                        zadd_args.append(score)
                        zadd_args.append(str(process_id))
                    pipe.execute_command('ZADD', run_process_key, *zadd_args)

                expire_time = 86400
                pipe.expire(main_key, expire_time)
                pipe.expire(max_process_key, expire_time)
                pipe.expire(run_process_key, expire_time)

                if 'service_name' in data:
                    service_name = data['service_name']
                    pipe.sadd(f"index:service_name:{service_name}", unique_id)
                    pipe.expire(f"index:service_name:{service_name}", expire_time)

                pipe.execute()
            return True
        except Exception as e:
            self.logger.error(f"Error storing worker data: {e}")
            return False

    def _validate_max_process(self, value: Any) -> int:
        """
        Validate and normalize max_process value to ensure it is at least 1.

        Args:
            value: Raw max_process value

        Returns:
            Validated integer >= 1, defaults to DEFAULT_MAX_PROCESS if invalid
        """
        try:
            if value is None:
                return DEFAULT_MAX_PROCESS

            int_value = int(value)

            if int_value <= 0:
                self.logger.warning(
                    f"Invalid max_process value {int_value}, using default {DEFAULT_MAX_PROCESS}"
                )
                return DEFAULT_MAX_PROCESS

            return int_value
        except (ValueError, TypeError) as e:
            self.logger.warning(
                f"Failed to parse max_process value {value}, using default {DEFAULT_MAX_PROCESS}: {e}"
            )
            return DEFAULT_MAX_PROCESS

    def get_max_process(self, unique_id: str) -> Optional[int]:
        """
        Retrieve max process count for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Max process count as integer, or None if not found
        """
        try:
            conn = self.get_connection()
            key = self._get_max_process_key(unique_id)
            result = conn.get(key)

            if result is None:
                main_key = self._get_worker_key(unique_id)
                result = conn.hget(main_key, 'worker_max_process')
                if result:
                    conn.set(key, result)

            if result is None:
                return None

            if not isinstance(result, bytes):
                return DEFAULT_MAX_PROCESS

            max_process = int(result.decode())

            if max_process <= 0:
                self.logger.warning(
                    f"Retrieved invalid max_process {max_process} for {unique_id}, returning default"
                )
                return DEFAULT_MAX_PROCESS

            return max_process

        except Exception as e:
            self.logger.error(f"Error getting max_process: {e}")
            return None

    def update_max_process(self, unique_id: str, new_value: int) -> bool:
        """
        Update max process count for a worker with transaction consistency.

        Args:
            unique_id: Unique identifier of the worker
            new_value: New max process count

        Returns:
            True if update succeeded, False otherwise
        """
        try:
            validated_value = self._validate_max_process(new_value)

            conn = self.get_connection()

            with conn.pipeline(transaction=True) as pipe:
                max_key = self._get_max_process_key(unique_id)
                pipe.set(max_key, str(validated_value))

                main_key = self._get_worker_key(unique_id)
                pipe.hset(main_key, 'worker_max_process', str(validated_value))

                pipe.execute()

            return True

        except Exception as e:
            self.logger.error(f"Error updating max_process: {e}")
            return False

    def increment_max_process(self, unique_id: str, delta: int = 1) -> Optional[int]:
        """
        Increment max process count atomically.

        Args:
            unique_id: Unique identifier of the worker
            delta: Amount to increment by (default: 1)

        Returns:
            Updated max process count, or None if error occurs
        """
        try:
            conn = self.get_connection()
            key = self._get_max_process_key(unique_id)

            result = conn.incrby(key, delta)

            if result <= 0:
                self.logger.warning(
                    f"Increment resulted in invalid max_process {result}, resetting to default"
                )
                result = DEFAULT_MAX_PROCESS
                conn.set(key, str(result))

            main_key = self._get_worker_key(unique_id)
            conn.hset(main_key, 'worker_max_process', str(result))

            return result

        except Exception as e:
            self.logger.error(f"Error incrementing max_process: {e}")
            return None

    def get_run_process_count(self, unique_id: str) -> int:
        """
        Get number of currently running processes for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Count of running processes
        """
        try:
            conn = self.get_connection()
            key = self._get_run_process_key(unique_id)
            return conn.zcard(key)
        except Exception as e:
            self.logger.error(f"Error getting run_process count: {e}")
            return 0

    def get_all_run_process(self, unique_id: str) -> List[int]:
        """
        Get list of currently running process IDs for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            List of running process IDs
        """
        try:
            conn = self.get_connection()
            key = self._get_run_process_key(unique_id)
            members = conn.zrange(key, 0, -1)

            process_list = []
            for member in members:
                try:
                    process_list.append(int(member.decode()))
                except (ValueError, AttributeError):
                    process_list.append(member.decode())

            return process_list

        except Exception as e:
            self.logger.error(f"Error getting all run_process: {e}")
            return []

    def add_to_run_process(self, unique_id: str, process_id: int) -> bool:
        """
        Add a process ID to the running process list.

        Args:
            unique_id: Unique identifier of the worker
            process_id: Process ID to add

        Returns:
            True if process was added, False otherwise
        """
        try:
            conn = self.get_connection()
            key = self._get_run_process_key(unique_id)
            score = time.time()
            result = conn.zadd(key, {str(process_id): score})
            self._sync_run_process_to_main(unique_id)

            return result > 0

        except Exception as e:
            self.logger.error(f"Error adding to run_process: {e}")
            return False

    def batch_add_to_run_process(self, unique_id: str, process_ids: List[int]) -> int:
        """
        Add multiple process IDs to the running process list.

        Args:
            unique_id: Unique identifier of the worker
            process_ids: List of process IDs to add

        Returns:
            Number of successfully added process IDs
        """
        try:
            conn = self.get_connection()
            key = self._get_run_process_key(unique_id)

            current_time = time.time()

            zset_data = {}
            for i, pid in enumerate(process_ids):
                score = current_time + (i * 0.000001)
                zset_data[str(pid)] = score

            if zset_data:
                result = conn.zadd(key, zset_data)
                self._sync_run_process_to_main(unique_id)
                return result

            return 0

        except Exception as e:
            self.logger.error(f"Error batch adding to run_process: {e}")
            return 0

    def remove_from_run_process(self, unique_id: str, process_id: int) -> bool:
        """
        Remove a process ID from the running process list.

        Args:
            unique_id: Unique identifier of the worker
            process_id: Process ID to remove

        Returns:
            True if process was removed, False otherwise
        """
        try:
            conn = self.get_connection()
            key = self._get_run_process_key(unique_id)

            result = conn.zrem(key, str(process_id))

            if result > 0:
                self._sync_run_process_to_main(unique_id)

            return result > 0

        except Exception as e:
            self.logger.error(f"Error removing from run_process: {e}")
            return False

    def clear_run_process(self, unique_id: str) -> bool:
        """
        Clear all running process IDs for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            True if running process list was cleared, False otherwise
        """
        try:
            conn = self.get_connection()
            key = self._get_run_process_key(unique_id)

            result = conn.delete(key)

            main_key = self._get_worker_key(unique_id)
            conn.hset(main_key, 'worker_run_process', '[]')

            return result > 0

        except Exception as e:
            self.logger.error(f"Error clearing run_process: {e}")
            return False

    def _sync_run_process_to_main(self, unique_id: str) -> None:
        """
        Synchronize running process list to worker's main data key.

        Args:
            unique_id: Unique identifier of the worker
        """
        try:
            conn = self.get_connection()

            process_ids = self.get_all_run_process(unique_id)

            main_key = self._get_worker_key(unique_id)
            conn.hset(main_key, 'worker_run_process', json.dumps(process_ids))

        except Exception as e:
            self.logger.error(f"Error syncing run_process to main: {e}")

    def get_available_slots(self, unique_id: str) -> int:
        """
        Calculate available process slots for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Number of available process slots (max - running)
        """
        try:
            conn = self.get_connection()

            with conn.pipeline() as pipe:
                pipe.get(self._get_max_process_key(unique_id))
                pipe.zcard(self._get_run_process_key(unique_id))
                results = pipe.execute()

            max_process = results[0]
            run_count = results[1] or 0

            if max_process is None:
                return 0

            max_process_int = int(max_process.decode())

            if max_process_int <= 0:
                max_process_int = DEFAULT_MAX_PROCESS

            return max(max_process_int - run_count, 0)

        except Exception as e:
            self.logger.error(f"Error calculating available slots: {e}")
            return 0

    def get_total_available_slots_by_server_name(self, server_name: str) -> int:
        """
        Calculate total available slots across all workers of a service.

        Args:
            server_name: Name of the service

        Returns:
            Total available slots
        """
        workers = self.scan_workers_by_service(service_name=server_name)

        total_available_slots = 0
        for unique_id in workers:
            total_available_slots += self.get_available_slots(unique_id=unique_id)

        return total_available_slots

    def get_worker_status(self, unique_id: str) -> Dict[str, Any]:
        """
        Retrieve core status metadata for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Dictionary containing worker status information
        """
        try:
            conn = self.get_connection()

            with conn.pipeline() as pipe:
                pipe.get(self._get_max_process_key(unique_id))
                pipe.zcard(self._get_run_process_key(unique_id))
                pipe.hget(self._get_worker_key(unique_id), 'worker_name')
                pipe.hget(self._get_worker_key(unique_id), 'service_ipaddr')

                results = pipe.execute()

            max_process_raw = results[0]
            if max_process_raw is None:
                max_process = DEFAULT_MAX_PROCESS
            else:
                max_process = int(max_process_raw.decode())
                if max_process <= 0:
                    max_process = DEFAULT_MAX_PROCESS

            status = {
                'unique_id': unique_id,
                'max_process': max_process,
                'run_process_count': results[1] or 0,
                'available_slots': 0,
                'worker_name': results[2].decode() if results[2] else None,
                'service_ipaddr': results[3].decode() if results[3] else None
            }

            status['available_slots'] = max(status['max_process'] - status['run_process_count'], 0)

            return status

        except Exception as e:
            self.logger.error(f"Error getting worker status: {e}")
            return {}

    def get_full_worker_data(self, unique_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve complete metadata for a worker.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            Dictionary with complete worker metadata, or None if not found
        """
        try:
            conn = self.get_connection()

            with conn.pipeline() as pipe:
                main_key = self._get_worker_key(unique_id)
                pipe.hgetall(main_key)
                pipe.get(self._get_max_process_key(unique_id))
                pipe.zrange(self._get_run_process_key(unique_id), 0, -1)
                results = pipe.execute()

            if not results[0]:
                return None

            worker_data = {}
            for key, value in results[0].items():
                key_str = key.decode('utf-8')
                value_str = value.decode('utf-8')

                if key_str in ['service_functions', 'worker_functions']:
                    try:
                        worker_data[key_str] = json.loads(value_str)
                    except (ValueError, AttributeError):
                        worker_data[key_str] = value_str
                else:
                    worker_data[key_str] = value_str

            worker_data['unique_id'] = unique_id

            if results[1]:
                max_process = int(results[1].decode())
                if max_process <= 0:
                    max_process = DEFAULT_MAX_PROCESS
                worker_data['worker_max_process'] = max_process
            else:
                worker_data['worker_max_process'] = DEFAULT_MAX_PROCESS

            run_process_list = []
            for member in results[2]:
                try:
                    run_process_list.append(int(member.decode()))
                except (ValueError, AttributeError):
                    run_process_list.append(member.decode())

            worker_data['worker_run_process'] = run_process_list

            return worker_data

        except Exception as e:
            self.logger.error(f"Error getting full worker data: {e}")
            return None

    def delete_worker_data(self, unique_id: str) -> bool:
        """
        Delete all metadata for a worker from Redis.

        Also removes the worker from its service index set.

        Args:
            unique_id: Unique identifier of the worker

        Returns:
            True if any data was deleted, False otherwise
        """
        try:
            conn = self.get_connection()

            # Look up the service name before deleting the worker data
            main_key = self._get_worker_key(unique_id)
            service_name = conn.hget(main_key, 'service_name')
            if service_name is not None:
                service_name = service_name.decode('utf-8')

            with conn.pipeline() as pipe:
                pipe.delete(
                    self._get_worker_key(unique_id),
                    self._get_max_process_key(unique_id),
                    self._get_run_process_key(unique_id)
                )
                if service_name:
                    pipe.srem(f"index:service_name:{service_name}", unique_id)
                results = pipe.execute()

            return sum(results) > 0

        except Exception as e:
            self.logger.error(f"Error deleting worker data: {e}")
            return False

    def scan_workers_by_service(self, service_name: str) -> List[str]:
        """
        Get list of worker unique IDs for a service.

        Args:
            service_name: Name of the service

        Returns:
            List of worker unique IDs
        """
        try:
            conn = self.get_connection()
            key = f"index:service_name:{service_name}"

            return [uid.decode() for uid in conn.smembers(key)]

        except Exception as e:
            self.logger.error(f"Error scanning workers by service: {e}")
            return []

    def get_all_service_names(self) -> List[str]:
        """
        Retrieve names of all registered services.

        Returns:
            List of service name strings
        """
        try:
            conn = self.get_connection()
            cursor = 0
            service_names = []
            pattern = "index:service_name:*"
            while True:
                cursor, keys = conn.scan(cursor=cursor, match=pattern, count=100)
                for key in keys:
                    key_str = key.decode()
                    service_name = key_str.split(':', 2)[-1]
                    service_names.append(service_name)
                if cursor == 0:
                    break
            return service_names
        except Exception as e:
            self.logger.error(f"Get all Server Error: {e}")
            return []

    def refresh_service_expiry(self, server_name: str, expire_seconds: int = 86400, min_remaining_seconds: int = 0) -> bool:
        """
        Refresh expiration time for all Redis keys associated with a service.

        Conditionally skips refresh if the minimum remaining TTL across all keys
        exceeds min_remaining_seconds, to avoid unnecessary Redis writes.

        Args:
            server_name: Name of the service
            expire_seconds: New expiration time in seconds (default: 86400)
            min_remaining_seconds: Skip refresh if all keys have at least this
                                   many seconds of TTL remaining (default: 0 = always refresh)

        Returns:
            True if all keys were refreshed, False otherwise
        """
        try:
            conn = self.get_connection()
            worker_ids = self.scan_workers_by_service(server_name)
            if not worker_ids:
                self.logger.warning(f"Server '{server_name}' Not Fund worker")
                return False

            if min_remaining_seconds > 0:
                # Check TTL of all keys; if any are below threshold, proceed with refresh
                needs_refresh = False
                for unique_id in worker_ids:
                    for key in [
                        self._get_worker_key(unique_id),
                        self._get_max_process_key(unique_id),
                        self._get_run_process_key(unique_id)
                    ]:
                        ttl = conn.ttl(key)
                        if ttl < 0 or ttl < min_remaining_seconds:
                            needs_refresh = True
                            break
                    if needs_refresh:
                        break

                if not needs_refresh:
                    return True  # All keys have sufficient TTL, skip

            with conn.pipeline(transaction=False) as pipe:
                for unique_id in worker_ids:
                    main_key = self._get_worker_key(unique_id)
                    max_key = self._get_max_process_key(unique_id)
                    run_key = self._get_run_process_key(unique_id)
                    pipe.expire(main_key, expire_seconds)
                    pipe.expire(max_key, expire_seconds)
                    pipe.expire(run_key, expire_seconds)

                service_index_key = f"index:service_name:{server_name}"
                pipe.expire(service_index_key, expire_seconds)

                pipe.execute()
            return True
        except Exception as e:
            self.logger.error(f"Refresh '{server_name}' Expiry Error: {e}")
            return False


@global_manager.register_fixture(name="fixture_redis_client", scope=Scope.GLOBAL)
def _worker_registry(fixture_config, fixture_logger):
    """
    Global fixture for creating and managing a WorkerRegistry instance.

    Args:
        fixture_config: Global application configuration fixture
        fixture_logger: Global logging fixture

    Yields:
        Initialized WorkerRegistry instance
    """
    _redis_config = fixture_config[REDIS.CONFIG.KEY.value]
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, REDIS.CONFIG.KEY.value)

    _redis_producer = WorkerRegistry(
        config=_redis_config,
        logger=_logger,
    )

    yield _redis_producer
