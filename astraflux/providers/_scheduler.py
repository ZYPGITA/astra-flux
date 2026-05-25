# -*- encoding: utf-8 -*-

import time
import dill
import threading
import multiprocessing
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from threading import Thread, Event
from datetime import datetime, timedelta, tzinfo
from typing import Dict, List, Any, Callable, Optional, Union, Set
from urllib.parse import quote_plus

from astraflux.config.constants import *
from astraflux.exports.other import ipaddr
from astraflux.core import global_manager


class AdvancedCronScheduler:
    """
    Advanced Cron Scheduler with second-level precision and timezone support
    Supports format: second minute hour day month weekday
    """

    def __init__(self, cron_expression: str, timezone: Union[str, tzinfo] = ZoneInfo("UTC")):
        """ Initialize Cron Scheduler
        Args:
            cron_expression: Cron expression (6 parts: second minute hour day month weekday)
            timezone: Timezone, supports string or tzinfo object
        Raises:
            ValueError: When cron expression is invalid
        """
        self.cron_parts = cron_expression.strip().split()
        if len(self.cron_parts) != 6:
            raise ValueError("Invalid cron expression (requires 6 parts: second minute hour day month weekday)")

        self.timezone = _parse_timezone(timezone)

        self.second_field = self._parse_field(self.cron_parts[0], 0, 59, "second")
        self.minute_field = self._parse_field(self.cron_parts[1], 0, 59, "minute")
        self.hour_field = self._parse_field(self.cron_parts[2], 0, 23, "hour")
        self.day_field = self._parse_field(self.cron_parts[3], 1, 31, "day")
        self.month_field = self._parse_field(self.cron_parts[4], 1, 12, "month")
        self.weekday_field = self._parse_field(self.cron_parts[5], 0, 6, "weekday")

    @staticmethod
    def _parse_field(field_str: str, min_val: int, max_val: int, field_name: str) -> Set[int]:
        """
        Parse cron field and return set of valid values

        Args:
            field_str: Field string to parse
            min_val: Minimum allowed value for this field
            max_val: Maximum allowed value for this field
            field_name: Field name (for error messages)

        Returns:
            Set of valid values for this field

        Raises:
            ValueError: When field format is invalid
        """
        if field_str == "*":
            return set(range(min_val, max_val + 1))

        values = set()
        components = field_str.split(",")

        for component in components:
            component = component.strip()
            if not component:
                continue

            if "/" in component:
                range_part, step_part = component.split("/", 1)
                step = int(step_part)
                if step <= 0:
                    raise ValueError(f"{field_name} field step must be positive: {step}")

                if range_part == "*":
                    start, end = min_val, max_val
                elif "-" in range_part:
                    start, end = map(int, range_part.split("-"))
                else:
                    start = end = int(range_part)

                if not (min_val <= start <= end <= max_val):
                    raise ValueError(f"{field_name} field range invalid: {start}-{end}")

                values.update(range(start, end + 1, step))

            elif "-" in component:
                start, end = map(int, component.split("-"))
                if not (min_val <= start <= end <= max_val):
                    raise ValueError(f"{field_name} field range invalid: {start}-{end}")
                values.update(range(start, end + 1))

            else:
                value = int(component)
                if not (min_val <= value <= max_val):
                    raise ValueError(f"{field_name} field value out of range: {value}")
                values.add(value)

        return values

    def get_next_execution_time(self, from_time: Optional[datetime] = None) -> datetime | None:
        """
        Calculate the next execution time based on cron expression

        Args:
            from_time: Starting time for calculation, defaults to current time if None

        Returns:
            Next execution time (in UTC timezone)
        """
        if from_time is None:
            from_time = datetime.now(ZoneInfo("UTC"))

        # Ensure from_time has timezone
        if from_time.tzinfo is None:
            from_time = from_time.replace(tzinfo=ZoneInfo("UTC"))

        local_time = from_time.astimezone(self.timezone)

        candidate = local_time + timedelta(seconds=1)
        candidate = candidate.replace(microsecond=0)

        while True:
            if candidate.month not in self.month_field:
                candidate = self._next_month(candidate)
                continue

            day_valid = candidate.day in self.day_field
            weekday_valid = candidate.weekday() in self.weekday_field

            if not (day_valid or weekday_valid):
                candidate = self._next_day(candidate)
                continue

            if candidate.hour not in self.hour_field:
                candidate = self._next_hour(candidate)
                continue

            if candidate.minute not in self.minute_field:
                candidate = self._next_minute(candidate)
                continue

            if candidate.second not in self.second_field:
                candidate = self._next_second(candidate)
                continue

            return candidate.astimezone(ZoneInfo("UTC"))

        return None

    @staticmethod
    def _next_month(dt: datetime) -> datetime:
        """Advance to first day of next month (DST-safe via UTC conversion)"""
        utc_dt = dt.astimezone(ZoneInfo("UTC"))
        next_month = utc_dt.month + 1
        next_year = utc_dt.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        result = utc_dt.replace(
            year=next_year, month=next_month, day=1,
            hour=0, minute=0, second=0
        )
        return result.astimezone(dt.tzinfo)

    @staticmethod
    def _next_day(dt: datetime) -> datetime:
        """Advance to beginning of next day"""
        # Convert to UTC first to avoid DST transition issues with .replace()
        utc_dt = dt.astimezone(ZoneInfo("UTC"))
        next_day = utc_dt + timedelta(days=1)
        result = next_day.replace(hour=0, minute=0, second=0)
        # Convert back to original timezone
        return result.astimezone(dt.tzinfo)

    @staticmethod
    def _next_hour(dt: datetime) -> datetime:
        """Advance to beginning of next hour"""
        # Convert to UTC first to avoid DST transition issues
        utc_dt = dt.astimezone(ZoneInfo("UTC"))
        next_hour = utc_dt + timedelta(hours=1)
        result = next_hour.replace(minute=0, second=0)
        return result.astimezone(dt.tzinfo)

    @staticmethod
    def _next_minute(dt: datetime) -> datetime:
        """Advance to beginning of next minute"""
        # Convert to UTC first to avoid DST transition issues
        utc_dt = dt.astimezone(ZoneInfo("UTC"))
        next_minute = utc_dt + timedelta(minutes=1)
        result = next_minute.replace(second=0)
        return result.astimezone(dt.tzinfo)

    @staticmethod
    def _next_second(dt: datetime) -> datetime:
        """Advance to next second"""
        return dt + timedelta(seconds=1)

    def get_next_n_executions(self, n: int, from_time: Optional[datetime] = None) -> list[datetime]:
        """
        Get the next n execution times

        Args:
            n: Number of execution times to retrieve
            from_time: Starting time for calculation

        Returns:
            List of execution times in chronological order
        """
        if n <= 0:
            return []

        executions = []
        current_time = from_time

        for _ in range(n):
            next_time = self.get_next_execution_time(current_time)
            executions.append(next_time)
            current_time = next_time

        return executions

    def validate_schedule(self) -> bool:
        """
        Validate if the cron expression is valid and can produce execution times

        Returns:
            Boolean indicating if expression is valid
        """
        try:
            test_time = datetime(2020, 1, 1, tzinfo=ZoneInfo("UTC"))
            self.get_next_execution_time(test_time)
            return True
        except Exception as e:
            print(e)
            return False

    @property
    def cron_expression(self) -> str:
        """Return the cron expression as string"""
        return " ".join(self.cron_parts)

    @property
    def timezone_name(self) -> str:
        """Return timezone name"""
        return str(self.timezone)


def _parse_timezone(timezone: Union[str, tzinfo]) -> tzinfo:
    """Parse timezone configuration using zoneinfo"""
    if isinstance(timezone, str):
        return ZoneInfo(timezone)
    elif isinstance(timezone, tzinfo):
        return timezone
    else:
        raise ValueError(f"Unsupported timezone type: {type(timezone)}")


class DistributedScheduler:
    """
    Universal distributed scheduler with high precision cron scheduling and MongoDB-based coordination
    Features:
    - Second-level precision scheduling
    - Distributed locking to prevent duplicate execution
    - Support for both thread and process execution
    - IP-based job routing
    - Automatic lock refresh and cleanup
    - Generic design for any type of scheduled job
    - Three execution modes: distributed unique, IP unique, and unrestricted
    """

    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DistributedScheduler, cls).__new__(cls)
        return cls._instance

    def __init__(self, config, logger):
        if getattr(self, '_initialized', False):
            return

        self.config = config
        self.logger = logger
        self.local_ip = ipaddr()

        self._host = config[MONGODB.CONFIG.HOST.value]
        self._port = config[MONGODB.CONFIG.PORT.value]
        self._username = config[MONGODB.CONFIG.USERNAME.value]
        self._password = config[MONGODB.CONFIG.PASSWORD.value]
        self._database = config[MONGODB.CONFIG.DATABASE.value]
        self._max_connections = config[MONGODB.CONFIG.MAX_CONNECTIONS.value]

        self._max_pool_size = int(self._max_connections)

        safe_username = quote_plus(self._username)
        safe_password = quote_plus(self._password)
        self.connection_string = f"mongodb://{safe_username}:{safe_password}@{self._host}:{self._port}/"

        self._scheduler_active = Event()
        self._scheduler_thread = None

        self._lock_refresh_interval = 5
        self._lock_expire_seconds = 15
        self._active_lock_refreshers: Dict[str, Event] = {}
        self._lock_management_lock = threading.RLock()

        self._execution_stats = {
            'jobs_executed': 0,
            'jobs_failed': 0,
            'lock_acquisitions': 0,
            'lock_failures': 0,
            'jobs_skipped_due_to_mode': 0
        }
        self._init_database_connections()

        self._initialized = True

    def _init_database_connections(self) -> None:
        """Initialize MongoDB connections with error handling"""
        try:
            self._mongo_client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )

            self._mongo_client.admin.command('ismaster')

            self._database = self._mongo_client[PROJECT.NAME.value]
            self._scheduled_jobs = self._database.scheduled_jobs
            self._job_locks = self._database.job_locks

            self._scheduled_jobs.create_index("next_execution_time")
            self._scheduled_jobs.create_index("enabled")

            # Use fixed TTL value for index to avoid "cannot change TTL" errors on restart
            # Internal lock timeout is _lock_expire_seconds but TTL index uses a safe margin
            TTL_EXPIRE_SECONDS = 60
            existing_indexes = self._job_locks.index_information()
            has_ttl_index = any(
                info.get("expireAfterSeconds") is not None
                for info in existing_indexes.values()
            )
            if not has_ttl_index:
                self._job_locks.create_index(
                    "expire_at",
                    expireAfterSeconds=TTL_EXPIRE_SECONDS
                )

            self._job_locks.create_index([("job_id", 1), ("lock_type", 1)], unique=True)

        except Exception as e:
            self.logger.error(f"Failed to initialize database connections: {str(e)}")
            raise

    def start_scheduler(self) -> None:
        """
        Start the distributed job scheduler
        """
        if self._scheduler_active.is_set():
            self.logger.warning("Scheduler is already running")
            return

        # Clean up any stale/expired locks from previous runs to prevent
        # jobs from being permanently blocked due to orphaned locks
        self._cleanup_stale_locks()

        self._scheduler_active.set()
        self._scheduler_thread = Thread(
            target=self._scheduling_loop,
            name="DistributedScheduler",
            daemon=True
        )
        self._scheduler_thread.start()
        self.logger.debug("Universal scheduler started successfully")

    def stop_scheduler(self) -> None:
        """
        Stop the scheduler and cleanup resources
        """
        if not self._scheduler_active.is_set():
            self.logger.warning("Scheduler is not running")
            return

        self.logger.debug("Stopping distributed job scheduler...")
        self._scheduler_active.clear()

        # Wait for scheduler thread to finish
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=10.0)
            if self._scheduler_thread.is_alive():
                self.logger.warning("Scheduler thread did not terminate gracefully")

        # Stop all lock refresh threads
        self._stop_all_lock_refreshers()
        self.logger.debug("Distributed job scheduler stopped successfully")

    def _scheduling_loop(self) -> None:
        """
        Main scheduling loop that checks for due jobs
        """
        self.logger.debug("Scheduling loop started")

        while self._scheduler_active.is_set():
            try:
                current_time = datetime.now(ZoneInfo("UTC"))

                # Find jobs that are due for execution
                due_jobs = self._find_due_jobs(current_time)

                # Execute due jobs
                for job in due_jobs:
                    if self._scheduler_active.is_set():
                        self._execute_job_if_eligible(job)
                    else:
                        break

                # Sleep with small intervals for responsive shutdown
                for _ in range(10):
                    if not self._scheduler_active.is_set():
                        break
                    time.sleep(0.1)

            except Exception as e:
                self.logger.debug(f"Error in scheduling loop: {str(e)}")
                self._execution_stats['jobs_failed'] += 1
                time.sleep(5)

    def _find_due_jobs(self, current_time: datetime) -> List[Dict]:
        """
        Find jobs that are due for execution with efficient query
        """
        try:
            return list(self._scheduled_jobs.find({
                "enabled": True,
                "next_execution_time": {"$lte": current_time}
            }).sort("next_execution_time", 1))
        except Exception as e:
            self.logger.error(f"Failed to query due jobs: {str(e)}")
            return []

    def _cleanup_stale_locks(self) -> None:
        """
        Remove all expired job locks from the database.

        Stale locks can occur when a process crashes unexpectedly and the lock
        refresh thread dies, leaving the lock document in MongoDB. This method
        cleans up all expired locks during scheduler startup to ensure no jobs
        are permanently blocked.
        """
        try:
            now = datetime.now(ZoneInfo("UTC"))
            result = self._job_locks.delete_many({"expire_at": {"$lt": now}})
            if result.deleted_count > 0:
                self.logger.debug(f"Cleaned up {result.deleted_count} stale job locks")
        except Exception as e:
            self.logger.error(f"Failed to clean up stale locks: {str(e)}")

    def _execute_job_if_eligible(self, job: Dict) -> None:
        """
        Execute job if it meets all eligibility criteria
        """
        job_id = job["_id"]
        execution_mode = job.get("execution_mode", ExecutionMode.DISTRIBUTED_UNIQUE.value)

        # Check IP restriction
        if not self._is_job_allowed_on_current_ip(job):
            self.logger.debug(f"Job {job_id} skipped - IP restriction")
            return

        # Check execution mode and acquire appropriate lock
        if not self._check_execution_mode_and_acquire_lock(job_id, execution_mode):
            self.logger.debug(f"Job {job_id} skipped - execution mode restriction")
            self._execution_stats['jobs_skipped_due_to_mode'] += 1
            return

        try:
            # Execute the job
            self._execute_job_with_lock_protection(job, execution_mode)

        except Exception as e:
            self.logger.error(f"Job execution failed: {job_id} - {str(e)}")
            self._execution_stats['jobs_failed'] += 1
            self._cleanup_after_job_failure(job_id, execution_mode)

    def _is_job_allowed_on_current_ip(self, job: Dict) -> bool:
        """Check if job is allowed to run on current IP"""
        allowed_ips = job.get("allowed_ips", [])
        return not allowed_ips or self.local_ip in allowed_ips

    def _check_execution_mode_and_acquire_lock(self, job_id: str, execution_mode: str) -> bool:
        """
        Check execution mode and acquire appropriate lock

        Returns:
            True if job can proceed, False should be skipped
        """
        if execution_mode == ExecutionMode.UNRESTRICTED.value:
            return True

        elif execution_mode == ExecutionMode.IP_UNIQUE.value:
            lock_key = f"{job_id}_{self.local_ip}"
            return self._acquire_job_lock(lock_key, "ip_unique")

        elif execution_mode == ExecutionMode.DISTRIBUTED_UNIQUE.value:
            return self._acquire_job_lock(job_id, "distributed_unique")

        else:
            self.logger.warning(f"Unknown execution mode: {execution_mode} for job {job_id}")
            return False

    def _acquire_job_lock(self, lock_key: str, lock_type: str) -> bool:
        """
        Acquire distributed lock for job execution using find_one_and_update with upsert.

        Uses MongoDB atomic upsert to guarantee only one process acquires the lock.
        The $or condition matches either expired locks (can be taken over) or
        non-existent documents (new lock creation).

        Args:
            lock_key: The key to use for locking
            lock_type: Type of lock ("distributed_unique" or "ip_unique")

        Returns:
            True if lock was acquired, False otherwise
        """
        import socket
        import os as os_mod

        try:
            now = datetime.now(ZoneInfo("UTC"))
            lock_expiry = now + timedelta(seconds=self._lock_expire_seconds)
            owner_id = f"{self.local_ip}:{os_mod.getpid()}:{lock_type}"

            result = self._job_locks.find_one_and_update(
                {
                    "job_id": lock_key,
                    "lock_type": lock_type,
                    "$or": [
                        {"expire_at": {"$lt": now}},
                        {"expire_at": {"$exists": False}}
                    ]
                },
                {
                    "$set": {
                        "expire_at": lock_expiry,
                        "locked_at": now,
                        "locked_by": self.local_ip,
                        "lock_type": lock_type,
                        "owner_id": owner_id
                    },
                    "$setOnInsert": {
                        "created_at": now
                    }
                },
                upsert=True,
                return_document=True
            )

            # If result exists, we either created or took over the lock
            if result is not None:
                acquired = result.get("owner_id") == owner_id
            else:
                acquired = False

            if acquired:
                self._execution_stats['lock_acquisitions'] += 1
                self.logger.debug(f"Lock acquired for {lock_type}: {lock_key}")
            else:
                self._execution_stats['lock_failures'] += 1

            return acquired

        except Exception as e:
            self.logger.debug(f"Lock acquisition failed for {lock_type} {lock_key}: {str(e)}")
            self._execution_stats['lock_failures'] += 1
            return False

    def _execute_job_with_lock_protection(self, job: Dict, execution_mode: str) -> None:
        """
        Execute job with proper lock management and error handling
        """
        job_id = job["_id"]

        # Determine lock key based on execution mode
        lock_key = self._get_lock_key(job_id, execution_mode)

        # For unrestricted mode, no lock management needed
        if execution_mode == ExecutionMode.UNRESTRICTED.value:
            try:
                self._execute_job_function(job)
                self._update_job_schedule(job)
                self._execution_stats['jobs_executed'] += 1
                self.logger.debug(f"Job executed successfully (unrestricted): {job_id}")
            except Exception as e:
                self.logger.error(f"Unrestricted job execution failed {job_id}: {str(e)}")
                raise
            return

        # For locked modes, manage lock lifecycle
        try:
            self._start_lock_refresh_thread(lock_key, execution_mode)
            self._execute_job_function(job)
            self._update_job_schedule(job)
            self._execution_stats['jobs_executed'] += 1
            self.logger.debug(f"Job executed successfully: {job_id}")

        finally:
            # Always stop lock refresh and cleanup
            self._stop_lock_refresh_thread(lock_key)

    def _get_lock_key(self, job_id: str, execution_mode: str) -> str:
        """Get the appropriate lock key based on execution mode"""
        if execution_mode == ExecutionMode.IP_UNIQUE.value:
            return f"{job_id}_{self.local_ip}"
        else:
            return job_id

    def _execute_job_function(self, job: Dict) -> None:
        """Execute the actual job function"""
        try:
            function = dill.loads(job["function_data"])
            execution_type = job.get("execution_type", "thread")
            args = job.get("arguments", [])
            kwargs = job.get("keyword_arguments", {})

            if execution_type == "process":
                self._execute_in_process(function, args, kwargs, job["_id"])
            else:
                self._execute_in_thread(function, args, kwargs, job["_id"])

        except Exception as e:
            self.logger.error(f"Job function execution failed {job['_id']}: {str(e)}")
            raise

    def _execute_in_thread(self, function: Callable, args: List, kwargs: Dict, job_id: str) -> None:
        """Execute job in a separate thread — does NOT block the scheduling loop."""

        def thread_wrapper():
            try:
                function(*args, **kwargs)
            except Exception as e:
                import logging
                logging.getLogger(f"JobThread-{job_id}").error(
                    f"Thread execution failed for job {job_id}: {str(e)}"
                )

        thread = Thread(target=thread_wrapper, name=f"JobThread-{job_id}", daemon=True)
        thread.start()
        # Intentionally NOT joining — scheduling loop must not block

    def _execute_in_process(self, function: Callable, args: List, kwargs: Dict, job_id: str) -> None:
        """Execute job in a separate process — does NOT block the scheduling loop."""

        def process_wrapper():
            try:
                function(*args, **kwargs)
            except Exception as e:
                import logging
                logging.getLogger(f"JobProcess-{job_id}").error(
                    f"Process execution failed for job {job_id}: {str(e)}"
                )
                import sys
                sys.exit(1)

        process = multiprocessing.Process(
            target=process_wrapper,
            name=f"JobProcess-{job_id}",
            daemon=True
        )
        process.start()
        # Intentionally NOT joining

    def _update_job_schedule(self, job: Dict) -> None:
        """Calculate and update next execution time"""
        try:
            cron_scheduler = AdvancedCronScheduler(
                cron_expression=job["cron_expression"],
                timezone=job.get("timezone", "UTC")
            )
            next_execution = cron_scheduler.get_next_execution_time()

            self._scheduled_jobs.update_one(
                {"_id": job["_id"]},
                {
                    "$set": {
                        "next_execution_time": next_execution,
                        "last_execution_time": datetime.now(ZoneInfo("UTC")),
                        "last_execution_ip": self.local_ip
                    }
                }
            )
        except Exception as e:
            self.logger.error(f"Failed to update job schedule {job['_id']}: {str(e)}")
            raise

    def _start_lock_refresh_thread(self, lock_key: str, execution_mode: str) -> None:
        """Start background thread to refresh job lock"""
        stop_event = Event()
        refresh_thread = Thread(
            target=self._lock_refresh_worker,
            args=(lock_key, execution_mode, stop_event),
            name=f"LockRefresh-{lock_key}",
            daemon=True
        )

        with self._lock_management_lock:
            self._active_lock_refreshers[lock_key] = stop_event

        refresh_thread.start()

    def _stop_lock_refresh_thread(self, lock_key: str) -> None:
        """Stop lock refresh thread and cleanup"""
        with self._lock_management_lock:
            stop_event = self._active_lock_refreshers.pop(lock_key, None)
            if stop_event:
                stop_event.set()

    def _stop_all_lock_refreshers(self) -> None:
        """Stop all active lock refresh threads"""
        with self._lock_management_lock:
            for lock_key, stop_event in list(self._active_lock_refreshers.items()):
                stop_event.set()
            self._active_lock_refreshers.clear()

    def _lock_refresh_worker(self, lock_key: str, execution_mode: str, stop_event: Event) -> None:
        """Background worker to keep job lock alive during execution"""
        self.logger.debug(f"Lock refresh started for: {lock_key}")

        while not stop_event.is_set():
            try:
                lock_expiry = datetime.now(ZoneInfo("UTC")) + timedelta(seconds=self._lock_expire_seconds)

                result = self._job_locks.update_one(
                    {"job_id": lock_key, "lock_type": execution_mode},
                    {"$set": {"expire_at": lock_expiry}}
                )

                if result.matched_count == 0:
                    self.logger.warning(f"Lock not found during refresh for: {lock_key}")
                    break

            except Exception as e:
                self.logger.error(f"Lock refresh failed for {lock_key}: {str(e)}")
                break

            for _ in range(self._lock_refresh_interval * 2):
                if stop_event.is_set():
                    break
                time.sleep(0.5)

        self._cleanup_job_lock(lock_key, execution_mode)
        self.logger.debug(f"Lock refresh stopped for: {lock_key}")

    def _cleanup_job_lock(self, lock_key: str, execution_mode: str) -> None:
        """Remove job lock with error handling"""
        try:
            self._job_locks.delete_one({"job_id": lock_key, "lock_type": execution_mode})
        except Exception as e:
            self.logger.error(f"Failed to cleanup lock for {lock_key}: {str(e)}")

    def _cleanup_after_job_failure(self, job_id: str, execution_mode: str) -> None:
        """Cleanup resources after job failure"""
        lock_key = self._get_lock_key(job_id, execution_mode)
        self._stop_lock_refresh_thread(lock_key)
        if execution_mode != ExecutionMode.UNRESTRICTED.value:
            self._cleanup_job_lock(lock_key, execution_mode)

    def add_scheduled_job(self, job_id: str, cron_expression: str, function: Callable,
                          timezone: str = "UTC", arguments: Optional[List] = None,
                          keyword_arguments: Optional[Dict] = None, allowed_ips: Optional[List[str]] = None,
                          execution_type: str = "thread",
                          execution_mode: str = ExecutionMode.DISTRIBUTED_UNIQUE.value) -> bool:
        """
        Add a new scheduled job to the system

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
        try:
            if not self._validate_job_parameters(job_id, cron_expression, function, execution_type, execution_mode):
                return False

            cron_scheduler = AdvancedCronScheduler(cron_expression, timezone=timezone)
            next_execution = cron_scheduler.get_next_execution_time()

            job_data = {
                "_id": job_id,
                "cron_expression": cron_expression,
                "function_data": dill.dumps(function),
                "timezone": timezone,
                "arguments": arguments or [],
                "keyword_arguments": keyword_arguments or {},
                "enabled": True,
                "next_execution_time": next_execution,
                "last_execution_time": None,
                "last_execution_ip": None,
                "execution_type": execution_type,
                "execution_mode": execution_mode,
                "created_time": datetime.now(ZoneInfo("UTC"))
            }

            if allowed_ips is not None:
                job_data["allowed_ips"] = allowed_ips

            if self._scheduled_jobs.find_one({"_id": job_id}):
                self.logger.debug(f"Job '{job_id}' already exists")
                self._scheduled_jobs.update_one({"_id": job_id}, {"$set": job_data})
            else:
                self._scheduled_jobs.insert_one(job_data)

            self.logger.debug(f"Job '{job_id}' added successfully with mode: {execution_mode}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to add job '{job_id}': {str(e)}")
            return False

    def _validate_job_parameters(self, job_id: str, cron_expression: str,
                                 function: Callable, execution_type: str, execution_mode: str) -> bool:
        """Validate job parameters before adding"""
        if not job_id or not isinstance(job_id, str):
            self.logger.error("Job ID must be a non-empty string")
            return False

        if not cron_expression:
            self.logger.error("Cron expression cannot be empty")
            return False

        if not callable(function):
            self.logger.error("Function must be callable")
            return False

        if execution_type not in ['thread', 'process']:
            self.logger.error("Execution type must be 'thread' or 'process'")
            return False

        valid_modes = [mode.value for mode in ExecutionMode]
        if execution_mode not in valid_modes:
            self.logger.error(f"Execution mode must be one of: {valid_modes}")
            return False

        return True

    def remove_scheduled_job(self, job_id: str) -> bool:
        """
        Remove a scheduled job from the system

        Args:
            job_id: ID of the job to remove

        Returns:
            Boolean indicating success
        """
        try:
            result = self._scheduled_jobs.delete_one({"_id": job_id})

            if result.deleted_count > 0:
                self.logger.debug(f"Job '{job_id}' removed successfully")
                # Clean up all possible lock types for this job
                for execution_mode in [ExecutionMode.DISTRIBUTED_UNIQUE.value, ExecutionMode.IP_UNIQUE.value]:
                    lock_key = self._get_lock_key(job_id, execution_mode)
                    self._cleanup_job_lock(lock_key, execution_mode)
                return True
            else:
                self.logger.warning(f"Job '{job_id}' not found for removal")
                return False

        except Exception as e:
            self.logger.error(f"Failed to remove job '{job_id}': {str(e)}")
            return False

    def get_execution_statistics(self) -> Dict[str, Any]:
        """Get current execution statistics"""
        return self._execution_stats.copy()


@global_manager.register_fixture(name="fixture_schedule", scope=Scope.GLOBAL)
def _distributed_scheduler(fixture_config, fixture_logger):
    """
    schedule fixture
    """
    _mongodb_config = fixture_config[MONGODB.CONFIG.KEY.value]
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, 'schedule_manager')

    _universal_scheduler = DistributedScheduler(
        config=_mongodb_config, logger=_logger
    )
    yield _universal_scheduler
