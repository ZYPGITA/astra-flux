# -*- coding: utf-8 -*-

from enum import Enum, unique


@unique
class STATUS(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    STOPPED = "stopped"
    WAITING = "waiting"


@unique
class PROJECT(Enum):
    NAME = 'astraflux'
    CURRENT_DIR = 'current_dir'
    CONFIG_PATH = 'config_path'


class ExecutionMode(Enum):
    DISTRIBUTED_UNIQUE = "distributed_unique"
    IP_UNIQUE = "ip_unique"
    UNRESTRICTED = "unrestricted"


@unique
class Scope(Enum):
    SINGLETON = "singleton"
    GLOBAL = "global"
    THREAD = "thread"


class MONGODB:
    @unique
    class CONFIG(Enum):
        KEY = 'mongodb'
        HOST = 'host'
        PORT = 'port'
        DATABASE = 'database'
        USERNAME = 'username'
        PASSWORD = 'password'
        MAX_CONNECTIONS = 'max_connections'

    @unique
    class DEFAULT(Enum):
        HOST = '127.0.0.1'
        PORT = 27017
        DATABASE = 'astraflux'
        USERNAME = 'scheduleAdmin'
        PASSWORD = 'scheduleAdminPassword'
        MAX_CONNECTIONS = 20


class REDIS:
    @unique
    class CONFIG(Enum):
        KEY = 'redis'
        HOST = 'host'
        PORT = 'port'
        PASSWORD = 'password'
        DB_INDEX = 'db_index'
        MAX_CONNECTIONS = 'max_connections'

    @unique
    class DEFAULT(Enum):
        HOST = '127.0.0.1'
        PORT = 6379
        PASSWORD = 'scheduleAdminPassword'
        DB_INDEX = 8
        MAX_CONNECTIONS = 20


class RABBITMQ:
    @unique
    class CONFIG(Enum):
        KEY = 'rabbitmq'
        HOST = 'host'
        PORT = 'port'
        USERNAME = 'username'
        PASSWORD = 'password'

    @unique
    class DEFAULT(Enum):
        HOST = '127.0.0.1'
        PORT = 5672
        USERNAME = 'scheduleAdmin'
        PASSWORD = 'scheduleAdminPassword'


class LOGGER:
    @unique
    class CONFIG(Enum):
        KEY = 'logger'
        PATH = 'path'
        LEVEL = 'level'

    @unique
    class DEFAULT(Enum):
        PATH = 'logs'
        LEVEL = 'INFO'
        SUFFIX = "%Y-%m-%d.log"
        FMT = '%(asctime)s - %(levelname)s - [%(threadName)s] - [%(filename)s:%(lineno)d] %(message)s'


class SOCKET:
    @unique
    class DEFAULT(Enum):
        BIND_IP = '10.255.255.255'
        BIND_PORT = 80


class TIME:
    @unique
    class DEFAULT(Enum):
        TIME_FMT = '%Y-%m-%d %H:%M:%S'
        TIMEZONE = 'Asia/Shanghai'


class RPC:
    class DEFAULT(Enum):
        RPC_CALL_TIMEOUT = 30
        MAX_RETRIES = 3
        RETRY_BASE_DELAY = 1.0
        RETRY_MAX_DELAY = 30.0
        CIRCUIT_BREAKER_THRESHOLD = 5
        CIRCUIT_BREAKER_RECOVERY = 30
        PROXY = 'proxy'
        FUNCTION_SELF = 'self'
        FUNCTION_RPC = 'RpcFunction'
        FUNCTION_WORKER = 'WorkerFunction'
        FUNCTION_PARAM_NAME = 'param_name'
        FUNCTION_PARAM_DEFAULT_VALUE = 'default_value'

    @unique
    class CONFIG(Enum):
        KEY = 'rpc'
        RPC_CALL_TIMEOUT = 'call_timeout'
        MAX_RETRIES = 'max_retries'
        RETRY_BASE_DELAY = 'retry_base_delay'
        RETRY_MAX_DELAY = 'retry_max_delay'
        CIRCUIT_BREAKER_THRESHOLD = 'circuit_breaker_threshold'
        CIRCUIT_BREAKER_RECOVERY = 'circuit_breaker_recovery'
        PROXY = 'proxy'
        FUNCTION_SELF = 'self'
        FUNCTION_RPC = 'RpcFunction'
        FUNCTION_WORKER = 'WorkerFunction'
        FUNCTION_PARAM_NAME = 'param_name'
        FUNCTION_PARAM_DEFAULT_VALUE = 'default_value'


class TASK:
    @unique
    class CONFIG(Enum):
        ID = 'task_id'
        STATUS = 'status'

        BODY = 'body'
        WEIGHT = 'weight'
        QUEUE_NAME = 'queue_name'

        END_TIME = 'end_time'
        START_TIME = 'start_time'
        CREATE_TIME = 'create_time'
        ERROR_MESSAGE = 'error_message'

        SOURCE_ID = 'source_id'
        RESOURCES = 'resources'
        DEPENDS_ON = 'depends_on'

        IS_SUB_TASK_ALL_FINISH = 'is_sub_task_all_finish'

    class DEFAULT(Enum):
        WEIGHT = 1
        STATUS = STATUS.PENDING.value
        SOURCE_ID = None
        RESOURCES = None
        DEPENDS_ON = None


class BUILD:
    @unique
    class CONFIG(Enum):
        NAME = 'name'
        UNIQUE_ID = 'unique_id'

        WORKER_PID = 'worker_pid'
        WORKER_NAME = 'worker_name'
        WORKER_IPADDR = 'worker_ipaddr'
        WORKER_VERSION = 'worker_version'
        WORKER_FUNCTIONS = 'worker_functions'
        WORKER_MAX_PROCESS = 'worker_max_process'
        WORKER_RUN_PROCESS = 'worker_run_process'

        SERVICE_PID = 'service_pid'
        SERVICE_NAME = 'service_name'
        SERVICE_IPADDR = 'service_ipaddr'
        SERVICE_VERSION = 'service_version'
        SERVICE_FUNCTIONS = 'service_functions'

        SYSTEM_SERVICE_NAME = 'system_proxy'


class WEB:
    @unique
    class CONFIG(Enum):
        KEY = 'web'
        PORT = 'port'
        USERNAME = 'username'
        PASSWORD = 'password'
        BIND_IP = 'bind_ip'

    class DEFAULT(Enum):
        PORT = 7860
        USERNAME = 'scheduleAdmin'
        PASSWORD = 'scheduleAdminPassword'
        BIND_IP = '0.0.0.0'


class DEFAULTS:
    """
    Centralized default values and magic constants for all subsystems.
    Use these instead of hard-coded numeric literals throughout the codebase.
    """

    # ── Logging ──────────────────────────────────────────────────────────
    LOGGER_MAX_BYTES = 10 * 1024 * 1024
    LOGGER_BACKUP_COUNT = 5

    # ── Task Scheduler ───────────────────────────────────────────────────
    SCHEDULER_TASK_FETCH_LIMIT = 1000
    SCHEDULER_LOCK_REFRESH_INTERVAL = 5
    SCHEDULER_LOCK_EXPIRE_SECONDS = 15
    SCHEDULER_TTL_EXPIRE_SECONDS = 60
    SCHEDULER_ERROR_BACKOFF_SECONDS = 5
    SCHEDULER_THREAD_JOIN_TIMEOUT = 10.0

    # ── Worker (task consumer) ───────────────────────────────────────────
    WORKER_DEFAULT_MAX_PROCESS = 10
    WORKER_STATUS_CACHE_TTL = 5
    WORKER_CAPACITY_SYNC_INTERVAL = 30
    WORKER_AVAILABLE_SLOTS_CACHE_TTL = 1
    WORKER_CAPACITY_EXHAUSTED_SLEEP = 0.1
    WORKER_PERSISTENT_ERROR_SLEEP = 0.5

    # ── Thread / Process Executor ────────────────────────────────────────
    EXECUTOR_DEFAULT_MAX_WORKERS = 5
    EXECUTOR_DEFAULT_RETRY_DELAY = 1.0
    EXECUTOR_DEFAULT_MAX_QUEUE_SIZE = 5000
    EXECUTOR_DEFAULT_MAX_RETRIES = 3
    EXECUTOR_TASK_QUEUE_PUT_TIMEOUT = 30
    EXECUTOR_QUEUE_GET_TIMEOUT = 1
    EXECUTOR_LOOP_YIELD_INTERVAL = 0.1
    EXECUTOR_SHUTDOWN_TIMEOUT = 5.0
    EXECUTOR_SIGTERM_JOIN_TIMEOUT = 2.0

    # ── RabbitMQ ──────────────────────────────────────────────────────────
    RABBITMQ_CONNECTION_POOL_SIZE = 5
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = 300
    RABBITMQ_SOCKET_TIMEOUT = 5
    RABBITMQ_DEFAULT_PRIORITY = 0
    RABBITMQ_MAX_PRIORITY = 10
    RABBITMQ_PROCESS_DATA_EVENTS_TIME_LIMIT = 1
    RABBITMQ_CONNECTION_ATTEMPTS = 3
    RABBITMQ_CONNECTION_RETRY_DELAY = 1
    RABBITMQ_SEND_RETRY_MAX = 3
    RABBITMQ_HEARTBEAT = 30
    RABBITMQ_PERSISTENT_DELIVERY = 2

    # ── Redis ─────────────────────────────────────────────────────────────
    REDIS_DEFAULT_EXPIRE_SECONDS = 86400
    REDIS_DEFAULT_INCREMENT_DELTA = 1
    REDIS_SOCKET_TIMEOUT = 5

    # ── MongoDB / Pagination ─────────────────────────────────────────────
    MONGODB_PAGINATION_DEFAULT_LIMIT = 10
    MONGODB_PAGINATION_DEFAULT_SKIP = 0

    # ── Process Management (launcher / web UI) ───────────────────────────
    PROCESS_WAIT_TIMEOUT = 3
    PROCESS_TASKKILL_TIMEOUT = 5

    # ── Snowflake ID Generator ────────────────────────────────────────────
    SNOWFLAKE_TWITTER_EPOCH = 1288834974657
    SNOWFLAKE_DATACENTER_ID_BITS = 5
    SNOWFLAKE_MACHINE_ID_BITS = 5
    SNOWFLAKE_SEQUENCE_BITS = 12
    SNOWFLAKE_DEFAULT_SEQUENCE = 0
    SNOWFLAKE_SMALL_ROLLBACK_THRESHOLD_MS = 100
    SNOWFLAKE_ROLLBACK_PADDING_MS = 1

    # ── Cron Expression ───────────────────────────────────────────────────
    CRON_PARTS_COUNT = 6


CONFIGS = [
    MONGODB,
    REDIS,
    RABBITMQ,
    LOGGER,
    RPC,
    WEB,
]
