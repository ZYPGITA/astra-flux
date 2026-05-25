=========
AstraFlux
=========

AstraFlux is a lightweight distributed service framework built around a self-developed dependency injection
container. It delivers enterprise-grade capabilities - task scheduling with DAG dependencies, RabbitMQ-based
RPC with circuit breaker, worker management, and a web dashboard - with minimal boilerplate and zero
external orchestration dependencies.

Install
=======

.. code-block:: bash

    pip install astraflux

Requires Python 3.9+, with MongoDB, Redis, and RabbitMQ running.

Quick Start
===========

1. Create a ``config.yaml``

.. code-block:: yaml

    mongodb:
      host: 127.0.0.1
      port: 27017
      username: scheduleAdmin
      password: scheduleAdminPassword

    redis:
      host: 127.0.0.1
      port: 6379
      password: scheduleAdminPassword
      db_index: 8

    rabbitmq:
      host: 127.0.0.1
      port: 5672
      username: scheduleAdmin
      password: scheduleAdminPassword

    logger:
      path: logs
      level: INFO

    web:
      port: 7860
      username: admin
      password: admin

    rpc:
      call_timeout: 30
      max_retries: 3
      circuit_breaker_threshold: 5
      circuit_breaker_recovery: 30

2. Write a service

.. important::
    The component builder imports the service/worker module and looks for **exact class names**:

    - ``RpcFunction`` - for the RPC service class
    - ``WorkerFunction`` - for the worker class

    These class names are hardcoded - *not* configurable - and must match exactly.

.. code-block:: python

    from astraflux import ServiceConstructor, WorkerConstructor, rpc_decorator

    class RpcFunction(ServiceConstructor):
        service_name = "my_service"

        @rpc_decorator
        def ping(self):
            return {"status": "ok"}

        @rpc_decorator
        def add(self, a: int, b: int) -> int:
            return {"result": a + b}

    class WorkerFunction(WorkerConstructor):
        worker_name = "my_service"

        def run(self, data: dict):
            self.logger.info(f"Executing task: {data}")
            return {"status": "success"}

3. Start everything

.. code-block:: python

    from astraflux import AstraFlux, launch_register, launch_start

    AstraFlux(yaml_path="path/to/config.yaml", current_dir="path/to/project")

    launch_register(services=[RpcFunction, WorkerFunction])
    launch_start()

    # Keep the process alive
    import time
    while True:
        time.sleep(60)

Architecture
============

::

    +---------------------------+
    |  DI Container (FixtureManager)  |
    |  Single / Global / Thread scopes|
    |  Lazy resolution of dependencies|
    +-------+-------------------+
            |
    +-------+------+----------+---------+----------+
    |       |      |          |         |          |
    v       v      v          v         v          v
  MongoDB  Redis  RabbitMQ  Scheduler Executors  Web UI
  (tasks) (workers) (RPC)   (DAG)    (pools)   (Flask)

Config propagation::

    config.yaml --> _settings.py (merge defaults) --> fixture_config (dict)
                                |
                    Each provider reads config[key]
                    (No fallback logic in business code)

Why AstraFlux?
==============

* **Self-owned DI container** - register once, inject everywhere. No framework lock-in.
* **Task DAG scheduler** - dependency graphs, failure propagation, subtask management.
* **Production RPC** - timeout, exponential backoff retry, circuit breaker.
* **Worker capacity management** - Redis-based slot tracking, automatic scheduling.
* **Web management UI** - out of the box, tasks, services, system monitoring.
* **Zero orchestration** - no Kubernetes, no etcd, no ZooKeeper needed.

Modules
=======

DI Container
------------

The ``FixtureManager`` is the backbone of AstraFlux. Services are registered as named fixtures
with configurable scopes. When a function is called, the container resolves its parameter names
as fixture names and injects the values automatically.

.. code-block:: python

    @register_fixture(name="fixture_config")
    def app_config():
        yield config_data  # merged with defaults

    @register_fixture(name="fixture_mongodb")
    def task_collection(fixture_config):
        cfg = fixture_config["mongodb"]
        yield MongoDatabase(cfg)

Supported scopes:

- ``SINGLETON`` - one instance per process
- ``GLOBAL`` - cached until explicit ``clear_cache()``
- ``THREAD`` - one cache entry per thread (thread-safe)

Cleanup is built in: when a fixture uses ``yield``, the code after the yield runs on
``clear_cache()``, making it trivial to release resources.

Configuration System
--------------------

All defaults are defined in constants and merged by the config loader into a complete dict.
Business code reads ``config[key]`` directly - no fallback logic.

Currently managed config sections:

- ``MONGODB``, ``REDIS``, ``RABBITMQ``, ``LOGGER``, ``RPC``, ``WEB``

If a config section or key is missing from the YAML file, the system falls back to the
hardcoded default - every optional field has a sensible default.

Distributed Task Scheduler
--------------------------

The scheduler runs as a cron job (every 10 seconds) in a single 8-step cycle:

1. **Fetch** active tasks from MongoDB
2. **Build** a DAG from task dependencies (``depends_on`` field)
3. **Propagate failures** - if a parent fails, its children are marked failed
4. **Find runnable tasks** - all dependencies must be satisfied or in a final state
5. **Identify priority subtasks** - children of currently running parents get priority
6. **Dispatch** to RabbitMQ queues, respecting worker capacity per service
7. **Update** parent statuses based on child completion
8. **Persist** all status changes back to MongoDB

.. code-block:: python

    from astraflux import task_submit, task_stop, task_retry

    # Submit a task to a specific worker queue
    task_id = task_submit(worker_name="my_service", body={...})

    # Stop a running task
    task_stop(task_id)

    # Retry a failed task
    task_retry(task_id)

    # Subtasks with dependency tracking
    subtasks_create(
        subtask_queue="my_service",
        source_id=task_id,
        subtask_list=[{...}, {...}]
    )

The scheduler runs in ``distributed_unique`` mode by default - only one instance across the
cluster executes the scheduling cycle. ``ip_unique`` ensures one monitor per host.

RPC
---

RabbitMQ-based RPC with production-grade reliability:

- **Timeout** - configurable per-call, defaults to 30 seconds
- **Retry** - exponential backoff with configurable base and max delay
- **Circuit breaker** - opens after N consecutive failures, recovers after M seconds
- **Auto-reconnection** - recovers from connection/channel errors transparently

.. code-block:: python

    from astraflux import proxy_call, rpc_decorator, start_consumer

    # Client: call a remote method
    result = proxy_call(
        service_name="my_service",
        method_name="add",
        a=1, b=2
    )

    # Server: mark methods as remotely callable
    class RpcFunction(ServiceConstructor):
        @rpc_decorator
        def add(self, a, b):
            return a + b

    # Start consuming RPC requests
    start_consumer(queue_name="my_service", service_instance=instance)

Arguments are serialized with ``dill`` (supports most Python types) and sent to a RabbitMQ
queue. The server dispatches by method name and returns the result to a private callback queue.

Worker Management
-----------------

Workers are registered in Redis with their capacity and live process list. The scheduler queries
this data to decide where to dispatch tasks.

.. code-block:: python

    from astraflux import (
        redis_store_worker_data,
        redis_get_available_slots,
        redis_get_worker_status,
        get_total_available_slots_by_server_name,
    )

    # Register a worker
    redis_store_worker_data({
        "unique_id": "my_service_192.168.1.10",
        "worker_name": "my_service",
        "worker_ipaddr": "192.168.1.10",
        "worker_max_process": 10,
        ...
    })

    # Check available capacity before dispatching
    slots = redis_get_available_slots(unique_id="my_service_192.168.1.10")
    if slots > 0:
        # Dispatch task...

    # Service-level capacity summary
    total_slots = get_total_available_slots_by_server_name("my_service")

Each worker process is spawned by ``multiprocessing.Process`` - fully isolated, robust resource
management.

Launcher
--------

The ``ServiceLauncher`` orchestrates the complete startup sequence. For each registered service:

1. Launch a **service process** - starts an RPC consumer that listens on RabbitMQ
2. Launch a **worker process** - starts a message queue consumer that processes tasks

Then, if scheduled jobs are enabled:

3. Start the **TaskScheduler** cron job (every 10 seconds, distributed-unique)
4. Start the **SystemMonitoring** cron job (every 30 seconds, ip-unique)
5. Optionally launch the **Web UI** as a separate process

.. code-block:: python

    from astraflux import launch_register, launch_start

    launch_register(services=[RpcFunction, WorkerFunction])
    launch_start(run_app=True, scheduled=True)

The launcher also handles graceful cleanup via ``kill()`` - terminates all spawned processes.

Web Management UI
-----------------

Flask-based dashboard with login authentication, providing:

- **Service monitoring** - platform info, RPC status, function listing
- **Task management** - submit, stop, retry, paginated task list
- **Worker status** - live capacity, run processes, available slots
- **System monitoring** - memory, CPU, disk usage via psutil

Access: ``http://<bind_ip>:<port>`` (default ``0.0.0.0:7860``).

Executors
---------

Thread and process pool executors with built-in retry and progress tracking:

.. code-block:: python

    from astraflux import thread_executor, process_executor

    # Thread pool (shared memory, fast)
    executor = thread_executor(max_workers=5, retry_delay=1.0)
    executor.submit(func=my_task, arg1=val1, max_retries=3)
    executor.start()

    # Process pool (isolated, for CPU-bound work)
    pe = process_executor(max_workers=4)
    pe.submit(func=my_cpu_task, data=big_data)
    pe.start()

Features:
- Exponential backoff retry for failed tasks
- Task status tracking (pending, running, failed, success)
- Configurable queue size

Configuration Reference
=======================

Full ``config.yaml`` reference:

.. code-block:: yaml

    mongodb:
      host: 127.0.0.1
      port: 27017
      username: scheduleAdmin
      password: scheduleAdminPassword
      max_connections: 20

    redis:
      host: 127.0.0.1
      port: 6379
      password: scheduleAdminPassword
      db_index: 8
      max_connections: 20

    rabbitmq:
      host: 127.0.0.1
      port: 5672
      username: scheduleAdmin
      password: scheduleAdminPassword

    logger:
      path: logs
      level: INFO

    web:
      port: 7860
      bind_ip: 0.0.0.0
      username: admin
      password: admin

    rpc:
      call_timeout: 30
      max_retries: 3
      retry_base_delay: 1.0
      retry_max_delay: 30.0
      circuit_breaker_threshold: 5
      circuit_breaker_recovery: 30

Any omitted key falls back to defaults.

License
=======

MIT
