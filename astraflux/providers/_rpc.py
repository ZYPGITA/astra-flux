# -*- encoding: utf-8 -*-

import time
import pika
import dill
import random
import builtins
from pika.exceptions import ChannelClosed, AMQPConnectionError

from astraflux.core import global_manager
from astraflux.config.constants import *
from astraflux.exports.generate_id import snowflake_id


class ServiceUnavailableError(Exception):

    def __init__(self, service_name):
        super().__init__(f"Service '{service_name}' is not available")
        self.service_name = service_name


class CircuitBreaker:
    """Simple circuit breaker for RPC calls."""

    def __init__(self, config: dict, logger=None):
        self._threshold = config[RPC.CONFIG.CIRCUIT_BREAKER_THRESHOLD.value]
        self._recovery_timeout = config[RPC.CONFIG.CIRCUIT_BREAKER_RECOVERY.value]
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._state = "closed"
        self.logger = logger

    @property
    def is_open(self) -> bool:
        if self._state == "open":
            if time.time() - self._last_failure_time > self._recovery_timeout:
                self._state = "half-open"
                return False
            return True
        return False

    def record_success(self):
        self._failure_count = 0
        self._state = "closed"

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._state == "half-open" or self._failure_count >= self._threshold:
            self._state = "open"
            if self.logger:
                self.logger.warning(
                    "Circuit breaker OPEN for RPC (%d consecutive failures, threshold=%d)",
                    self._failure_count, self._threshold
                )


class RpcCaller:
    """
    A RabbitMQ RPC client with timeout, retry with exponential backoff,
    and circuit breaker protection.
    """

    def __init__(self, config: dict, rpc_config: dict = None, logger=None):
        self.logger = logger
        _rc = rpc_config or {}
        self.timeout = _rc[RPC.CONFIG.RPC_CALL_TIMEOUT.value]
        self.max_retries = _rc[RPC.CONFIG.MAX_RETRIES.value]
        self.retry_base_delay = _rc[RPC.CONFIG.RETRY_BASE_DELAY.value]
        self.retry_max_delay = _rc[RPC.CONFIG.RETRY_MAX_DELAY.value]

        self.circuit_breaker = CircuitBreaker(rpc_config or {}, logger)

        self.response = None
        self.corr_id = None

        self._host = config[RABBITMQ.CONFIG.HOST.value]
        self._port = config[RABBITMQ.CONFIG.PORT.value]
        self._user = config[RABBITMQ.CONFIG.USERNAME.value]
        self._password = config[RABBITMQ.CONFIG.PASSWORD.value]

        self.credentials = pika.PlainCredentials(self._user, self._password)

        self.connection = self._create_connection()
        self.channel = self.connection.channel()

        self.queue = self.channel.queue_declare(
            queue='',
            exclusive=True,
            auto_delete=True
        ).method.queue

        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )

    def _create_connection(self):
        params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=self.credentials,
            heartbeat=600,
            connection_attempts=3,
            retry_delay=5
        )
        return pika.BlockingConnection(params)

    def _reconnect(self):
        """Re-establish connection after a failure."""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            if self.logger:
                self.logger.error(e)
        self.connection = self._create_connection()
        self.channel = self.connection.channel()
        self.queue = self.channel.queue_declare(
            queue='',
            exclusive=True,
            auto_delete=True
        ).method.queue
        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )

    def _check_service_available(self, service_name):
        try:
            self.channel.queue_declare(
                queue=service_name,
                passive=True
            )
            return True
        except ChannelClosed as e:
            if e.args[0] == 404:
                return False
            raise

    @staticmethod
    def _raise_rpc_exception(error_info):
        ex_type = error_info.get('type', 'RpcError')
        ex_msg = error_info.get('exception', 'Unknown RPC error')

        allowed_exceptions = [
            'TypeError', 'ValueError', 'KeyError',
            'AttributeError', 'RuntimeError', 'PermissionError'
        ]

        try:
            if ex_type in allowed_exceptions:
                exception_class = getattr(builtins, ex_type)
                if issubclass(exception_class, Exception):
                    raise exception_class(ex_msg)
            raise RuntimeError(ex_msg)
        except AttributeError:
            raise RuntimeError(ex_msg)

    def _on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = dill.loads(body)

    def _execute_call(self, service_name, method_name, args, kwargs):
        """Single RPC call attempt without retry logic."""
        self.response = None

        if not self._check_service_available(service_name):
            raise ServiceUnavailableError(service_name)

        request = {
            'method': method_name,
            'args': args,
            'kwargs': kwargs
        }

        self.channel.basic_publish(
            exchange='',
            routing_key=service_name,
            properties=pika.BasicProperties(
                reply_to=self.queue,
                correlation_id=self.corr_id,
                delivery_mode=2
            ),
            body=dill.dumps(request)
        )

        start_time = time.time()
        while self.response is None:
            if time.time() - start_time > self.timeout:
                raise TimeoutError(f"RPC call to {service_name}.{method_name} timed out after {self.timeout}s")
            self.connection.process_data_events(time_limit=1)

        if isinstance(self.response, dict):
            status = self.response.get('status')
            if status == 'error':
                self._raise_rpc_exception(self.response)
            if status == 'success':
                return self.response.get('result')
        return self.response

    def call(self, service_name, method_name, *args, **kwargs):
        """
        Call a remote procedure with timeout, retry (exponential backoff),
        and circuit breaker protection.
        """
        if self.circuit_breaker.is_open:
            raise RuntimeError(
                f"RPC circuit breaker is OPEN for {service_name}.{method_name}. "
                f"Retry later when the circuit recovers."
            )

        self.corr_id = snowflake_id()

        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                result = self._execute_call(service_name, method_name, args, kwargs)
                self.circuit_breaker.record_success()
                return result

            except (AMQPConnectionError, ChannelClosed) as conn_err:
                last_exception = conn_err
                if attempt < self.max_retries:
                    delay = min(self.retry_base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5),
                                self.retry_max_delay)
                    if self.logger:
                        self.logger.warning(
                            "RPC connection error (attempt %d/%d): %s. Reconnecting in %.1fs...",
                            attempt, self.max_retries, conn_err, delay
                        )
                    try:
                        self._reconnect()
                    except Exception as reconnect_err:
                        if self.logger:
                            self.logger.error(f"RPC reconnection failed: {reconnect_err}")
                    time.sleep(delay)

            except (TimeoutError, ServiceUnavailableError) as call_err:
                last_exception = call_err
                if isinstance(call_err, TimeoutError) and attempt < self.max_retries:
                    delay = min(self.retry_base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5),
                                self.retry_max_delay)
                    if self.logger:
                        self.logger.warning(
                            "RPC call timed out (attempt %d/%d) for %s.%s. Retrying in %.1fs...",
                            attempt, self.max_retries, service_name, method_name, delay
                        )
                    time.sleep(delay)

        self.circuit_breaker.record_failure()
        raise TimeoutError(
            f"RPC call to {service_name}.{method_name} failed after {self.max_retries} attempts. "
            f"Last error: {last_exception}"
        ) from last_exception


class RpcListener:
    """
    RabbitMQ RPC server -- listens on a durable queue and dispatches to a service instance.
    """

    def __init__(self, config, logger=None):
        self.logger = logger
        self._host = config[RABBITMQ.CONFIG.HOST.value]
        self._port = config[RABBITMQ.CONFIG.PORT.value]
        self._user = config[RABBITMQ.CONFIG.USERNAME.value]
        self._password = config[RABBITMQ.CONFIG.PASSWORD.value]

        self.credentials = pika.PlainCredentials(self._user, self._password)

        self.connection = self._create_connection()
        self.channel = self.connection.channel()

    def _create_connection(self):
        params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=self.credentials,
            heartbeat=600,
            connection_attempts=3,
            retry_delay=5
        )
        return pika.BlockingConnection(params)

    def start_consumer(self, queue_name, service_instance):
        """
        Start consuming from a durable queue and dispatching to service_instance.
        """
        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={'x-ha-policy': 'all'}
        )
        self.channel.basic_qos(prefetch_count=100)

        def callback(ch, method_frame, props, body):
            response = None
            try:
                data = dill.loads(body)
                method_name = data['method']
                args = data.get('args', [])
                kwargs = data.get('kwargs', {})

                method = getattr(service_instance(), method_name)
                result = method(*args, **kwargs)
                response = dill.dumps({
                    'status': 'success',
                    'result': result
                })
            except Exception as e:
                if self.logger:
                    self.logger.error(e)
                response = dill.dumps({
                    'status': 'error',
                    'exception': str(e),
                    'type': type(e).__name__
                })
            finally:
                ch.basic_ack(method_frame.delivery_tag)
                if props.reply_to:
                    ch.basic_publish(
                        exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(
                            correlation_id=props.correlation_id,
                            delivery_mode=2
                        ),
                        body=response
                    )

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            consumer_tag=f"{queue_name}_consumer"
        )

        self.channel.start_consuming()


@global_manager.register_fixture(name="fixture_rpc_client", scope=Scope.THREAD)
def _rpc_caller(fixture_config, fixture_logger):
    """
    Create a RabbitMQ RPC client with circuit breaker, retry, and timeout.
    """
    _rabbitmq_config = fixture_config[RABBITMQ.CONFIG.KEY.value]
    _rpc_config = fixture_config[RPC.CONFIG.KEY.value]
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, RABBITMQ.CONFIG.KEY.value)

    rpc_client = RpcCaller(config=_rabbitmq_config, rpc_config=_rpc_config, logger=_logger)
    yield rpc_client
    try:
        rpc_client.connection.close()
    except Exception as e:
        if _logger:
            _logger.error(e)


@global_manager.register_fixture(name="fixture_rpc_server", scope=Scope.THREAD)
def _rpc_listener(fixture_config, fixture_logger):
    """
    Create a RabbitMQ RPC server (listener).
    """
    _rabbitmq_config = fixture_config[RABBITMQ.CONFIG.KEY.value]
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, RABBITMQ.CONFIG.KEY.value)

    rpc_server = RpcListener(config=_rabbitmq_config, logger=_logger)
    yield rpc_server
    try:
        rpc_server.connection.close()
    except Exception as e:
        if _logger:
            _logger.error(e)
