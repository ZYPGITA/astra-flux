# -*- coding: utf-8 -*-

import json
import time
import uuid
import logging
import threading
from typing import Callable, Dict, Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError, StreamLostError

from astraflux.core import global_manager
from astraflux.config.constants import *


class RabbitMQError:
    """Base exception for RabbitMQ errors"""

    def __init__(self, e: Exception) -> None:
        pass


class RabbitMQBroker:
    """Thread-safe implementation for RabbitMQ producer and consumer operations"""

    def __init__(self, config: dict, logger: logging, connection_pool_size: int = 5):
        """
        Initialize the thread-safe RabbitMQ producer/consumer

        Args:
            config (dict): Configuration parameters for RabbitMQ connection
            logger (logging): Logger instance for logging operations
            connection_pool_size (int): Size of the connection pool (unused in current implementation)
        """
        self.logger = logger
        self._lock = threading.RLock()

        self._host = config[RABBITMQ.CONFIG.HOST.value]
        self._port = config[RABBITMQ.CONFIG.PORT.value]
        self._user = config[RABBITMQ.CONFIG.USERNAME.value]
        self._password = config[RABBITMQ.CONFIG.PASSWORD.value]
        self._virtual_host = '/'

        self._connection_pool_size = connection_pool_size
        self._producer_connections: Dict[str, pika.BlockingConnection] = {}
        self._producer_channels: Dict[str, BlockingChannel] = {}
        self._consumer_connections: Dict[str, pika.BlockingConnection] = {}
        self._consumer_channels: Dict[str, BlockingChannel] = {}

        self._connection_params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            virtual_host=self._virtual_host,
            credentials=pika.PlainCredentials(self._user, self._password),
            heartbeat=30,
            blocked_connection_timeout=300,
            connection_attempts=3,
            retry_delay=1,
            socket_timeout=5
        )

        self._declared_queues = set()
        self._queue_lock = threading.RLock()

        self._thread_local = threading.local()

    def _get_producer_connection_id(self) -> str:
        """
        Generate and get a unique producer connection ID for current thread

        Returns:
            str: Unique producer connection ID
        """
        if not hasattr(self._thread_local, 'producer_conn_id'):
            with self._lock:
                conn_id = f"producer_{threading.current_thread().ident}_{uuid.uuid4().hex[:8]}"
                self._thread_local.producer_conn_id = conn_id
        return self._thread_local.producer_conn_id

    def _get_consumer_connection_id(self) -> str:
        """
        Generate and get a unique consumer connection ID for current thread

        Returns:
            str: Unique consumer connection ID
        """
        if not hasattr(self._thread_local, 'consumer_conn_id'):
            with self._lock:
                conn_id = f"consumer_{threading.current_thread().ident}_{uuid.uuid4().hex[:8]}"
                self._thread_local.consumer_conn_id = conn_id
        return self._thread_local.consumer_conn_id

    def _create_connection(self, conn_type: str = 'producer') -> pika.BlockingConnection:
        """
        Create a new RabbitMQ connection and corresponding channel

        Args:
            conn_type (str): Type of connection, 'producer' or 'consumer'

        Returns:
            pika.BlockingConnection: Created RabbitMQ connection

        Raises:
            AMQPConnectionError: If connection creation fails
        """
        try:
            connection = pika.BlockingConnection(self._connection_params)

            if conn_type == 'producer':
                conn_id = self._get_producer_connection_id()
                self._producer_connections[conn_id] = connection

                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)
                self._producer_channels[conn_id] = channel
            else:
                conn_id = self._get_consumer_connection_id()
                self._consumer_connections[conn_id] = connection

                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)
                self._consumer_channels[conn_id] = channel

            self.logger.debug(f"Successfully created RabbitMQ {conn_type} connection {conn_id}")
            return connection

        except Exception as e:
            self.logger.error(f"Failed to create RabbitMQ {conn_type} connection: {e}")
            raise AMQPConnectionError(f"Could not connect to RabbitMQ: {e}")

    def _get_producer_channel(self) -> BlockingChannel:
        """
        Get a valid producer channel, create new one if current is invalid

        Returns:
            BlockingChannel: Valid producer channel
        """
        conn_id = self._get_producer_connection_id()

        with self._lock:
            channel = self._producer_channels.get(conn_id)

            if channel is None or channel.is_closed:
                if conn_id in self._producer_connections:
                    try:
                        self._producer_connections[conn_id].close()
                    except Exception as e:
                        self.logger.error(f"Error closing producer connection {conn_id}: {e}")

                    del self._producer_connections[conn_id]

                self._create_connection('producer')
                channel = self._producer_channels[conn_id]

            return channel

    def _get_consumer_channel(self) -> BlockingChannel:
        """
        Get a valid consumer channel, create new one if current is invalid

        Returns:
            BlockingChannel: Valid consumer channel
        """
        conn_id = self._get_consumer_connection_id()

        with self._lock:
            channel = self._consumer_channels.get(conn_id)

            if channel is None or channel.is_closed:
                if conn_id in self._consumer_connections:
                    try:
                        self._consumer_connections[conn_id].close()
                    except Exception as e:
                        RabbitMQError(e)
                    del self._consumer_connections[conn_id]

                self._create_connection('consumer')
                channel = self._consumer_channels[conn_id]

            return channel

    def _ensure_queue_declared(self, queue: str, channel: BlockingChannel) -> BlockingChannel:
        """
        Ensure a queue is declared with double-checked locking to avoid duplicate declarations

        Args:
            queue (str): Name of the queue to declare
            channel (BlockingChannel): Channel to use for declaration

        Returns:
            BlockingChannel: Channel used for declaration (may be recreated if closed)

        Raises:
            AMQPChannelError: If queue declaration fails
            StreamLostError: If connection is lost during declaration
        """
        if queue in self._declared_queues:
            return channel

        with self._queue_lock:
            if queue not in self._declared_queues:
                try:
                    if channel.is_closed:
                        if self._is_producer_channel(channel):
                            channel = self._get_producer_channel()
                        else:
                            channel = self._get_consumer_channel()

                    channel.queue_declare(
                        queue=queue,
                        durable=True,
                        auto_delete=False,
                        exclusive=False,
                        arguments={'x-max-priority': 10}
                    )
                    self._declared_queues.add(queue)
                    self.logger.debug(f"Queue declared: {queue}")

                except (AMQPChannelError, StreamLostError) as e:
                    self.logger.error(f"Failed to declare queue {queue}: {e}")
                    if queue in self._declared_queues:
                        self._declared_queues.remove(queue)
                    raise

        return channel

    def _is_producer_channel(self, channel: BlockingChannel) -> bool:
        """
        Check if a channel is a producer channel

        Args:
            channel (BlockingChannel): Channel to check

        Returns:
            bool: True if channel is a producer channel, False otherwise
        """
        conn_id = self._get_producer_connection_id()
        return channel == self._producer_channels.get(conn_id)

    def rabbitmq_send_message(self, queue: str, message: dict, priority: int = 0):
        """
        Send a thread-safe message to specified RabbitMQ queue with retry mechanism

        Args:
            queue (str): Name of the target queue
            message (dict): Message content to send
            priority (int): Message priority (0-9, default 0)

        Raises:
            AMQPConnectionError: If message sending fails after max retries
            AMQPChannelError: If channel error occurs after max retries
            StreamLostError: If connection lost during sending after max retries
        """
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                channel = self._get_producer_channel()
                channel = self._ensure_queue_declared(queue, channel)

                properties = pika.BasicProperties(
                    delivery_mode=2,
                    priority=min(max(priority, 0), 9),
                    content_type='application/json',
                    timestamp=int(time.time())
                )

                if not isinstance(message, str):
                    message_body = json.dumps(message, ensure_ascii=False)
                else:
                    message_body = message

                channel.basic_publish(
                    exchange='',
                    routing_key=queue,
                    body=message_body,
                    properties=properties,
                    mandatory=True
                )

                self.logger.debug(f"Successfully sent message to queue {queue}")
                return

            except (AMQPConnectionError, AMQPChannelError, StreamLostError) as e:
                retry_count += 1
                self.logger.warning(
                    f"Failed to send message to queue {queue}, retrying {retry_count}/{max_retries}: {e}")

                conn_id = self._get_producer_connection_id()
                with self._lock:
                    if conn_id in self._producer_connections:
                        try:
                            self._producer_connections[conn_id].close()
                        except Exception as e:
                            RabbitMQError(e)
                        del self._producer_connections[conn_id]

                    if conn_id in self._producer_channels:
                        del self._producer_channels[conn_id]

                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                else:
                    self.logger.error(f"Failed to send message to queue {queue}, maximum retries reached")
                    raise

    def rabbitmq_receive_message(self, queue: str, callback: Callable, auto_ack: bool = False):
        """
        Start thread-safe message consumption from specified RabbitMQ queue with retry mechanism

        Args:
            queue (str): Name of the queue to consume from
            callback (Callable): Callback function to process received messages
            auto_ack (bool): Whether to auto-acknowledge messages (default False)

        Raises:
            AMQPConnectionError: If consumption fails after max retries
            AMQPChannelError: If channel error occurs after max retries
            StreamLostError: If connection lost during consumption after max retries
        """
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                channel = self._get_consumer_channel()
                channel = self._ensure_queue_declared(queue, channel)

                consumer_tag = f"{threading.current_thread().name}_{uuid.uuid4().hex[:8]}"

                channel.basic_consume(
                    queue=queue,
                    on_message_callback=callback,
                    auto_ack=auto_ack,
                    consumer_tag=consumer_tag
                )

                self.logger.debug(
                    f"Thread {threading.current_thread().name} started consuming "
                    f"queue {queue} (consumer_tag: {consumer_tag})")

                try:
                    channel.start_consuming()
                except KeyboardInterrupt:
                    self.logger.debug(f"Consumer {consumer_tag} interrupted")
                    channel.stop_consuming()
                    break
                except Exception as e:
                    self.logger.error(f"Error occurred during consumption: {e}")
                    raise

                return

            except (AMQPConnectionError, AMQPChannelError, StreamLostError) as e:
                retry_count += 1
                self.logger.warning(f"Failed to consume queue {queue}, retrying {retry_count}/{max_retries}: {e}")

                conn_id = self._get_consumer_connection_id()
                with self._lock:
                    if conn_id in self._consumer_connections:
                        try:
                            self._consumer_connections[conn_id].close()
                        except Exception as e:
                            RabbitMQError(e)
                        del self._consumer_connections[conn_id]

                    if conn_id in self._consumer_channels:
                        del self._consumer_channels[conn_id]

                if hasattr(self._thread_local, 'consumer_conn_id'):
                    delattr(self._thread_local, 'consumer_conn_id')

                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                else:
                    self.logger.error(f"Failed to consume queue {queue}, maximum retries reached")
                    raise

    def close_all_connections(self):
        """Close all RabbitMQ connections (producer and consumer) in a thread-safe manner"""
        with self._lock:
            for conn_id, connection in list(self._producer_connections.items()):
                try:
                    connection.close()
                    self.logger.debug(f"Closed producer connection {conn_id}")
                except Exception as e:
                    self.logger.error(f"Error closing producer connection {conn_id}: {e}")

            for conn_id, connection in list(self._consumer_connections.items()):
                try:
                    connection.close()
                    self.logger.debug(f"Closed consumer connection {conn_id}")
                except Exception as e:
                    self.logger.error(f"Error closing consumer connection {conn_id}: {e}")

            self._producer_connections.clear()
            self._producer_channels.clear()
            self._consumer_connections.clear()
            self._consumer_channels.clear()
            self._declared_queues.clear()

    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get statistics about current connections and declared queues

        Returns:
            Dict[str, Any]: Dictionary containing connection counts and declared queue list
        """
        with self._lock:
            return {
                'producer_connections': len(self._producer_connections),
                'consumer_connections': len(self._consumer_connections),
                'declared_queues': list(self._declared_queues)
            }


@global_manager.register_fixture(name="fixture_rabbitmq", scope=Scope.GLOBAL)
def _rabbitmq_broker(fixture_config, fixture_logger):
    """Register RabbitMQ fixture (thread-safe version)"""
    _rabbitmq_config = fixture_config[RABBITMQ.CONFIG.KEY.value]
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, RABBITMQ.CONFIG.KEY.value)

    _rabbitmq_producer = RabbitMQBroker(
        config=_rabbitmq_config,
        logger=_logger,
        connection_pool_size=10
    )

    yield _rabbitmq_producer

    _rabbitmq_producer.close_all_connections()
