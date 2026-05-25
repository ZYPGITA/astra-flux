# -*- coding: utf-8 -*-

from astraflux.core import global_manager


def rabbitmq_send_message(queue: str, message: dict):
    """
    Send a message to a specified RabbitMQ queue by leveraging the global fixture manager.

    :param queue: Name of the target RabbitMQ queue to which the message will be sent. Must be a
                  non-empty string corresponding to an existing or declarable queue.
    :param message: A dictionary containing the message payload. The dictionary should be JSON-serializable
                    (i.e., contain only primitive types, lists, and nested dictionaries) to ensure
                    compatibility with the underlying RabbitMQ producer's serialization logic.
    :return: None. The function executes the message sending operation synchronously and returns
             nothing upon successful completion.
    :raises: Any exceptions raised by the `global_manager.bind_fixture_func` or the underlying
             `RabbitMQBroker.rabbitmq_send_message` method, such as AMQPConnectionError
             (if RabbitMQ connection fails) or AMQPChannelError (if channel operations fail), will
             propagate up from this function.
    """

    def _backcall(fixture_rabbitmq):
        """
        Callback function to be executed by the global fixture manager, receiving the bound
        RabbitMQBroker instance as an argument.

        :param fixture_rabbitmq: An instance of RabbitMQBroker retrieved from the
                                 global fixture manager, used to perform the actual message sending.
        """
        fixture_rabbitmq.rabbitmq_send_message(queue=queue, message=message)

    return global_manager.bind_fixture_func(_backcall)()


def rabbitmq_receive_message(queue: str, callback):
    """
    Start consuming messages from a specified RabbitMQ queue by leveraging the global fixture manager.

    :param queue: Name of the target RabbitMQ queue to consume messages from. Must be a non-empty
                  string corresponding to an existing or declarable queue.
    :param callback: A callable function that will be invoked for each received message. The callback
                     must conform to the signature expected by the underlying RabbitMQ consumer:
                     it should accept at least four arguments (channel, method, properties, body) as
                     defined by the pika library's on_message_callback specification.
    :return: None. The function starts the consumer loop synchronously and returns only when the
             consumer is stopped (e.g., via channel.stop_consuming() or an exception).
    :raises: Any exceptions raised by the `global_manager.bind_fixture_func` or the underlying
             `RabbitMQBroker.rabbitmq_receive_message` method, such as AMQPConnectionError
             (if RabbitMQ connection fails), AMQPChannelError (if channel operations fail), or
             exceptions from the callback function itself, will propagate up from this function.
    """

    def _backcall(fixture_rabbitmq):
        """
        Callback function to be executed by the global fixture manager, receiving the bound
        RabbitMQBroker instance as an argument.

        :param fixture_rabbitmq: An instance of RabbitMQBroker retrieved from the
                                 global fixture manager, used to start the message consumer.
        """
        fixture_rabbitmq.rabbitmq_receive_message(queue=queue, callback=callback)

    return global_manager.bind_fixture_func(_backcall)()
