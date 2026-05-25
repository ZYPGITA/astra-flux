# -*- coding: utf-8 -*-

from functools import wraps

from astraflux.core import global_manager
from astraflux.config.constants import *


def rpc_decorator(func):
    """
    Decorator for RPC functions.
    Args:
        func (function): The function to be decorated.
    Returns:
        function: The decorated function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper._is_rpc_func = True
    return wrapper


def proxy_call(service_name: str, method_name: str, **params):
    """
    Makes a remote procedure call to the specified service and method with the given parameters.

    Args:
        service_name (str): The name of the service to call.
        method_name (str): The name of the method to call.
        **params: Arbitrary keyword arguments to pass to the method.

    Returns:
        Any: The result of the remote procedure call.
    """

    _name = '{}_{}'.format(PROJECT.NAME.value, service_name)

    def _backcall(fixture_rpc_client):
        return fixture_rpc_client.call(
            service_name=_name,
            method_name=method_name,
            **params
        )

    return global_manager.bind_fixture_func(_backcall)()


def start_consumer(queue_name: str, service_instance: object):
    """
    Start a RabbitMQ consumer.
    Args:
        queue_name (str): The name of the queue to consume from.
        service_instance (object): The instance of the service to call when a message is received.
    """

    def _backcall(fixture_rpc_server):
        return fixture_rpc_server.start_consumer(queue_name=queue_name, service_instance=service_instance)

    return global_manager.bind_fixture_func(_backcall)()
