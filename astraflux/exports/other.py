# -*- coding: utf-8 -*-
from astraflux.core import global_manager


def ipaddr():
    """
    Retrieves the IP address of the current machine by establishing a UDP connection
    to the specified IP and port.

    Returns:
        str: The IP address of the current machine.
    """

    def _backcall(fixture_ipaddr):
        return fixture_ipaddr

    return global_manager.bind_fixture_func(_backcall)()


def devices_id():
    """
    Get Devices ID
    """

    def _backcall(fixture_devices_id):
        return fixture_devices_id

    return global_manager.bind_fixture_func(_backcall)()


def date_time_obj(data_str: str, fmt=False, timezone=False):
    """
    Specify the timezone and format, and return a time object.

    Args:
        data_str (str): The time string to be converted.
        fmt (str or bool): The format of the time string. If False, use the default format.
        timezone (str or bool): The timezone. If False, use the default timezone.

    Returns:
        datetime.datetime: A datetime object representing the converted time.
    """

    def _backcall(fixture_time_processor):
        return fixture_time_processor(fmt, timezone).date_time_obj(data_str)

    return global_manager.bind_fixture_func(_backcall)()


def format_converted_time(data_str: str, fmt=False, timezone=False, r_fmt=False):
    """
    Format a time string according to the specified format and timezone.

    Args:
        data_str (str): The time string to be formatted.
        fmt (str or bool): The input format of the time string. If False, use the default format.
        timezone (str or bool): The timezone. If False, use the default timezone.
        r_fmt (str or bool): The output format of the time string. If False, use the input format.

    Returns:
        str: A formatted time string.
    """

    def _backcall(fixture_time_processor):
        return fixture_time_processor(fmt, timezone).format_converted_time(data_str, r_fmt)

    return global_manager.bind_fixture_func(_backcall)()


def converted_time(fmt=False, timezone=False):
    """
    Specify timezone and format, return the current time.

    Args:
        fmt (str or bool): The format of the time string. If False, use the default format.
        timezone (str or bool): The timezone. If False, use the default timezone.

    Returns:
        str: A string representing the current time in the specified format and timezone.
    """

    def _backcall(fixture_time_processor):
        return fixture_time_processor(fmt, timezone).converted_time()

    return global_manager.bind_fixture_func(_backcall)()


def config_obj():
    """
    Get Config Object
    """

    def _backcall(fixture_config):
        return fixture_config

    return global_manager.bind_fixture_func(_backcall)()
