# -*- encoding: utf-8 -*-

import socket
import base64
import hashlib
import datetime
from zoneinfo import ZoneInfo

from astraflux.core import global_manager
from astraflux.config.constants import *


class TimeHelper:
    def __init__(self, fmt=False, timezone=False):
        self.fmt, self.timezone = self._get_default_params(fmt, timezone)

    @staticmethod
    def _get_default_params(fmt, timezone):
        """
        Get default format and timezone if the input parameters are not provided.

        Args:
            fmt (str or bool): The format of the time string. If False, use the default format.
            timezone (str or bool): The timezone. If False, use the default timezone.

        Returns:
            tuple: A tuple containing the format and timezone.
        """
        if fmt is False:
            fmt = TIME.DEFAULT.TIME_FMT.value

        if timezone is False:
            timezone = TIME.DEFAULT.TIMEZONE.value

        return fmt, timezone

    def date_time_obj(self, data_str: str):
        """
        Specify the timezone and format, and return a time object.

        Args:
            data_str (str): The time string to be converted.

        Returns:
            datetime.datetime: A datetime object representing the converted time.
        """
        target_timezone = ZoneInfo(self.timezone)
        current_time = datetime.datetime.strptime(data_str, self.fmt)
        converted_time = current_time.replace(tzinfo=target_timezone)
        return converted_time

    def format_converted_time(self, data_str: str, r_fmt=False):
        """
        Format a time string according to the specified format and timezone.

        Args:
            data_str (str): The time string to be formatted.
            r_fmt (str or bool): The output format of the time string. If False, use the input format.

        Returns:
            str: A formatted time string.
        """
        if r_fmt is False:
            r_fmt = self.fmt

        current_time = datetime.datetime.strptime(data_str, self.fmt)
        target_timezone = ZoneInfo(self.timezone)
        converted_time = current_time.replace(tzinfo=target_timezone)

        return converted_time.strftime(r_fmt)

    def converted_time(self):
        """
        Specify timezone and format, return the current time.

        Returns:
            str: A string representing the current time in the specified format and timezone.
        """

        target_timezone = ZoneInfo(self.timezone)
        converted_time = datetime.datetime.now(target_timezone)

        return converted_time.strftime(self.fmt)


@global_manager.register_fixture(name="fixture_time_processor", scope=Scope.GLOBAL)
def _time_helper():
    """
    Returns:
        TimeHelper: A TimeHelper object.
    """
    yield TimeHelper


@global_manager.register_fixture(name="fixture_ipaddr", scope=Scope.GLOBAL)
def _ip_address():
    """
    Retrieves the IP address of the current machine by establishing a UDP connection
    to the specified IP and port.

    Returns:
        str: The IP address of the current machine.
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((SOCKET.DEFAULT.BIND_IP.value, SOCKET.DEFAULT.BIND_PORT.value))
        ip = s.getsockname()[0]
    except socket.error:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
    finally:
        s.close()
    yield ip


@global_manager.register_fixture(name="fixture_devices_id", scope=Scope.GLOBAL)
def _device_id(fixture_ipaddr, fixture_current_dir):
    """
    Get Devices ID
    """
    ipaddr = fixture_ipaddr
    current_dir = fixture_current_dir
    token_str = f'{ipaddr}.{current_dir}'
    hash_object = hashlib.sha256(token_str.encode('utf-8'))
    hash_bytes = hash_object.digest()
    encoded_hash = base64.b64encode(hash_bytes)
    return encoded_hash.decode('utf-8')
