# -*- encoding: utf-8 -*-

import time
from astraflux.core import global_manager
from astraflux.config.constants import *


class SnowflakeGenerator:
    """
    A singleton class that generates unique IDs based on the Snowflake algorithm.
    This algorithm ensures that the generated IDs are unique across different machines and time.
    """

    _instance = None
    __initialized = False

    def __new__(cls, *args, **kwargs):
        """
        Overrides the __new__ method to implement the singleton pattern.
        Ensures that only one instance of the Snowflake class is created.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Snowflake: The singleton instance of the Snowflake class.
        """

        if not cls._instance:
            cls._instance = super(SnowflakeGenerator, cls).__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, ipaddress, datacenter_id: int = None, machine_id: int = None, sequence: int = 0):
        """
        Initializes the Snowflake ID generator.

        Args:
            ipaddress (str): The IP address of the Snowflake.
            datacenter_id (int, optional): The ID of the data center. Defaults to None.
            machine_id (int, optional): The ID of the machine. Defaults to None.
            sequence (int, optional): The initial sequence number. Defaults to 0.
        """
        if self.__initialized:
            return

        self.__initialized = True
        self.ipaddress = ipaddress

        self.start_timestamp = 1288834974657

        self.datacenter_id_bits = 5
        self.machine_id_bits = 5
        self.sequence_bits = 12

        self.max_datacenter_id = (1 << self.datacenter_id_bits) - 1
        self.max_machine_id = (1 << self.machine_id_bits) - 1
        self.max_sequence = (1 << self.sequence_bits) - 1

        self.machine_id_shift = self.sequence_bits
        self.datacenter_id_shift = self.sequence_bits + self.machine_id_bits
        self.timestamp_left_shift = self.sequence_bits + self.machine_id_bits + self.datacenter_id_bits

        if datacenter_id is not None:
            self.datacenter_id = datacenter_id
        else:
            self.datacenter_id = int(self.ipaddress.split('.')[-2])

        if machine_id is not None:
            self.machine_id = machine_id
        else:
            self.machine_id: int = int(self.ipaddress.split('.')[-1])

        self.sequence = sequence
        self.last_timestamp = -1

    @staticmethod
    def _current_timestamp():
        """
        Returns the current timestamp in milliseconds.

        Returns:
            int: The current timestamp in milliseconds.
        """
        return int(time.time() * 1000)

    def _till_next_millis(self, last_timestamp):
        """
        Waits until the next millisecond to ensure the timestamp is greater than the last one.

        Args:
            last_timestamp (int): The last timestamp used to generate an ID.

        Returns:
            int: The new timestamp.
        """
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    @property
    def generate_id(self) -> str:
        """
        Generates a Snowflake ID using the Snowflake algorithm.

        Handles clock rollbacks gracefully:
        - Small rollback (<100ms): waits for clock to catch up
        - Large rollback (>=100ms): regenerates start_timestamp to avoid duplicates

        Returns:
            str: A Snowflake ID.
        """
        timestamp = self._current_timestamp()
        rollback_ms = self.last_timestamp - timestamp

        if rollback_ms > 0:
            if rollback_ms <= 100:
                # Small clock rollback — wait until we reach last_timestamp
                import time as _time
                _time.sleep(rollback_ms / 1000.0 + 0.001)
                timestamp = self._current_timestamp()
            else:
                # Large clock rollback — reset epoch to avoid duplicate IDs
                self.start_timestamp = timestamp
                self.last_timestamp = timestamp
                self.sequence = 0
                # Log the event
                import logging
                logging.getLogger("SnowflakeGenerator").warning(
                    f"Clock rolled back {rollback_ms}ms; reset ID epoch to current time"
                )

        if self.last_timestamp == timestamp:
            self.sequence = (self.sequence + 1) & self.max_sequence
            if self.sequence == 0:
                timestamp = self._till_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        _id = ((timestamp - self.start_timestamp) << self.timestamp_left_shift) | \
              (self.datacenter_id << self.datacenter_id_shift) | \
              (self.machine_id << self.machine_id_shift) | self.sequence

        return str(_id)


class IdGenerator:
    """
    A class that generates unique IDs based on the Snowflake algorithm.
    """

    def __init__(self, ipaddress: str):
        self.ipaddress = ipaddress

    def snowflake_id(self) -> str:
        """
        Generates a Snowflake ID using the Snowflake algorithm.

        Returns:
            str: A Snowflake ID.
        """
        return SnowflakeGenerator(self.ipaddress).generate_id


@global_manager.register_fixture(name="fixture_generate_id", scope=Scope.GLOBAL)
def _id_generator(fixture_ipaddr):
    """
    A fixture that generates unique IDs based on the Snowflake algorithm.
    """
    yield IdGenerator(fixture_ipaddr)
