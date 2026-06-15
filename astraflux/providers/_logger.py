# -*- coding: utf-8 -*-

import os
import logging
import threading
from logging.handlers import RotatingFileHandler

from astraflux.core import global_manager
from astraflux.config.constants import *


class RotatingLogger:
    """
    A thread-safe logger implementation that supports creating loggers for different directories and files.

    This class ensures that loggers are created in a thread-safe manner and cached for reuse.
    It supports log rotation based on file size and outputs logs to both files and console.
    """
    _lock = threading.Lock()
    _loggers = {}
    _base_path = None
    _logger_level = logging.DEBUG

    def __init__(self, base_path, logger_level):
        """
        Initialize the RotatingLogger with base path and log level.

        Args:
            base_path : The base directory where log files will be stored
            logger_level (int): The logging level (e.g., logging.INFO, logging.DEBUG)
        """
        self._base_path = base_path
        self._logger_level = logger_level
        # Ensure the base directory exists
        if not os.path.exists(self._base_path):
            os.makedirs(self._base_path, exist_ok=True)

    def get_logger(self, dirname: str, filename: str, max_bytes: int = DEFAULTS.LOGGER_MAX_BYTES,
                   backup_count: int = DEFAULTS.LOGGER_BACKUP_COUNT) -> logging.Logger:
        """
        Get or create a thread-safe logger for the specified directory and filename.

        This method creates a logger that writes to a file in the specified directory.
        If the logger for the given directory/filename combination already exists,
        it returns the cached instance.

        Args:
            dirname (str): The subdirectory under base path where the log file will be stored.
                          If None, uses the base path directly.
            filename (str): The name of the log file (without extension).
                          If None, uses the project name as the filename.
            max_bytes (int, optional): Maximum size of a log file before rotation.
                                     Defaults to 10MB.
            backup_count (int, optional): Number of backup files to keep. Defaults to 5.

        Returns:
            logging.Logger: A configured logger instance that writes to the specified file
                           and also outputs to console.

        Note:
            The logger is thread-safe and uses a rotating file handler to manage log file sizes.
            Logs are formatted with timestamp, logger name, level, thread name, and message.
        """
        # Determine file path and logger name
        if dirname is None or filename is None:
            file_path = os.path.join(self._base_path, f'{PROJECT.NAME.value}.log')
            logger_name = "default"
        else:
            full_dir_path = os.path.join(self._base_path, dirname)
            file_path = os.path.join(full_dir_path, filename)
            logger_name = f"{dirname}/{filename}"

        with self._lock:
            # Create necessary directories inside the lock to avoid race conditions
            if dirname and filename:
                full_dir_path = os.path.join(self._base_path, dirname)
                if not os.path.exists(full_dir_path):
                    os.makedirs(full_dir_path, exist_ok=True)

            # Check if logger already exists in cache
            if logger_name in self._loggers:
                return self._loggers[logger_name]

            # Create a new logger instance
            logger = logging.getLogger(logger_name)
            logger.setLevel(self._logger_level)

            # Avoid adding handlers multiple times
            if not logger.handlers:
                # Create a rotating file handler for log file management
                file_handler = RotatingFileHandler(
                    file_path,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding='utf-8'
                )
                file_handler.suffix = LOGGER.DEFAULT.SUFFIX.value

                # Create a console handler for terminal output
                console_handler = logging.StreamHandler()

                # Set log message format
                formatter = logging.Formatter(LOGGER.DEFAULT.FMT.value)
                file_handler.setFormatter(formatter)
                console_handler.setFormatter(formatter)

                # Add handlers to the logger
                logger.addHandler(file_handler)
                logger.addHandler(console_handler)

                # Prevent log messages from being propagated to the root logger
                logger.propagate = False

                # Cache the logger for future use
                self._loggers[logger_name] = logger

            return logger


@global_manager.register_fixture(name="fixture_logger", scope=Scope.GLOBAL)
def _rotating_logger(fixture_config):
    """
    Factory function to create and register a RotatingLogger instance as a global fixture.

    This function is registered as a fixture in the global manager and provides
    a RotatingLogger instance configured based on the application configuration.

    Args:
        fixture_config (dict): Configuration dictionary containing:
            - PROJECT.CURRENT_DIR.value: Current working directory
            - LOGGER.CONFIG.KEY.value: Dictionary containing logger configuration
                - LOGGER.CONFIG.PATH.value: Relative path for log files
                - LOGGER.CONFIG.LEVEL.value: Logging level as string

    Yields:
        RotatingLogger: A configured thread-safe logger instance

    Note:
        This fixture follows the dependency injection pattern and is managed by
        the global_manager. The logger instance is created once and reused throughout
        the application lifecycle.
    """
    current_dir = fixture_config[PROJECT.CURRENT_DIR.value]
    logger_path = fixture_config[LOGGER.CONFIG.KEY.value][LOGGER.CONFIG.PATH.value]
    logger_level = fixture_config[LOGGER.CONFIG.KEY.value][LOGGER.CONFIG.LEVEL.value]

    # Construct the full base path for log files
    base_path = os.path.join(current_dir, logger_path)

    # Convert string log level to logging constant
    logger_level = logging.getLevelName(logger_level)

    # Create and yield the thread-safe logger instance
    logger = RotatingLogger(
        base_path=base_path,
        logger_level=logger_level
    )

    yield logger
