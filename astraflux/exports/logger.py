# -*- coding: utf-8 -*-
from astraflux.core import global_manager


def logger(dirname=None, filename=None):
    """
    Get a thread-safe logger instance for the specified directory and filename.

    This function provides a convenient interface to obtain a configured logger
    by leveraging the dependency injection system. It binds the logger fixture
    and returns a logger instance configured for the specified path.

    Args:
        dirname (str, optional): The subdirectory under the base log path where
                                the log file will be stored. If None, logs will
                                be stored directly in the base log directory.
        filename (str, optional): The name of the log file (without extension).
                                 If None, a default filename will be used.

    Returns:
        logging.Logger: A configured thread-safe logger instance that writes
                       to the specified file and outputs to console.

    Example:
        >>> # Get a logger for application logs
        >>> app_logger = logger("application", "main")
        >>> app_logger.info("Application started")

        >>> # Get a logger for database operations
        >>> db_logger = logger("database", "queries")
        >>> db_logger.debug("Executing SQL query")

        >>> # Get a default logger
        >>> default_logger = logger()
        >>> default_logger.warning("This is a warning message")

    Note:
        This function uses the global dependency injection system to resolve
        the logger fixture. The actual logger instance is managed by the
        ThreadSafeLogger class which ensures thread safety and proper
        log rotation.
    """

    def _backcall(fixture_logger):
        """
        Internal callback function that retrieves the actual logger instance.

        This function is bound to the fixture system and is called with the
        configured ThreadSafeLogger instance when the dependency is resolved.

        Args:
            fixture_logger (ThreadSafeLogger): The logger fixture instance
                                             provided by the dependency
                                             injection system.

        Returns:
            logging.Logger: The logger instance for the specified directory
                          and filename.
        """
        return fixture_logger.get_logger(dirname, filename)

    return global_manager.bind_fixture_func(_backcall)()
