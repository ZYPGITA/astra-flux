# -*- coding: utf-8 -*-

from astraflux.core import global_manager


def launch_register(services: list):
    """
        Register services to be managed by the framework.

        Args:
            services: List of service classes or modules to register
    """

    def _backcall(fixture_launcher):
        return fixture_launcher.launch_register(services=services)

    return global_manager.bind_fixture_func(_backcall)()


def launch_start(run_app: bool = True, scheduled: bool = True):
    """
    Initialize and launch all registered services with their associated worker components.

        This method orchestrates the startup process for all services registered in the system.
        For each service, it launches two distinct components:
        1. A service component (RPC server) for handling remote procedure calls and API requests
        2. A worker component (task processor) for executing background tasks and job processing

        After launching all service/worker pairs, the method configures and starts essential
        system-level background jobs:
        - TaskScheduler: Distributes tasks across available workers (runs every 10 seconds)
        - SystemMonitoring: Monitors system health and performance (runs every 30 seconds)

        These background jobs run with different execution modes to ensure proper coordination
        in distributed environments.

        Raises:
            RuntimeError: If any service or worker fails to start properly
            ImportError: If required launcher modules cannot be imported
            FileNotFoundError: If service module file cannot be located

        Note:
            - Service and worker components are launched as separate processes
            - Both components share the same service class module but operate in different modes
            - The scheduler uses distributed_unique mode for task scheduling to ensure
              only one scheduler runs across the entire cluster
            - Monitoring runs in ip_unique mode to ensure one monitor per IP address
    """

    def _backcall(fixture_launcher):
        return fixture_launcher.launch_start(run_app=run_app, scheduled=scheduled)

    return global_manager.bind_fixture_func(_backcall)()
