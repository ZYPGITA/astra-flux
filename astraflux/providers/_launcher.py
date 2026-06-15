# -*- encoding: utf-8 -*-

import sys
import os
import signal
import psutil
import logging
import subprocess
from pathlib import Path
from typing import List, Callable

from astraflux.core import global_manager
from astraflux.config.constants import *


class ServiceLauncher:

    def __init__(self, config: dict, schedule, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.schedule = schedule

        self.yaml_file = config[PROJECT.CONFIG_PATH.value]
        self.current_dir = config[PROJECT.CURRENT_DIR.value]

        self.python_process_name = 'python'
        self.pyexe = sys.executable if 'python' in sys.executable else 'python3'

        self.services = []
        self.run_process = []

    def _terminate_existing_process(self, script_path: Path, target_path: Path):
        """
        Terminate existing processes running the same script to prevent duplicates.

        This method identifies and kills processes that are running the same
        service or worker script to ensure clean startup without conflicts.

        Args:
            script_path: Path to the framework launcher script
            target_path: Path to the target service/worker class file

        Note:
            Uses psutil to safely identify and terminate matching processes.
            Excludes the current process to avoid self-termination.
        """
        current_pid = os.getpid()
        for process in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Skip the current process to avoid self-kill
                if process.info.get('pid') == current_pid:
                    continue

                process_name = process.name()
                command_line = process.cmdline() if process.info.get('cmdline') else []

                # Check if this is a Python process
                if self.python_process_name in process_name:
                    script_running = False
                    target_running = False

                    # Check command line arguments for script and target paths
                    for argument in command_line:
                        argument_path = Path(argument).as_posix()
                        if script_path.as_posix() in argument_path:
                            script_running = True
                        if target_path.as_posix() in argument_path:
                            target_running = True

                    # Terminate if both script and target are running
                    if script_running and target_running:
                        process.kill()
                        # Wait for process to fully exit
                        try:
                            process.wait(timeout=DEFAULTS.PROCESS_WAIT_TIMEOUT)
                        except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                            pass
                        break

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                # Continue with other processes if current one is inaccessible
                continue

    def _launch_service_component(self, launcher_module, class_path: Path | str) -> int:
        """
        Launch a service or worker component as a separate process.

        Args:
            launcher_module: Type of component to launch ('service' or 'worker')
            class_path: Path to the service/worker class file

        Returns:
            Process ID of the launched component

        Raises:
            ImportError: If the required launcher module cannot be imported
            subprocess.SubprocessError: If process launch fails
        """
        launcher_script = Path(launcher_module.__file__).resolve()

        # Ensure no existing processes are running
        self._terminate_existing_process(launcher_script, class_path)

        # Build command for subprocess execution
        command = [
            self.pyexe,
            launcher_script,
            '--yaml_file', self.yaml_file,
            '--class_path', class_path.as_posix(),
            '--current_dir', self.current_dir
        ]

        # Launch process and return PID
        process = subprocess.Popen(command)
        self.run_process.append(process.pid)
        return process.pid

    def launch_register(self, services: List[Callable]):
        """
        Register services to be managed by the framework.

        Args:
            services: List of service classes or modules to register

        """
        self.services.extend(services)

    def launch_start(self, run_app: bool = True, scheduled: bool = True):
        """
        Initialize and launch all registered services with their associated worker components.

        This method orchestrates the startup process for all services registered in the system.
        For each service, it launches two distinct components:
        1. A service component (RPC server) for handling remote procedure calls and API requests
        2. A worker component (task processor) for executing background tasks and job processing

        After launching all service/worker pairs, the method configures and starts essential
        system-level background jobs:
        - TaskScheduler: Distributes tasks across available workers (runs every 10 seconds)
        - Systemsystem_monitor: Monitors system health and performance (runs every 30 seconds)

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
            - system_monitor runs in ip_unique mode to ensure one monitor per IP address

        """
        from astraflux.boot import service_launcher
        from astraflux.boot import worker_launcher

        for service in self.services:
            # Resolve absolute path to the service module file
            service_class_path = Path(service.__file__).resolve()

            # Launch service component (RPC server)
            service_pid = self._launch_service_component(service_launcher, service_class_path)
            self.logger.debug(f"Service started with PID: {service_pid}")

            # Launch worker component (task processor)
            worker_pid = self._launch_service_component(worker_launcher, service_class_path)
            self.logger.debug(f"Worker started with PID: {worker_pid}")

        if scheduled:
            # Import system-level workflow components
            from astraflux.controllers.task_scheduler import TaskScheduler
            from astraflux.controllers.system_monitor import SystemMonitoring

            # Configure and schedule the TaskScheduler job
            # This job runs every 10 seconds and is responsible for distributing tasks
            # across available workers in a distributed environment
            self.schedule.add_scheduled_job(
                job_id='TaskScheduler',
                cron_expression='*/10 * * * * *',  # Every 10 seconds
                execution_type='thread',  # Execute in a separate thread
                function=TaskScheduler().execute,  # Function to execute
                execution_mode='distributed_unique'  # Ensure only one instance runs in the cluster
            )

            # Configure and schedule the Systemsystem_monitor job
            # This job runs every 30 seconds and monitors system health and performance
            self.schedule.add_scheduled_job(
                job_id='SystemMonitoring',
                cron_expression='*/30 * * * * *',  # Every 30 seconds
                execution_type='thread',  # Execute in a separate thread
                function=SystemMonitoring().run,  # Function to execute
                execution_mode='ip_unique'  # Ensure one instance per IP address
            )

            # Start the scheduler to begin executing scheduled jobs
            self.schedule.start_scheduler()

        if run_app:
            from astraflux.web_ui import web_app
            app_class_path = Path(web_app.__file__).resolve()
            pid = self._launch_service_component(web_app, app_class_path)
            self.logger.debug(f"Web APP started with PID: {pid}")

    def kill(self):
        """
        Terminate all running service and worker processes.

        Uses taskkill on Windows, SIGKILL on Unix.
        Each process is killed individually with proper error handling.
        Operates on a snapshot of the process list to avoid modification during iteration.
        """
        processes = list(self.run_process)
        self.run_process.clear()

        for pid in processes:
            try:
                if os.name == 'nt':
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(pid)],
                        capture_output=True, timeout=DEFAULTS.PROCESS_TASKKILL_TIMEOUT, check=False
                    )
                else:
                    os.kill(pid, signal.SIGKILL)
                self.logger.debug(f"Killed process {pid}")
            except ProcessLookupError:
                self.logger.debug(f"Process {pid} already exited")
            except Exception as e:
                self.logger.warning(f"Failed to kill process {pid}: {e}")

        self.run_process.clear()


@global_manager.register_fixture(name="fixture_launcher", scope=Scope.GLOBAL)
def _service_launcher(fixture_config, fixture_logger, fixture_schedule):
    """Register LauncherManager fixture"""
    _config = fixture_config
    _logger = fixture_logger.get_logger(PROJECT.NAME.value, 'launcher_manager')

    _launcher_manager = ServiceLauncher(
        config=_config,
        schedule=fixture_schedule,
        logger=_logger,
    )

    yield _launcher_manager

    _launcher_manager.kill()
