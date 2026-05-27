# -*- encoding: utf-8 -*-

import os
import sys
import argparse


class ServiceComponentLauncher:
    """
    Launcher for RPC service components in the distributed framework.

    Handles the complete lifecycle of service components including:
    - Dynamic class loading and configuration
    - Service registration in the service discovery system
    - RPC server startup and request handling
    """

    def __init__(self, class_path: str):
        """
        Initialize the service launcher.

        Args:
            class_path: Path to the service class definition file
        """
        self.class_path = class_path

    def launch_service(self):
        """
        Launch and register the RPC service component.

        This method:
        1. Builds the service component using ServiceBuilder
        2. Registers the service in the service discovery database
        3. Starts the RPC server to handle incoming requests
        """
        service_builder = ComponentBuilder(
            class_path=self.class_path,
            component_type='service',
            constructor=ServiceConstructor
        )
        service_component = service_builder.build_component()

        service_registration_data = self._prepare_service_data(service_component)
        self._register_service_in_discovery(service_registration_data)

        service_component.logger.info(f'Service component started: {service_registration_data}')

        self._start_rpc_server(service_component)

    @staticmethod
    def _prepare_service_data(service_component) -> dict:
        """
        Prepare service data for registration in discovery system.

        Returns:
            Dictionary containing service metadata for registration
        """
        return {
            BUILD.CONFIG.UNIQUE_ID.value: service_component.unique_id,
            BUILD.CONFIG.NAME.value: service_component.name,
            BUILD.CONFIG.SERVICE_IPADDR.value: service_component.ipaddr,
            BUILD.CONFIG.SERVICE_NAME.value: service_component.service_name,
            BUILD.CONFIG.SERVICE_VERSION.value: service_component.version,
            BUILD.CONFIG.SERVICE_PID.value: os.getpid(),
            BUILD.CONFIG.SERVICE_FUNCTIONS.value: service_component.functions,

            # Initialize worker attributes for service discovery
            BUILD.CONFIG.WORKER_IPADDR.value: service_component.ipaddr,
            BUILD.CONFIG.WORKER_NAME.value: service_component.service_name
        }

    @staticmethod
    def _register_service_in_discovery(service_data: dict):
        """
        Register service in the service discovery database.

        Args:
            service_data: Service metadata for registration
        """
        redis_store_worker_data(data=service_data)

    @staticmethod
    def _start_rpc_server(service_component):
        """
        Start the RPC server to handle remote procedure calls.

        Args:
            service_component: Service component instance to handle RPC requests
        """
        start_consumer(queue_name=service_component.name, service_instance=service_component)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Distributed Service Component Launcher")

    parser.add_argument("--yaml_file", type=str, required=True,
                        help="Path to YAML configuration file")
    parser.add_argument("--class_path", type=str, required=True,
                        help="Path to service class definition file")
    parser.add_argument("--current_dir", type=str, required=True,
                        help="Current working directory")

    args = parser.parse_args()

    from astraflux import AstraFlux
    from astraflux.config.constructor import ServiceConstructor

    af = AstraFlux(yaml_path=args.yaml_file, current_dir=args.current_dir)

    sys.path.append(args.current_dir)

    from astraflux.boot.component_builder import ComponentBuilder
    from astraflux.config.constants import BUILD

    from astraflux.exports import (
        redis_store_worker_data, start_consumer
    )

    ServiceComponentLauncher(class_path=args.class_path).launch_service()
