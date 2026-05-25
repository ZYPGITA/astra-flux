# -*- coding: utf-8 -*-
import abc
import logging
from typing import Any


class Constructor(object):
    """
    Abstract base class defining the fundamental properties and methods
    that all constructors (service and worker constructors) must implement.

    This class serves as a foundational interface for components that need
    to be instantiated and managed by the service framework. It provides
    essential attributes such as name, IP address, version, and logging
    capabilities that are common to all system components.

    All subclasses of Constructor are expected to be used as factory objects
    or configuration templates that can create instances of services or workers.

    Note:
        This is an abstract base class with abstract methods that must be
        implemented by concrete subclasses.
    """

    name: str = None  # unique name identifier for this constructor
    ipaddr: str = None  # IP address associated with this constructor
    version: str = None  # version string for this constructor
    unique_id: str = None  # unique id string for this constructor.

    @property
    def logger(self) -> logging.Logger:
        """
        Get the logger instance for this constructor.

        Returns:
            logging.Logger: A configured logger instance that should be used
                for all logging within the component. This ensures consistent
                logging format, level, and output across all system components.

        """
        return logging.getLogger(self.name)


class ServiceConstructor(abc.ABC, Constructor):
    """
    Abstract base class for constructing service instances (RPC servers).

    This class extends Constructor to provide additional functionality
    specific to services, which typically handle remote procedure calls,
    API requests, and external communications. Services are long-running
    components that expose functionality to other parts of the system or
    external clients.

    ServiceConstructor serves as a factory and configuration template
    for creating service instances. The __call__() method enables the
    class to be used as a callable factory that returns an instance
    of the service.

    Attributes:
        Inherits all attributes from Constructor

    Note:
        Subclasses must implement all abstract methods from both abc.ABC
        and Constructor, plus the service_name() method.
    """

    @property
    def service_name(self):
        """
        Get the specific service name for the component created by this constructor.

        The service name is distinct from the constructor's name and typically
        represents the logical service type or functionality (e.g., 'UserService',
        'PaymentService', 'NotificationService').

        Returns:
            str: The name of the service that will be created by this constructor.
                This name is used for service discovery, routing, and monitoring.
        """
        return

    @classmethod
    def setattr(cls, name: str, value: Any) -> None:
        """
        Dynamically set an attribute on the class.

        This class method provides a convenient way to configure class-level
        attributes dynamically. It can be used to set configuration values,
        register callbacks, or modify behavior without subclassing.

        Args:
            name: The name of the attribute to set
            value: The value to assign to the attribute
        """
        setattr(cls, name, value)

    def __call__(self) -> 'ServiceConstructor':
        """
        Make the constructor instance callable, returning itself.

        This method enables the constructor instance to be used as a
        factory callable. When called, it returns the instance itself,
        which can then be used to create or configure service instances.

        This pattern allows for flexible instantiation patterns, such as:
        - Singleton-like behavior when the constructor is called multiple times
        - Use as a decorator or factory function
        - Lazy initialization of the actual service

        Returns:
            ServiceConstructor: The constructor instance itself
        """
        return self


class WorkerConstructor(abc.ABC, Constructor):
    """
    Abstract base class for constructing worker instances (task processors).

    This class extends Constructor to provide functionality specific to
    workers, which typically execute background tasks, process jobs from
    queues, and perform computational work. Workers are designed to handle
    processing tasks asynchronously and efficiently.

    WorkerConstructor serves as a factory and configuration template for
    creating worker instances. The __call__() method enables the class to
    be used as a callable factory, and the run() method defines the
    core processing logic that workers must implement.

    Attributes:
        Inherits all attributes from Constructor

    Note:
        Subclasses must implement all abstract methods from both abc.ABC
        and Constructor, plus the worker_name() and run() methods.
    """

    @property
    def worker_name(self):
        """
        Get the specific worker name for the component created by this constructor.

        The worker name is distinct from the constructor's name and typically
        represents the type of work or processing capability (e.g., 'DataProcessor',
        'ImageRenderer', 'ReportGenerator').

        Returns:
            str: The name of the worker that will be created by this constructor.
                This name is used for task routing, load balancing, and monitoring.
        """
        return

    def run(self, data: Any) -> Any:
        """
        Execute the core processing logic for the worker.

        This is the main method that defines what the worker does. It takes
        input data, processes it according to the worker's specific logic,
        and returns a result. The exact signature and return type may vary
        by implementation.

        Args:
            data: The input data to be processed. The type and structure
                depend on the specific worker implementation. This could be
                a dictionary, a custom object, a file path, or any other
                data structure.
        """

    @classmethod
    def setattr(cls, name: str, value: Any) -> None:
        """
        Dynamically set an attribute on the class.

        This class method provides a convenient way to configure class-level
        attributes dynamically. It can be used to set configuration values,
        worker-specific parameters, or modify behavior without subclassing.

        Args:
            name: The name of the attribute to set
            value: The value to assign to the attribute
        """
        setattr(cls, name, value)

    def __call__(self) -> 'WorkerConstructor':
        """
        Make the constructor instance callable, returning itself.

        This method enables the constructor instance to be used as a
        factory callable. When called, it returns the instance itself,
        which can then be used to create or configure worker instances.

        This pattern is particularly useful for:
        - Creating worker pools where the same configuration is reused
        - Delaying worker instantiation until needed
        - Using workers as callbacks or handlers in event-driven systems

        Returns:
            WorkerConstructor: The constructor instance itself
        """
        return self
