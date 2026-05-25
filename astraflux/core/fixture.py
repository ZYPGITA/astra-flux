# -*- coding: utf-8 -*-

import inspect
import threading
from functools import wraps
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Optional, Tuple, TypeVar, Generic

from astraflux.config.constants import Scope

__all__ = ["FixtureManager"]

T = TypeVar("T")


class FixtureDef(Generic[T]):
    """
    A class to represent a fixture definition.

    Attributes:
        name (str): The name of the fixture.
        func (Callable[..., T | Generator[T, None, None]]): The function that
            produces the fixture value.
        scope (Scope): The scope of the fixture.
        cached_result (Optional[Tuple[T, Any]]): The cached result of the fixture
            if it has been computed.
        finalizers (list[Callable[[], None]]): A list of finalizer functions to
            be called when the fixture is cleared.
    """

    def __init__(
            self,
            name: str,
            func: Callable[..., T | Generator[T, None, None]],
            scope: Scope,
    ):
        self.name = name
        self.func = func
        self.scope = scope
        self.cached_result: Optional[Tuple[T, Any]] = None
        self.finalizers: list[Callable[[], None]] = []
        self._lock = threading.RLock()

    def add_finalizer(self, finalizer: Callable[[], None]) -> None:
        """Add a finalizer function to be called when the fixture is cleared."""
        if not callable(finalizer):
            raise TypeError("Finalizer must be a callable object")

        with self._lock:
            self.finalizers.append(finalizer)

    def clear_cache(self) -> None:
        """Clear the cached result and call all finalizers."""
        with self._lock:
            for finalizer in reversed(self.finalizers):
                try:
                    finalizer()
                except Exception as e:
                    print(f"Error in finalizer for {self.name}: {e}")

            self.cached_result = None
            self.finalizers.clear()

    def get_cached_result(self, cache_key: Any) -> Optional[T]:
        """Get the cached result if it exists and matches the cache key."""
        with self._lock:
            if (
                    self.cached_result is not None
                    and self.cached_result[1] == cache_key
            ):
                return self.cached_result[0]
            return None

    def set_cached_result(self, result: T, cache_key: Any) -> None:
        """Set the cached result for the fixture with the given cache key."""
        with self._lock:
            self.cached_result = (result, cache_key)


class ThreadLocalSession:
    """
    A class to manage thread-local sessions for fixture caching.

    This class provides a way to create and manage unique cache keys for each
    thread, ensuring that fixtures are cached and cleared appropriately
    within the scope of each thread.
    """

    def __init__(self):
        self._local = threading.local()
        self._ensure_cache_key()

    def _ensure_cache_key(self):
        """Ensure that each thread has a unique cache key."""
        if not hasattr(self._local, 'cache_key'):
            self._local.cache_key = object()

    @property
    def cache_key(self) -> Any:
        """Get the cache key for the current thread."""
        self._ensure_cache_key()
        return self._local.cache_key

    def start_session(self) -> None:
        """Start a new session for the current thread, generating a new cache key."""
        self._local.cache_key = object()

    @staticmethod
    def is_active() -> bool:
        """Check if a session is active for the current thread."""
        return True


class FixtureManager:
    """
    A class to manage fixtures and their caching.

    This class provides methods to register fixtures, execute them with
    appropriate scoping, and manage thread-local sessions for caching.
    """

    def __init__(self):
        self._fixtures: Dict[str, FixtureDef[Any]] = {}
        self._global_cache_key: Any = object()
        self._lock = threading.RLock()
        self._thread_local = ThreadLocalSession()
        self._shutdown = False

        self.start_global_session()

    def register_fixture(
            self,
            name: str,
            scope: Scope = Scope.GLOBAL,
    ) -> Callable[[Callable[..., T | Generator[T, None, None]]], Callable[..., T | Generator[T, None, None]]]:
        """
        Register a fixture with the specified name and scope.

        Args:
            name (str): The name of the fixture.
            scope (Scope, optional): The scope of the fixture. Defaults to Scope.GLOBAL.

        Returns:
            Callable[[Callable[..., T | Generator[T, None, None]]], Callable[..., T | Generator[T, None, None]]]:
                A decorator function that registers the fixture and returns the original function.
        """

        def decorator(func: Callable[..., T | Generator[T, None, None]]) -> Callable[..., T | Generator[T, None, None]]:
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("FixtureManager has been shutdown")

                if name in self._fixtures:
                    raise ValueError(f"Fixture '{name}' already registered")

                fixture_def = FixtureDef(name, func, scope)
                self._fixtures[name] = fixture_def
            return func

        return decorator

    def get_fixture(self, name: str) -> T:
        """
        Get the fixture with the specified name.

        Args:
            name (str): The name of the fixture.

        Returns:
            T: The instance of the fixture.

        Raises:
            RuntimeError: If the FixtureManager has been shutdown.
            LookupError: If the fixture with the specified name is not found.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("FixtureManager has been shutdown")

            if name not in self._fixtures:
                raise LookupError(f"Fixture '{name}' not found")

            fixture_def = self._fixtures[name]
            cache_key = self._get_cache_key(fixture_def)

            cached_result = fixture_def.get_cached_result(cache_key)
            if cached_result is not None:
                return cached_result

            result = self._execute_fixture(fixture_def)
            fixture_def.set_cached_result(result, cache_key)
            return result

    @contextmanager
    def session_context(self) -> Generator[None, None, None]:
        """
        Create a context manager for a new session.

        This context manager starts a new session for the current thread,
        generating a new cache key. It ensures that fixtures with a scope of
        THREAD are cached and cleared appropriately within the session.

        Yields:
            None: This context manager does not yield any value.
        """

        self._thread_local.start_session()
        try:
            yield
        finally:
            self.clear_thread_fixtures()

    def start_global_session(self) -> None:
        """
        Start a new global session.

        This method generates a new cache key for the global scope,
        effectively resetting the cache for all fixtures with a scope of
        GLOBAL. It should be called at the beginning of each test run to
        ensure that fixtures are re-created and cached appropriately.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("FixtureManager has been shutdown")

            self._global_cache_key = object()

            for fixture in self._fixtures.values():
                if fixture.scope == Scope.GLOBAL:
                    fixture.clear_cache()

    def clear_thread_fixtures(self) -> None:
        """
        Clear the cache for all fixtures with a scope of THREAD.

        This method should be called at the end of each test run to ensure
        that fixtures with a scope of THREAD are properly cleaned up and
        cached for the next test run.
        """
        with self._lock:
            for fixture in self._fixtures.values():
                if fixture.scope == Scope.THREAD:
                    fixture.clear_cache()

    def shutdown(self) -> None:
        """
        Shutdown the FixtureManager.

        This method shuts down the FixtureManager, releasing any resources
        and ensuring that no further fixtures can be registered or retrieved.
        """
        with self._lock:
            if self._shutdown:
                return

            self._shutdown = True

            for fixture in self._fixtures.values():
                fixture.clear_cache()

    def is_active(self) -> bool:
        """
        Check if the FixtureManager is active.

        Returns:
            bool: True if the FixtureManager is active, False if it has been shutdown.
        """
        with self._lock:
            return not self._shutdown

    def get_fixture_names(self) -> list[str]:
        """
        Get a list of all registered fixture names.

        Returns:
            list[str]: A list of fixture names.
        """
        with self._lock:
            return list(self._fixtures.keys())

    def _get_cache_key(self, fixture_def: FixtureDef[Any]) -> Any:
        """
        Get the cache key for the specified fixture definition.

        Args:
            fixture_def (FixtureDef[Any]): The fixture definition.

        Returns:
            Any: The cache key for the fixture definition.

        Raises:
            ValueError: If the fixture definition has an unsupported scope.
        """

        if fixture_def.scope == Scope.SINGLETON:
            return "singleton"
        elif fixture_def.scope == Scope.GLOBAL:
            return self._global_cache_key
        elif fixture_def.scope == Scope.THREAD:
            return self._thread_local.cache_key
        else:
            raise ValueError(f"Unsupported scope: {fixture_def.scope}")

    def _execute_fixture(self, fixture_def: FixtureDef[T]) -> T:
        """
        Execute the fixture function with the specified dependencies.

        Args:
            fixture_def (FixtureDef[T]): The fixture definition.

        Returns:
            T: The result of executing the fixture function.

        Raises:
            ValueError: If the fixture function is a generator function and
                       does not yield a value.
            RuntimeError: If the fixture function is a generator function and
                          yields more than one value.
        """
        func = fixture_def.func
        params = inspect.signature(func).parameters

        dependencies = {}
        for param in params:
            dependencies[param] = self.get_fixture(param)

        result = func(**dependencies)

        if inspect.isgeneratorfunction(func):
            generator = result
            try:
                fixture_result = next(generator)
            except StopIteration:
                raise ValueError(f"Fixture '{fixture_def.name}' did not yield a value")

            def finalizer():
                try:
                    next(generator)
                except StopIteration:
                    pass
                else:
                    raise RuntimeError(f"Fixture '{fixture_def.name}' has more than one yield")

            fixture_def.add_finalizer(finalizer)
            return fixture_result
        else:
            return result

    def bind_fixture_func(self, func: Callable[..., T]) -> Callable[..., T]:
        """
        Bind fixture dependencies to the specified function.

        This method wraps the given function and injects fixture dependencies
        into it. Fixture dependencies are resolved using the FixtureManager's
        dependency injection system.

        Args:
            func (Callable[..., T]): The function to bind fixture dependencies to.

        Returns:
            Callable[..., T]: The wrapped function with injected fixture dependencies.
        """
        sig = inspect.signature(func)
        fixture_params = [p for p in sig.parameters if p in self._fixtures]
        user_params = [p for p in sig.parameters if p not in fixture_params]

        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            if self._shutdown:
                raise RuntimeError("FixtureManager has been shutdown")

            bound_args = sig.bind_partial(*args, **kwargs)
            bound_args.apply_defaults()
            user_kwargs = {k: v for k, v in bound_args.arguments.items() if k in user_params}

            fixture_kwargs = {p: self.get_fixture(p) for p in fixture_params}

            return func(**{**fixture_kwargs, **user_kwargs})

        return wrapped
