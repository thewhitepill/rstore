from __future__ import annotations

import asyncio
import inspect

from asyncio import Lock, Task

from inspect import signature
from typing import (
    Awaitable,
    Callable,
    Generic,
    Optional,
    Type,
    TypeAlias,
    TypeVar,
    Union
)

from uuid import UUID, uuid4

from pydantic import BaseModel, field_validator, field_serializer
from redis.asyncio import Redis, WatchError
from redis.asyncio.client import PubSub


__all__ = (
    "ConcurrencyError",
    "Dispatch",
    "InvalidStateError",
    "Middleware",
    "RedisNamespaceFactory",
    "Reducer",
    "StateFactory",
    "Store",
    "StoreError",
    "Subscriber",
    "Unsubscribe",

    "create_store",
    "default_redis_namespace"
)


A = TypeVar("A", bound=BaseModel)
S = TypeVar("S", bound=BaseModel)


class InvalidStateError(Exception):
    pass


class StoreError(Exception):
    pass


class ConcurrencyError(StoreError):
    pass


Dispatch = Callable[[A], Awaitable[S]]
Reducer = Callable[[S, A], S]
Subscriber = Callable[[Optional[A], Optional[S]], None]
Unsubscribe = Callable[[], None]


class Store(Generic[S, A]):
    state_type: type[S]
    action_type: type[A]

    async def bind(self, client: Redis) -> None:
        raise NotImplementedError

    async def unbind(self) -> None:
        raise NotImplementedError

    async def dispatch(self, action: A) -> S:
        raise NotImplementedError

    async def get_state(self) -> S:
        raise NotImplementedError

    def subscribe(self, subscriber: Subscriber) -> Unsubscribe:
        raise NotImplementedError


Middleware = Callable[[Store[S, A], Dispatch, A], S]


def _apply_middlware(
    middleware: list[Middleware]
) -> Callable[[Store[S, A]], Store[S, A]]:
    def apply(original_store: Store[S, A]) -> Store[S, A]:
        class EnhancedStore(Store[S, A]):
            def __init__(self) -> None:
                self.state_type = original_store.state_type
                self.action_type = original_store.action_type

            async def bind(self, client: Redis) -> None:
                await original_store.bind(client)

            async def unbind(self) -> None:
                await original_store.unbind()

            async def get_state(self) -> S:
                return await original_store.get_state()

            def subscribe(self, subscriber: Subscriber) -> Unsubscribe:
                return original_store.subscribe(subscriber)

        enhanced_store = \
            EnhancedStore[original_store.state_type, original_store.action_type]() # type: ignore[misc]

        enhanced_dispatch = original_store.dispatch

        for callable in reversed(middleware):
            enhanced_dispatch = lambda action: callable( # noqa: E731
                enhanced_store,
                enhanced_dispatch,
                action
            )

        setattr(enhanced_store, "dispatch", enhanced_dispatch)

        return enhanced_store

    return apply


def _get_model_generic_args(model_type: type[BaseModel]) -> tuple[type, ...]:
    return model_type.__pydantic_generic_metadata__["args"]


def _get_reducer_generic_args(reducer: Reducer) -> tuple[Type[S], Type[A]]:
    state_type, action_type = map(
        lambda p: p.annotation,
        signature(reducer).parameters.values()
    )

    if inspect._empty in (state_type, action_type):
        raise TypeError("Reducer must have type annotations")

    return state_type, action_type


class _ActionContainer(BaseModel, Generic[A]):
    previous_version: UUID
    updated_version: UUID
    action: A

    @staticmethod
    def channel_name(namespace: str) -> str:
        return f"{namespace}:actions"


class _StateContainer(BaseModel, Generic[S]):
    version: UUID
    state: S

    @field_serializer("version")
    @classmethod
    def serialize_version(cls, value: UUID) -> str:
        return str(value)

    @field_serializer("state")
    @classmethod
    def serialize_state(cls, value: S) -> str:
        return value.model_dump_json()

    @field_validator("version", mode="before")
    @classmethod
    def validate_version(cls, value: Union[UUID, str]) -> UUID:
        if isinstance(value, UUID):
            return value

        return UUID(value)

    @field_validator("state", mode="before")
    @classmethod
    def validate_state(cls, value: Union[S, str]) -> S:
        state_model: type[S]
        state_model = _get_model_generic_args(cls)[0]

        if isinstance(value, state_model):
            return value

        assert isinstance(value, str)

        return state_model.model_validate_json(value)

    @staticmethod
    def version_key(namespace: str) -> str:
        return f"{namespace}:version"

    @staticmethod
    def state_key(namespace: str) -> str:
        return f"{namespace}:state"


async def _get_state_container(
    state_type: TypeAlias,
    client: Redis,
    namespace: str
) -> Optional[_StateContainer[S]]:
    if not await client.exists(_StateContainer.version_key(namespace)):
        return None

    version, state = await client.mget(
        _StateContainer.version_key(namespace),
        _StateContainer.state_key(namespace)
    )

    return _StateContainer[state_type](version=version, state=state)


async def _set_state_container(
    container: _StateContainer[S],
    current_local_version: Optional[UUID],
    client: Redis,
    namespace: str
) -> None:
    async with client.pipeline(transaction=True) as pipe:
        await pipe.watch(_StateContainer.version_key(namespace))

        if current_local_version:
            current_remote_version = _StateContainer.validate_version(  # type: ignore[call-arg]
                await pipe.get(_StateContainer.version_key(namespace))
            )

            if current_remote_version != current_local_version:
                raise ConcurrencyError

        await pipe.multi()

        data = container.model_dump()

        await pipe.mset(
            {
                _StateContainer.version_key(namespace): data["version"],
                _StateContainer.state_key(namespace): data["state"]
            }
        )

        try:
            await pipe.execute()
        except WatchError:
            raise ConcurrencyError


def default_redis_namespace(store: Store[S, A]) -> str:
    return f"rstore:{store.state_type.__qualname__}"


StateFactory = Callable[[], S]
RedisNamespaceFactory = Callable[[Store[S, A]], str]


class _DefaultStore(Store[S, A]):
    _reducer: Reducer
    _initial_state_factory: StateFactory

    _version: Optional[UUID]
    _state: Optional[S]

    _subscribers: set[Subscriber]

    _lock: Lock

    _redis_client: Optional[Redis]
    _redis_namespace: Optional[str]
    _redis_namespace_factory: RedisNamespaceFactory
    _redis_pubsub_task: Optional[Task]

    def __init__(
        self,
        state_type: Type[S],
        action_type: Type[A],
        reducer: Reducer,
        initial_state_factory: StateFactory,
        redis_namespace_factory: RedisNamespaceFactory
    ) -> None:
        self.state_type = state_type
        self.action_type = action_type

        self._reducer = reducer
        self._initial_state_factory = initial_state_factory

        self._version = None
        self._state = None

        self._subscribers = set()

        self._lock = Lock()

        self._redis_client = None
        self._redis_namespace = None
        self._redis_namespace_factory = redis_namespace_factory
        self._redis_pubsub_task = None

    def _notify(self, action: Optional[A], state: Optional[S]) -> None:
        for subscriber in self._subscribers:
            subscriber(action, state)

    async def _pubsub_handler(self, pubsub: PubSub) -> None:
        assert self._state is not None

        assert self._redis_client is not None
        assert self._redis_namespace is not None

        while True:
            message = await pubsub.get_message()

            if message is None:
                continue

            data = message["data"]
            action_type = self.action_type
            action_container: _ActionContainer[A] = \
                _ActionContainer[action_type].model_validate_json(data) # type: ignore[valid-type]

            async with self._lock:
                is_fresh = self._version == action_container.previous_version

                if not is_fresh:
                    state_container: Optional[_StateContainer[S]] = \
                        await _get_state_container(
                            self.state_type,
                            self._redis_client,
                            self._redis_namespace
                        )

                    assert state_container is not None

                    self._version = state_container.version
                    self._state = state_container.state

                    return

                action = action_container.action
                self._state = self._reducer(self._state, action)
                self._version = action_container.updated_version
                self._notify(action, self._state)

    async def bind(self, client: Redis) -> None:
        if self._state is not None:
            raise InvalidStateError

        state_type = self.state_type

        async with self._lock:
            self._redis_client = client
            self._redis_namespace = self._redis_namespace_factory(self)

            state_container: Optional[_StateContainer[S]] = \
                await _get_state_container(
                    self.state_type,
                    self._redis_client,
                    self._redis_namespace
                )

            if state_container is None:
                self._version = uuid4()
                self._state = self._initial_state_factory()

                state_container = _StateContainer[state_type]( # type: ignore[valid-type]
                    version=self._version,
                    state=self._state
                )

                await _set_state_container(
                    state_container,
                    None,
                    self._redis_client,
                    self._redis_namespace
                )
            else:
                self._version = state_container.version
                self._state = state_container.state

            async with self._redis_client.pubsub() as pubsub:
                await pubsub.subscribe(
                    _ActionContainer.channel_name(self._redis_namespace),
                    ignore_subscribe_messages=True
                )

                self._redis_pubsub_task = asyncio.create_task(
                    self._pubsub_handler(pubsub)
                )

            self._notify(None, self._state)

    async def unbind(self) -> None:
        if self._state is None:
            raise InvalidStateError

        assert self._redis_pubsub_task is not None

        async with self._lock:
            self._redis_pubsub_task.cancel()

            self._version = None
            self._state = None

            self._redis_client = None
            self._redis_namespace = None
            self._redis_pubsub_task = None

        self._notify(None, None)

    async def dispatch(self, action: A) -> S:
        if not self._state:
            raise InvalidStateError

        assert self._version is not None

        assert self._redis_client is not None
        assert self._redis_namespace is not None

        state_type = self.state_type
        action_type = self.action_type
        state_container: Optional[_StateContainer[S]]

        async with self._lock:
            previous_version = self._version

            self._state = self._reducer(self._state, action)
            self._version = uuid4()
            state_container = _StateContainer[state_type]( # type: ignore[valid-type]
                version=self._version,
                state=self._state
            )

            try:
                await _set_state_container(
                    state_container,
                    previous_version,
                    self._redis_client,
                    self._redis_namespace
                )
            except ConcurrencyError:
                state_container = await _get_state_container(
                    state_type,
                    self._redis_client,
                    self._redis_namespace
                )

                assert state_container is not None

                self._version = state_container.version
                self._state = state_container.state

                raise

            action_container = _ActionContainer[action_type]( # type: ignore[valid-type]
                previous_version=previous_version,
                updated_version=self._version,
                action=action
            )

            await self._redis_client.publish(
                _ActionContainer.channel_name(self._redis_namespace),
                action_container.json()
            )

            self._notify(action, self._state)

            assert self._state is not None

            return self._state

    async def get_state(self) -> S:
        if self._state is None:
            raise InvalidStateError

        async with self._lock:
            return self._state

    def subscribe(self, subscriber: Subscriber[A, S]) -> Callable[[], None]:
        self._subscribers.add(subscriber)

        def unsubscribe() -> None:
            self._subscribers.remove(subscriber)

        return unsubscribe


def create_store(
    reducer: Reducer,
    initial_state_factory: Optional[StateFactory] = None,
    redis_namespace_factory: RedisNamespaceFactory = default_redis_namespace,
    middleware: list[Middleware] = []
) -> Store[S, A]:
    state_type: type[S]
    action_type: type[A]

    state_type, action_type = _get_reducer_generic_args(reducer)

    store = _DefaultStore[state_type, action_type]( # type: ignore[valid-type]
        state_type,
        action_type,
        reducer,
        initial_state_factory or state_type,
        redis_namespace_factory
    )

    if not middleware:
        return store

    return _apply_middlware(middleware)(store)
