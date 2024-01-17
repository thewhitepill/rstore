from __future__ import annotations

import asyncio

from asyncio import Lock
from typing import Callable, Generic, Optional, Type, TypeVar

from pydantic import BaseModel
from redis.asyncio import Redis
from redis.client import PubSub
from redis.commands.json.path import Path as JsonPath

from ._reducer import Reducer


__all__ = (
    "RedisStore",
)


A = TypeVar("A", bound=BaseModel)
S = TypeVar("S", bound=BaseModel)


async def _load_state(
    client: Redis,
    key: str,
    cls: Type[S],
    factory: Callable[[], S]
) -> S:
    exists = await client.exists(key)

    if not exists:
        return factory()
    
    data = await client.json().get(key, path=JsonPath.root_path())

    return cls.model_validate(data)


async def _dump_state(
    client: Redis,
    key: str,
    state: S
) -> None:
    data = state.model_dump()

    await client.json().set(key, path=JsonPath.root_path(), value=data)


async def _subscribe_loop(
    store: RedisStore[S, A],
    action_type: Type[A],
    channel: PubSub
) -> None:
    while True:
        message = await channel.get_message()

        if message is None:
            continue

        if message["type"] != "message":
            continue

        data = message["data"]
        action = action_type.model_validate_json(data)

        await store.dispatch(action, propagate=False)


class RedisStore(Generic[S, A]):
    def __ini__(
        self,
        client: Redis,
        state_key: str,
        action_channel_key: str,
        state_type: Type[S],
        action_type: Type[A],
        reducer: Reducer[S, A],
        state_factory: Optional[Callable[[], S]] = None
    ) -> None:
        self._client = client
        self._state_key = state_key
        self._action_channel_key = action_channel_key
        self._state_type = state_type
        self._action_type = action_type
        self._reducer = reducer
        self._state_factory = state_factory or state_type

        self._lock = Lock()
        self._subscribe_task = None
        self._state = None

    async def bind(self) -> None:
        async with self._lock:
            self._state = await _load_state(
                self._client,
                self._state_key,
                self._state_type,
                self._state_factory
            )
        
        async with self._client.pubsub() as channel:
            await channel.subscribe(self._action_channel_key)
            subscription = _subscribe_loop(
                self,
                self._action_type,
                channel
            )

            self._subscribe_task = asyncio.create_task(subscription)

    async def dispatch(self, action: A, propagate: bool = True) -> S:
        async with self._lock:
            self._state = self._reducer.apply(self._state, action)

            if propagate:
                await _dump_state(self._client, self._state_key, self._state)
                await self._client.publish(
                    self._action_channel_key,
                    action.model_dump_json()
                )

            return self._state

    async def unbind(self) -> None:
        async with self._lock:
            self._state = None

        if self._subscribe_task is not None:
            self._subscribe_task.cancel()
            self._subscribe_task = None
