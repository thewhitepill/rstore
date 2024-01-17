from typing import TypeVar, Generic


A = TypeVar("A")
S = TypeVar("S")


__all__ = (
    "Reducer",
)


class Reducer(Generic[S, A]):
    def apply(self, state: S, action: A) -> S:
        raise NotImplementedError
