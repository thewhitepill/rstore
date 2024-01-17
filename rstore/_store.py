from typing import Generic, TypeVar

from ._reducer import Reducer


A = TypeVar("A")
S = TypeVar("S")


__all__ = (
    "Store",
)


class Store(Generic[S, A]):
    def __init__(self, state: S, reducer: Reducer[S, A]) -> None:
        self.state = state
        self.reducer = reducer

    def dispatch(self, action: A) -> S:
        self.state = self.reducer.apply(self.state, action)

        return self.state
