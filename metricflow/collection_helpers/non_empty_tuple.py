from __future__ import annotations

from typing import Generic, Iterable, Tuple, TypeVar

ItemT = TypeVar("ItemT")


class NonEmptyTuple(Generic[ItemT], Tuple[ItemT]):
    def __new__(cls, items: Iterable[ItemT]) -> NonEmptyTuple[ItemT]:
        items_tuple = tuple(items)
        if len(items_tuple) == 0:
            raise RuntimeError(f"Can't create a {cls.__name__} without elements.")

        return tuple.__new__(NonEmptyTuple, items_tuple)

    @staticmethod
    def from_one(item: ItemT) -> NonEmptyTuple[ItemT]:
        return NonEmptyTuple((item,))

    @property
    def first(self) -> ItemT:
        return self[0]

    @property
    def after_first(self) -> Tuple[ItemT, ...]:  # noqa: D
        return self[1:]
