from __future__ import annotations

import pytest

from metricflow.collection_helpers.non_empty_tuple import NonEmptyTuple


def test_non_empty_tuple_behavior() -> None:  # noqa: D
    items = NonEmptyTuple[int]([3, 2, 1])

    assert isinstance(items, tuple)
    assert items[0] == 3
    assert sorted(items) == [1, 2, 3]


def test_error_on_empty() -> None:  # noqa: D
    with pytest.raises(RuntimeError):
        NonEmptyTuple[int]([])
