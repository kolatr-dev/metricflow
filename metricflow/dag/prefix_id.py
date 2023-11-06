from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict

from metricflow.dag.id_prefix import IdPrefix


@dataclass(frozen=True)
class PrefixId:
    id_prefix: IdPrefix
    index: int

    def __str__(self) -> str:
        return f"{self.id_prefix.value}_{self.index}"


class PrefixIdGenerator:
    """Generate ID values based on an ID prefix.

    TODO: Migrate ID generation use cases to this class.
    """

    DEFAULT_START_VALUE = 0
    _state_lock = threading.Lock()
    _prefix_to_next_value: Dict[IdPrefix, int] = {}

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> PrefixId:
        with cls._state_lock:
            if id_prefix not in cls._prefix_to_next_value:
                cls._prefix_to_next_value[id_prefix] = cls.DEFAULT_START_VALUE
            index = cls._prefix_to_next_value[id_prefix]
            cls._prefix_to_next_value[id_prefix] = index + 1

            return PrefixId(id_prefix, index)
