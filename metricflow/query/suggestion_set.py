from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from metricflow.specs.specs import LinkableInstanceSpec


@dataclass(frozen=True)
class GroupByItemSuggestionSet:
    group_by_item_specs: Tuple[LinkableInstanceSpec, ...]
