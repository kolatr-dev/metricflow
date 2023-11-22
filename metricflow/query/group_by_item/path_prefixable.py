from __future__ import annotations

from abc import ABC, abstractmethod

from typing_extensions import Self

from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode


class PathPrefixable(ABC):
    @abstractmethod
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> Self:
        raise NotImplementedError
