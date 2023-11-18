from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from typing_extensions import override

from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode


@dataclass(frozen=True)
class MetricFlowQueryResolutionPath(PathPrefixable):
    resolution_path_nodes: Tuple[GroupByItemResolutionNode, ...]

    @staticmethod
    def empty_instance() -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=(),
        )

    @property
    def last_item(self) -> GroupByItemResolutionNode:  # noqa: D
        return self.resolution_path_nodes[-1]

    @property
    def ui_description(self) -> str:
        # TODO: Use a type to enforce this.
        assert len(self.resolution_path_nodes) > 0
        descriptions = tuple(f"[Resolve {path_node.ui_description}]" for path_node in self.resolution_path_nodes)
        output = descriptions[0]

        for i, description in enumerate(descriptions[1:]):
            output += "\n"
            output += "  " * (i + 1)
            output += "-> "
            output += description

        return output

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(resolution_path_nodes=(path_prefix_node,) + self.resolution_path_nodes)

    @override
    def __str__(self) -> str:
        items = [self.__class__.__name__, "(", ", ".join(tuple(str(node) for node in self.resolution_path_nodes)), ")"]
        return "".join(items)

    @staticmethod
    def from_path_item(node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=(node,),
        )
