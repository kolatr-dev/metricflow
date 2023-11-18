from __future__ import annotations

from typing import List, Optional, Sequence, Union

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import Self, override

from metricflow.dag.id_prefix import IdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.query.group_by_item.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_nodes.measure_resolution_node import MeasureGroupByItemResolutionNode
from metricflow.query.group_by_item.resolve_filters.where_filter_location import MetricInputLocation
from metricflow.visitor import VisitorOutputT


class MetricGroupByItemResolutionNode(GroupByItemResolutionNode):
    ID_PREFIX = IdPrefix.METRIC_GROUP_BY_ITEM_RESOLUTION_NODE

    def __init__(  # noqa: D
        self,
        metric_reference: MetricReference,
        metric_input_location: Optional[MetricInputLocation],
        parent_nodes: Sequence[Union[MeasureGroupByItemResolutionNode, Self]],
    ) -> None:
        self._metric_reference = metric_reference
        self._metric_input_location = metric_input_location
        self._parent_nodes = parent_nodes
        super().__init__()

    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_metric_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the valid group by items for this metric."

    @property
    @override
    def parent_nodes(self) -> Sequence[Union[MeasureGroupByItemResolutionNode, Self]]:
        return self._parent_nodes

    @classmethod
    @override
    def id_prefix_enum(cls) -> IdPrefix:
        return IdPrefix.METRIC_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def displayed_properties(self) -> List[DisplayedProperty]:
        return super().displayed_properties + [
            DisplayedProperty(
                key="metric_reference",
                value=str(self._metric_reference),
            ),
        ]

    @property
    def metric_reference(self) -> MetricReference:  # noqa: D
        return self._metric_reference

    @property
    def metric_input_location(self) -> Optional[MetricInputLocation]:  # noqa: D
        return self._metric_input_location

    @property
    @override
    def ui_description(self) -> str:
        return f"Metric('{self._metric_reference.element_name}')"
