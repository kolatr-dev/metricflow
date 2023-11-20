from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from typing_extensions import override

from metricflow.dag.id_prefix import IdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.query.group_by_item.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.visitor import VisitorOutputT


class MeasureGroupByItemResolutionNode(GroupByItemResolutionNode):
    ID_PREFIX = IdPrefix.MEASURE_GROUP_BY_ITEM_RESOLUTION_NODE

    def __init__(  # noqa: D
        self,
        measure_reference: MeasureReference,
        child_metric_reference: MetricReference,
    ) -> None:
        self._measure_reference = measure_reference
        self._child_metric_reference = child_metric_reference
        super().__init__()

    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_measure_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the group by items possible for a measure."

    @property
    @override
    def parent_nodes(self) -> Sequence[GroupByItemResolutionNode]:
        return ()

    @classmethod
    @override
    def id_prefix_enum(cls) -> IdPrefix:
        return IdPrefix.MEASURE_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def displayed_properties(self) -> List[DisplayedProperty]:
        return super().displayed_properties + [
            DisplayedProperty(
                key="measure_reference",
                value=str(self._measure_reference),
            )
        ]

    @property
    def measure_reference(self) -> MeasureReference:  # noqa: D
        return self._measure_reference

    @property
    @override
    def ui_description(self) -> str:
        return f"Measure({repr(self._measure_reference.element_name)})"

    @property
    def child_metric_reference(self) -> MetricReference:
        return self._child_metric_reference
