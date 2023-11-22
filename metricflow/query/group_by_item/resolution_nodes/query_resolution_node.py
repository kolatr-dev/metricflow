from __future__ import annotations

from typing import List, Optional, Sequence, Union

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.dag.id_prefix import IdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.query.group_by_item.resolution_nodes.any_model_resolution_node import AnyModelGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.visitor import VisitorOutputT


class QueryGroupByItemResolutionNode(GroupByItemResolutionNode):
    def __init__(
        self,
        parent_nodes: Sequence[Union[MetricGroupByItemResolutionNode, AnyModelGroupByItemResolutionNode]],
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: Optional[WhereFilterIntersection],
    ) -> None:
        self._parent_nodes = tuple(parent_nodes)
        self._metrics_in_query = tuple(metrics_in_query)
        self._where_filter_intersection = where_filter_intersection
        super().__init__()

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_query_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the valid group by items in the metric query."

    @property
    @override
    def parent_nodes(self) -> Sequence[Union[MetricGroupByItemResolutionNode, AnyModelGroupByItemResolutionNode]]:
        return self._parent_nodes

    @classmethod
    @override
    def id_prefix_enum(cls) -> IdPrefix:
        return IdPrefix.QUERY_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    def metrics_in_query(self) -> Sequence[MetricReference]:
        return self._metrics_in_query

    @property
    @override
    def displayed_properties(self) -> List[DisplayedProperty]:
        return super().displayed_properties + [
            DisplayedProperty(
                key="metrics_in_query",
                value=[str(metric_reference) for metric_reference in self.metrics_in_query],
            )
        ]

    @property
    def where_filter_intersection(self) -> Optional[WhereFilterIntersection]:
        return self._where_filter_intersection

    @property
    @override
    def ui_description(self) -> str:
        return f"Query({repr([metric_reference.element_name for metric_reference in self._metrics_in_query])})"
