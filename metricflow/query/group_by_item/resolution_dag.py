from __future__ import annotations

from typing import Optional, Union

from metricflow.dag.id_generation import GROUP_BY_ITEM_RESOLUTION_DAG, IdGeneratorRegistry
from metricflow.dag.mf_dag import DagId, MetricFlowDag
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode


class GroupByItemResolutionDagId(DagId):
    @classmethod
    def create_unique(cls) -> GroupByItemResolutionDagId:  # noqa: D
        return GroupByItemResolutionDagId(IdGeneratorRegistry.for_class(cls).create_id(GROUP_BY_ITEM_RESOLUTION_DAG))


ResolutionDagSinkNode = Union[QueryGroupByItemResolutionNode, MetricGroupByItemResolutionNode]


class GroupByItemResolutionDag(MetricFlowDag[GroupByItemResolutionNode]):
    def __init__(
        self,
        sink_node: ResolutionDagSinkNode,
        dag_id: Optional[GroupByItemResolutionDagId] = None,
    ) -> None:  # noqa: D
        super().__init__(
            dag_id=GroupByItemResolutionDagId.create_unique() if dag_id is None else dag_id,
            sink_nodes=[sink_node],
        )
        self._sink_node = sink_node

    @property
    def sink_node(self) -> ResolutionDagSinkNode:  # noqa: D
        return self._sink_node
