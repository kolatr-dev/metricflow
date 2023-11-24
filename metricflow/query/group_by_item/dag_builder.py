from __future__ import annotations

import logging
from typing import Optional, Sequence

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolution_nodes.any_model_resolution_node import (
    NoMetricsQueryGroupByItemResolutionNode,
)
from metricflow.query.group_by_item.resolution_nodes.measure_resolution_node import MeasureGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode
from metricflow.query.group_by_item.resolve_filters.where_filter_location import MetricInputLocation

logger = logging.getLogger(__name__)


class GroupByItemResolutionDagBuilder:
    def __init__(
        self,
        manifest_lookup: SemanticManifestLookup,
    ) -> None:
        self._manifest_lookup = manifest_lookup

    def _build_dag_from_metric_node(
        self,
        metric_reference: MetricReference,
        metric_input_location: Optional[MetricInputLocation],
    ) -> MetricGroupByItemResolutionNode:
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference)

        # For a base metric, the parents are measure nodes
        if len(metric.input_metrics) == 0:
            measure_references_for_metric = tuple(
                input_measure.measure_reference for input_measure in metric.input_measures
            )

            source_candidates_for_measure_nodes = tuple(
                MeasureGroupByItemResolutionNode(
                    measure_reference=measure_reference,
                    child_metric_reference=metric_reference,
                )
                for measure_reference in measure_references_for_metric
            )
            return MetricGroupByItemResolutionNode(
                metric_reference=metric_reference,
                metric_input_location=metric_input_location,
                parent_nodes=source_candidates_for_measure_nodes,
            )
        # For a derived metric, the parents are other metrics.
        return MetricGroupByItemResolutionNode(
            metric_reference=metric_reference,
            metric_input_location=metric_input_location,
            parent_nodes=tuple(
                self._build_dag_from_metric_node(
                    metric_reference=metric_input.as_reference,
                    metric_input_location=MetricInputLocation(
                        parent_metric_reference=metric_reference,
                        metric_input_index=metric_input_index,
                    ),
                )
                for metric_input_index, metric_input in enumerate(metric.input_metrics)
            ),
        )

    def _build_dag_from_query_node(
        self, metric_references: Sequence[MetricReference], where_filter_intersection: WhereFilterIntersection
    ) -> QueryGroupByItemResolutionNode:
        if len(metric_references) == 0:
            return QueryGroupByItemResolutionNode(
                parent_nodes=(NoMetricsQueryGroupByItemResolutionNode(),),
                metrics_in_query=metric_references,
                where_filter_intersection=where_filter_intersection,
            )
        return QueryGroupByItemResolutionNode(
            parent_nodes=tuple(
                self._build_dag_from_metric_node(
                    metric_reference=metric_reference,
                    metric_input_location=None,
                )
                for metric_reference in metric_references
            ),
            metrics_in_query=metric_references,
            where_filter_intersection=where_filter_intersection,
        )

    def build(
        self, metric_references: Sequence[MetricReference], where_filter_intersection: Optional[WhereFilterIntersection]
    ) -> GroupByItemResolutionDag:
        return GroupByItemResolutionDag(
            sink_node=self._build_dag_from_query_node(
                metric_references=metric_references,
                where_filter_intersection=where_filter_intersection
                or PydanticWhereFilterIntersection(where_filters=[]),
            )
        )
