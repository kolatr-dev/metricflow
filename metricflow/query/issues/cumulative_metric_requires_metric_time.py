from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class CumulativeMetricRequiresMetricTimeIssue(MetricFlowQueryResolutionIssue):
    metric_reference: MetricReference

    @override
    def ui_description(self, associated_input: Optional[MetricFlowQueryResolverInput]) -> str:
        return (
            f"The query includes a cumulative metric {repr(self.metric_reference.element_name)} but the "
            f"group-by-items do not include {repr(METRIC_TIME_ELEMENT_NAME)}"
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> CumulativeMetricRequiresMetricTimeIssue:
        return CumulativeMetricRequiresMetricTimeIssue(
            issue_type=self.issue_type,
            parent_issues=self.parent_issues,
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            metric_reference=self.metric_reference,
        )

    @staticmethod
    def create(
        metric_reference: MetricReference, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> CumulativeMetricRequiresMetricTimeIssue:
        return CumulativeMetricRequiresMetricTimeIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            metric_reference=metric_reference,
        )
