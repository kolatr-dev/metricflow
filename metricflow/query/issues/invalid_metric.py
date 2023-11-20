from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)


@dataclass(frozen=True)
class InvalidMetricIssue(MetricFlowQueryResolutionIssue):
    """Describes when a metric specified as an input to a query does not match any of the known metrics."""

    invalid_metric_reference: MetricReference
    candidate_metric_references: Tuple[MetricReference, ...]

    @staticmethod
    def create(  # noqa: D
        invalid_metric_reference: MetricReference,
        candidate_metric_references: Sequence[MetricReference],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidMetricIssue:
        return InvalidMetricIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            invalid_metric_reference=invalid_metric_reference,
            candidate_metric_references=tuple(candidate_metric_references),
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, naming_scheme: Optional[QueryItemNamingScheme]) -> str:
        # TODO: Provide suggestions for alternative metrics.
        return f"{self.invalid_metric_reference} does not match any of the known metrics."

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> InvalidMetricIssue:
        return InvalidMetricIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            invalid_metric_reference=self.invalid_metric_reference,
            candidate_metric_references=self.candidate_metric_references,
        )
