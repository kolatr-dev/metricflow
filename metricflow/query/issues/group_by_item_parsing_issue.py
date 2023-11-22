from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from typing_extensions import override

from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class GroupByItemNameParsingIssue(MetricFlowQueryResolutionIssue):
    input_str: str

    @staticmethod
    def from_parameters(input_str: str) -> GroupByItemNameParsingIssue:
        return GroupByItemNameParsingIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=MetricFlowQueryResolutionPath.empty_instance(),
            input_str=input_str,
        )

    @override
    def ui_description(self, associated_input: Optional[MetricFlowQueryResolverInput]) -> str:
        return f"The group-by-item {repr(self.input_str)} does not match any of the known formats."

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> GroupByItemNameParsingIssue:
        return GroupByItemNameParsingIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            input_str=self.input_str,
        )
