from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from typing_extensions import override

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import ResolverInputForOrderBy


@dataclass(frozen=True)
class InvalidOrderByItemIssue(MetricFlowQueryResolutionIssue):
    order_by_item_input: ResolverInputForOrderBy

    @staticmethod
    def create(
        order_by_item_input: ResolverInputForOrderBy,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidOrderByItemIssue:
        return InvalidOrderByItemIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            order_by_item_input=order_by_item_input,
        )

    @override
    def ui_description(self, naming_scheme: Optional[QueryItemNamingScheme]) -> str:
        return (
            f"The order by item {repr(self.order_by_item_input.ui_description)} does not match any of the input "
            f"query items."
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> InvalidOrderByItemIssue:
        return InvalidOrderByItemIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            order_by_item_input=self.order_by_item_input,
        )
