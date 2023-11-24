from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from typing_extensions import override

from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import NamedResolverInput, ResolverInputForGroupBy
from metricflow.specs.specs import LinkableInstanceSpec


@dataclass(frozen=True)
class InvalidGroupByItemIssue(MetricFlowQueryResolutionIssue):
    """Describes when a metric specified as an input to a query does not match any of the known metrics."""

    group_by_item_input: ResolverInputForGroupBy
    possible_group_by_item_specs: Tuple[LinkableInstanceSpec, ...]

    @staticmethod
    def create(  # noqa: D
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
        group_by_item_input: ResolverInputForGroupBy,
        possible_group_by_item_specs: Sequence[LinkableInstanceSpec],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidGroupByItemIssue:
        return InvalidGroupByItemIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=tuple(parent_issues),
            query_resolution_path=query_resolution_path,
            group_by_item_input=group_by_item_input,
            possible_group_by_item_specs=tuple(possible_group_by_item_specs),
        )

    @override
    def ui_description(self, associated_input: Optional[NamedResolverInput]) -> str:
        # TODO: Improve message.
        return f"{self.group_by_item_input} is not a valid group by item for the query."

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> InvalidGroupByItemIssue:
        return InvalidGroupByItemIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            group_by_item_input=self.group_by_item_input,
            possible_group_by_item_specs=self.possible_group_by_item_specs,
        )
