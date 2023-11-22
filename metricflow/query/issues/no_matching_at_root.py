from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.specs.specs import LinkableInstanceSpec


@dataclass(frozen=True)
class NoMatchingGroupByItemsAtRoot(MetricFlowQueryResolutionIssue):
    candidate_specs: Tuple[LinkableInstanceSpec, ...]

    @staticmethod
    def create(
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
        query_resolution_path: MetricFlowQueryResolutionPath,
        candidate_specs: Sequence[LinkableInstanceSpec],
    ) -> NoMatchingGroupByItemsAtRoot:
        return NoMatchingGroupByItemsAtRoot(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=tuple(parent_issues),
            query_resolution_path=query_resolution_path,
            candidate_specs=tuple(candidate_specs),
        )

    @override
    def ui_description(self, naming_scheme: Optional[QueryItemNamingScheme]) -> str:
        # TODO: Improve error.
        description_prefix = (
            f"The given input does not match any of the available group by items for "
            f"{self.query_resolution_path.last_item.ui_description}. Available items:\n"
        )
        available_items: List[str] = []
        for spec in self.candidate_specs:
            if naming_scheme is not None:
                input_str = naming_scheme.input_str(spec)
                if input_str is not None:
                    available_items.append(input_str)
            else:
                available_items.append(repr(spec))

        return description_prefix + mf_pformat(available_items)

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> NoMatchingGroupByItemsAtRoot:
        return NoMatchingGroupByItemsAtRoot(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            candidate_specs=self.candidate_specs,
        )
