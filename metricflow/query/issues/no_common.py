from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import NamedResolverInput


@dataclass(frozen=True)
class NoCommonItemsInParents(MetricFlowQueryResolutionIssue):
    parent_candidate_sets: Tuple[GroupByItemCandidateSet, ...]

    @staticmethod
    def create(  # noqa: D
        query_resolution_path: MetricFlowQueryResolutionPath,
        parent_node_to_candidate_set: Dict[GroupByItemResolutionNode, GroupByItemCandidateSet],
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
    ) -> NoCommonItemsInParents:
        return NoCommonItemsInParents(
            issue_type=MetricFlowQueryIssueType.ERROR,
            query_resolution_path=query_resolution_path,
            parent_candidate_sets=tuple(candidate_set for _, candidate_set in parent_node_to_candidate_set.items()),
            parent_issues=tuple(parent_issues),
        )

    @override
    def ui_description(self, associated_input: Optional[NamedResolverInput]) -> str:
        last_path_item = self.query_resolution_path.last_item
        last_path_item_parent_descriptions = ", ".join(
            [parent_node.ui_description for parent_node in last_path_item.parent_nodes]
        )
        naming_scheme = associated_input.naming_scheme if associated_input is not None else None

        parent_to_available_items = {}
        for candidate_set in self.parent_candidate_sets:
            resolution_node = candidate_set.path_from_leaf_node.last_item
            if naming_scheme is not None:
                spec_as_strs = tuple(naming_scheme.input_str(spec) for spec in candidate_set.specs)
            else:
                spec_as_strs = tuple(repr(spec) for spec in candidate_set.specs)
            parent_to_available_items["Matching items for: " + resolution_node.ui_description] = ", ".join(
                (spec_str if spec_str is not None else "None") for spec_str in spec_as_strs
            )
        return (
            f"{last_path_item.ui_description} is built from {last_path_item_parent_descriptions}. However, the "
            f"given input does not match to a common item that is available to those parents:\n\n"
            f"{indent_log_line(mf_pformat(parent_to_available_items))}\n\n"
            f"For time dimension inputs, please specify a time grain as ambiguous resolution will only allows "
            f"resolution when the parents have the same defined time gain."
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> NoCommonItemsInParents:
        return NoCommonItemsInParents(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            parent_candidate_sets=tuple(
                candidate_set.with_path_prefix(path_prefix_node) for candidate_set in self.parent_candidate_sets
            ),
        )
