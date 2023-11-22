from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import MetricFlowQueryResolverInput
from metricflow.specs.specs import LinkableInstanceSpec


@dataclass(frozen=True)
class NoCandidatesWithNoneDatePartIssue(MetricFlowQueryResolutionIssue):
    candidate_specs: Tuple[LinkableInstanceSpec, ...]

    @staticmethod
    def create(
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
        query_resolution_path: MetricFlowQueryResolutionPath,
        candidate_specs: Sequence[LinkableInstanceSpec],
    ) -> NoCandidatesWithNoneDatePartIssue:
        return NoCandidatesWithNoneDatePartIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=tuple(parent_issues),
            query_resolution_path=query_resolution_path,
            candidate_specs=tuple(candidate_specs),
        )

    @override
    def ui_description(self, associated_input: Optional[MetricFlowQueryResolverInput]) -> str:
        # TODO: Improve error.
        lines = [
            f"{self.query_resolution_path.last_item.ui_description} does not allow group-by-items "
            f"with a date part in the query. Considered group by items:\n"
        ]

        if associated_input is not None and associated_input.naming_scheme is not None:
            naming_scheme = associated_input.naming_scheme
        else:
            naming_scheme = None

        if naming_scheme is not None:
            lines.append(mf_pformat([naming_scheme.input_str(spec) for spec in self.candidate_specs]))
        else:
            lines.append(mf_pformat([spec for spec in self.candidate_specs]))

        lines.append("\nwere excluded by this metric.")
        return "\n".join(lines)

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> NoCandidatesWithNoneDatePartIssue:
        return NoCandidatesWithNoneDatePartIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            candidate_specs=self.candidate_specs,
        )
