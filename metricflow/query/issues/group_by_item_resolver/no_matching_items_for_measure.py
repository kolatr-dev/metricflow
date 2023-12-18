from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Sequence, Tuple

from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput
from metricflow.specs.specs import InstanceSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NoMatchingItemsForMeasure(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where there are no matching items for a measure.

    This can happen if the user specifies a group-by-item that does not exist or is not available for the measure.
    """

    similar_specs_for_suggestion: Tuple[InstanceSpec, ...]

    @staticmethod
    def from_parameters(  # noqa: D
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
        query_resolution_path: MetricFlowQueryResolutionPath,
        similar_specs_for_suggestion: Sequence[InstanceSpec],
    ) -> NoMatchingItemsForMeasure:
        return NoMatchingItemsForMeasure(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=tuple(parent_issues),
            query_resolution_path=query_resolution_path,
            similar_specs_for_suggestion=tuple(similar_specs_for_suggestion),
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        message = (
            f"The given input does not match any of the available group-by-items for "
            f"{self.query_resolution_path.last_item.ui_description}."
        )

        input_pattern_description = associated_input.input_pattern_description

        if input_pattern_description is None:
            return message

        suggestions: List[str] = []
        for spec in self.similar_specs_for_suggestion:
            spec_as_str = input_pattern_description.naming_scheme.input_str(spec)
            if spec_as_str is not None:
                suggestions.append(spec_as_str)

        if len(suggestions) == 0:
            return message

        return message + f"\n\nSuggestions:\n{indent_log_line(mf_pformat(suggestions))}"

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoMatchingItemsForMeasure:
        return NoMatchingItemsForMeasure(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            similar_specs_for_suggestion=self.similar_specs_for_suggestion,
        )
