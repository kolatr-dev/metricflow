from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolve_filters.filter_to_pattern import WhereFilterLinkableSpecLookup
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.resolver_inputs.query_resolver_inputs import MetricFlowQueryResolverInput
from metricflow.specs.specs import MetricFlowQuerySpec


@dataclass(frozen=True)
class InputToIssueSetMappingItem:
    resolver_input: MetricFlowQueryResolverInput
    issue_set: MetricFlowQueryResolutionIssueSet


@dataclass(frozen=True)
class InputToIssueSetMapping(Mergeable):
    items: Tuple[InputToIssueSetMappingItem, ...]

    @property
    def has_errors(self) -> bool:
        return any(item.issue_set.has_errors for item in self.items)

    @property
    def merged_issue_set(self) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.merge_iterable(item.issue_set for item in self.items)

    @override
    def merge(self, other: InputToIssueSetMapping) -> InputToIssueSetMapping:
        return InputToIssueSetMapping(items=self.items + other.items)

    @classmethod
    def empty_instance(cls) -> InputToIssueSetMapping:
        return InputToIssueSetMapping(
            items=(),
        )


@dataclass(frozen=True)
class MetricFlowQueryResolution:
    """The result of resolving query inputs to specs."""

    # Can be None if there were errors.
    query_spec: Optional[MetricFlowQuerySpec]
    resolution_dag: Optional[GroupByItemResolutionDag]
    where_filter_linkable_spec_lookup: WhereFilterLinkableSpecLookup
    input_to_issue_set: InputToIssueSetMapping

    @property
    def checked_query_spec(self) -> MetricFlowQuerySpec:
        """Returns the query_spec, but if MetricFlowQueryResolution.has_errors was True, raise a RuntimeError."""
        if self.input_to_issue_set.has_errors:
            raise RuntimeError(
                f"Can't get the query spec because errors were present in the resolution:\n"
                f"{mf_pformat(self.input_to_issue_set.has_errors)}"
            )
        if self.query_spec is None:
            raise RuntimeError("If there were no errors, query_spec should have been populated.")
        return self.query_spec

    @property
    def has_errors(self) -> bool:  # noqa: D
        return self.input_to_issue_set.has_errors


# @dataclass(frozen=True)
# class MetricFlowQueryGroupByItemResolution:
#     specs: Tuple[LinkableInstanceSpec, ...]
#     issue_set: MetricFlowQueryResolutionIssueSet


# @dataclass(frozen=True)
# class StringBasedQueryResolverInput:
#     metric_names: Tuple[str, ...]
#     group_by_item_names: Tuple[str, ...]
#     order_by_item_names: Tuple[str, ...]
#     limit: Optional[int]
#     where_filter_sql: Optional[str] = None
#
#
# @dataclass(frozen=True)
# class QueryParameterBasedQueryResolverInput:
#     metric_parameters: Tuple[MetricQueryParameter, ...]
#     group_by_parameters: Tuple[]
