from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterCallParameterSets,
    ParseWhereFilterException,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.protocols import WhereFilter
from dbt_semantic_interfaces.references import MetricReference
from more_itertools import is_sorted
from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import DagTraversalPathTracker
from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolution_nodes.any_model_resolution_node import AnyModelGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_nodes.measure_resolution_node import MeasureGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode
from metricflow.query.issues.invalid_where import InvalidWhereFilterFormatIssue
from metricflow.query.issues.issues_base import (
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionIssueSet,
    MetricFlowQueryResolutionPath,
)
from metricflow.specs.patterns.entity_link_pattern import (
    DimensionPattern,
    EntityPattern,
    TimeDimensionPattern,
)
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec

logger = logging.getLogger(__name__)


CallParameterSet = Union[DimensionCallParameterSet, TimeDimensionCallParameterSet, EntityCallParameterSet]


@dataclass(frozen=True)
class ResolvedSpecApplicabilityKey:
    metric_references: Tuple[MetricReference, ...]
    call_parameter_set: CallParameterSet

    def __post_init__(self) -> None:  # noqa: D
        assert is_sorted(self.metric_references)

    @staticmethod
    def from_parameters(
        metric_references: Sequence[MetricReference], call_parameter_set: CallParameterSet
    ) -> ResolvedSpecApplicabilityKey:
        return ResolvedSpecApplicabilityKey(
            metric_references=tuple(sorted(metric_references)), call_parameter_set=call_parameter_set
        )

    @staticmethod
    def for_metric(
        metric_reference: MetricReference, call_parameter_set: CallParameterSet
    ) -> ResolvedSpecApplicabilityKey:
        return ResolvedSpecApplicabilityKey(
            metric_references=(metric_reference,),
            call_parameter_set=call_parameter_set,
        )

    @staticmethod
    def for_query(
        metrics_in_query: Sequence[MetricReference], call_parameter_set: CallParameterSet
    ) -> ResolvedSpecApplicabilityKey:
        return ResolvedSpecApplicabilityKey(
            metric_references=tuple(sorted(metrics_in_query)),
            call_parameter_set=call_parameter_set,
        )


@dataclass(frozen=True)
class CallParameterSetToSpecMapping(PathPrefixable):
    applicability_key: ResolvedSpecApplicabilityKey
    issue_set: MetricFlowQueryResolutionIssueSet
    resolution_path: MetricFlowQueryResolutionPath
    resolved_spec: Optional[LinkableInstanceSpec]

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> CallParameterSetToSpecMapping:
        return CallParameterSetToSpecMapping(
            applicability_key=self.applicability_key,
            issue_set=self.issue_set.with_path_prefix(path_prefix_node),
            resolution_path=self.resolution_path.with_path_prefix(path_prefix_node),
            resolved_spec=self.resolved_spec,
        )


@dataclass(frozen=True)
class ResolvedSpecLookup(Mergeable, PathPrefixable):
    call_parameter_set_to_spec_mappings: Tuple[CallParameterSetToSpecMapping, ...]
    other_issue_set: MetricFlowQueryResolutionIssueSet

    def get_mappings(self, applicability_key: ResolvedSpecApplicabilityKey) -> Sequence[CallParameterSetToSpecMapping]:
        return tuple(
            mapping
            for mapping in self.call_parameter_set_to_spec_mappings
            if mapping.applicability_key == applicability_key
        )

    @override
    def merge(self, other: ResolvedSpecLookup) -> ResolvedSpecLookup:
        return ResolvedSpecLookup(
            call_parameter_set_to_spec_mappings=self.call_parameter_set_to_spec_mappings
            + other.call_parameter_set_to_spec_mappings,
            other_issue_set=self.other_issue_set.merge(other.other_issue_set),
        )

    @override
    @classmethod
    def empty_instance(cls) -> ResolvedSpecLookup:
        return ResolvedSpecLookup(
            call_parameter_set_to_spec_mappings=(),
            other_issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> ResolvedSpecLookup:
        return ResolvedSpecLookup(
            call_parameter_set_to_spec_mappings=tuple(
                mapping.with_path_prefix(path_prefix_node) for mapping in self.call_parameter_set_to_spec_mappings
            ),
            other_issue_set=self.other_issue_set.with_path_prefix(path_prefix_node),
        )

    @property
    def issue_set(self) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.merge_iterable(
            tuple(
                call_parameter_set_to_spec_mapping.issue_set
                for call_parameter_set_to_spec_mapping in self.call_parameter_set_to_spec_mappings
            )
            + (self.other_issue_set,)
        )


@dataclass(frozen=True)
class _SpecPatternResult:
    call_parameter_set: CallParameterSet
    spec_pattern: SpecPattern


class _ResolveCallParameterSetToSpecMappingSetVisitor(GroupByItemResolutionNodeVisitor[ResolvedSpecLookup]):
    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._manifest_lookup = manifest_lookup
        self._path_from_start_node_tracker = DagTraversalPathTracker()

    @staticmethod
    def _convert_filter_call_parameter_set_to_patterns(
        filter_call_parameter_sets: FilterCallParameterSets,
    ) -> Sequence[_SpecPatternResult]:
        mappings = []
        for dimension_call_parameter_set in filter_call_parameter_sets.dimension_call_parameter_sets:
            mappings.append(
                _SpecPatternResult(
                    call_parameter_set=dimension_call_parameter_set,
                    spec_pattern=DimensionPattern.from_call_parameter_set(dimension_call_parameter_set),
                )
            )
        for time_dimension_call_parameter_set in filter_call_parameter_sets.time_dimension_call_parameter_sets:
            mappings.append(
                _SpecPatternResult(
                    call_parameter_set=time_dimension_call_parameter_set,
                    spec_pattern=TimeDimensionPattern.from_call_parameter_set(time_dimension_call_parameter_set),
                )
            )
        for entity_call_parameter_set in filter_call_parameter_sets.entity_call_parameter_sets:
            mappings.append(
                _SpecPatternResult(
                    call_parameter_set=entity_call_parameter_set,
                    spec_pattern=EntityPattern.from_call_parameter_set(entity_call_parameter_set),
                )
            )

        return mappings

    @override
    def visit_measure_node(self, node: MeasureGroupByItemResolutionNode) -> ResolvedSpecLookup:
        with self._path_from_start_node_tracker.track_node_visit(node):
            return ResolvedSpecLookup.empty_instance()

    @override
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> ResolvedSpecLookup:
        with self._path_from_start_node_tracker.track_node_visit(node):
            results_to_merge: List[ResolvedSpecLookup] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self).with_path_prefix(node))

            results_to_merge.append(
                self._resolve_mappings_for_where_filters_at_metric_node(
                    metric_node=node,
                )
            )

            return ResolvedSpecLookup.merge_iterable(results_to_merge)

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> ResolvedSpecLookup:
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[ResolvedSpecLookup] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self).with_path_prefix(node))

            results_to_merge.append(
                self._resolve_mappings_for_where_filters_at_query_node(
                    query_node=node,
                    resolution_path=resolution_path,
                )
            )
            return ResolvedSpecLookup.merge_iterable(results_to_merge)

    @override
    def visit_any_model_node(self, node: AnyModelGroupByItemResolutionNode) -> ResolvedSpecLookup:
        with self._path_from_start_node_tracker.track_node_visit(node):
            return ResolvedSpecLookup.empty_instance()

    def _get_where_filters_at_metric_node(self, metric_node: MetricGroupByItemResolutionNode) -> Sequence[WhereFilter]:
        where_filters: List[WhereFilter] = []
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_node.metric_reference)

        # TODO: Add test case.
        if metric.input_measures is not None:
            for input_measure in metric.input_measures:
                if input_measure.filter is not None:
                    where_filters.extend(input_measure.filter.where_filters)

        if metric.filter:
            where_filters.extend(metric.filter.where_filters)

        if metric_node.metric_input_location is not None:
            parent_metric_input = metric_node.metric_input_location.get_metric_input(
                self._manifest_lookup.metric_lookup
            )
            if parent_metric_input.filter is not None:
                where_filters.extend(parent_metric_input.filter.where_filters)

        return where_filters

    def _resolve_mappings_for_where_filters_at_metric_node(
        self, metric_node: MetricGroupByItemResolutionNode
    ) -> ResolvedSpecLookup:
        results_to_merge: List[ResolvedSpecLookup] = []
        resolution_dag = GroupByItemResolutionDag(
            sink_node=metric_node,
        )
        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )
        call_parameter_set_to_pattern_mappings: List[_SpecPatternResult] = []
        for where_filter in self._get_where_filters_at_metric_node(metric_node):
            call_parameter_set_to_pattern_mappings.extend(
                _ResolveCallParameterSetToSpecMappingSetVisitor._convert_filter_call_parameter_set_to_patterns(
                    filter_call_parameter_sets=where_filter.call_parameter_sets
                )
            )

        mappings: List[CallParameterSetToSpecMapping] = []
        for call_parameter_set_to_pattern_mapping in call_parameter_set_to_pattern_mappings:
            group_by_item_resolution = group_by_item_resolver.resolve_matching_item_for_where_filter(
                spec_pattern=call_parameter_set_to_pattern_mapping.spec_pattern,
                resolution_node=metric_node,
            )
            mappings.append(
                CallParameterSetToSpecMapping(
                    applicability_key=ResolvedSpecApplicabilityKey.for_metric(
                        metric_reference=metric_node.metric_reference,
                        call_parameter_set=call_parameter_set_to_pattern_mapping.call_parameter_set,
                    ),
                    issue_set=group_by_item_resolution.issue_set,
                    resolution_path=MetricFlowQueryResolutionPath.from_path_item(metric_node),
                    resolved_spec=group_by_item_resolution.spec,
                )
            )

        results_to_merge.append(
            ResolvedSpecLookup(
                call_parameter_set_to_spec_mappings=tuple(mappings),
                other_issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
        )

        return ResolvedSpecLookup.merge_iterable(results_to_merge)

    def _resolve_mappings_for_where_filters_at_query_node(
        self,
        query_node: QueryGroupByItemResolutionNode,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> ResolvedSpecLookup:
        results_to_merge: List[ResolvedSpecLookup] = []
        resolution_dag = GroupByItemResolutionDag(
            sink_node=query_node,
        )
        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )
        call_parameter_set_to_pattern_mappings: List[_SpecPatternResult] = []
        other_issues: List[MetricFlowQueryResolutionIssue] = []
        if query_node.where_filter_intersection is not None:
            for where_filter in query_node.where_filter_intersection.where_filters:
                try:
                    call_parameter_set_to_pattern_mappings.extend(
                        _ResolveCallParameterSetToSpecMappingSetVisitor._convert_filter_call_parameter_set_to_patterns(
                            filter_call_parameter_sets=where_filter.call_parameter_sets
                        )
                    )
                except ParseWhereFilterException as e:
                    other_issues.append(
                        InvalidWhereFilterFormatIssue.from_parameters(
                            where_filter=where_filter,
                            parse_exception=e,
                            query_resolution_path=resolution_path,
                        )
                    )

        mappings: List[CallParameterSetToSpecMapping] = []
        for call_parameter_set_to_pattern_mapping in call_parameter_set_to_pattern_mappings:
            group_by_item_resolution = group_by_item_resolver.resolve_matching_item_for_where_filter(
                spec_pattern=call_parameter_set_to_pattern_mapping.spec_pattern,
                resolution_node=query_node,
            )
            mappings.append(
                CallParameterSetToSpecMapping(
                    applicability_key=ResolvedSpecApplicabilityKey.for_query(
                        metrics_in_query=query_node.metrics_in_query,
                        call_parameter_set=call_parameter_set_to_pattern_mapping.call_parameter_set,
                    ),
                    issue_set=group_by_item_resolution.issue_set,
                    resolution_path=MetricFlowQueryResolutionPath.from_path_item(query_node),
                    resolved_spec=group_by_item_resolution.spec,
                )
            )

        results_to_merge.append(
            ResolvedSpecLookup(
                call_parameter_set_to_spec_mappings=tuple(mappings),
                other_issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
        )

        return ResolvedSpecLookup.merge_iterable(results_to_merge).merge(
            ResolvedSpecLookup(
                call_parameter_set_to_spec_mappings=(),
                other_issue_set=MetricFlowQueryResolutionIssueSet.from_issues(other_issues),
            )
        )


class WhereFilterLinkableSpecResolver:
    def __init__(  # noqa: D
        self,
        manifest_lookup: SemanticManifestLookup,
        resolution_dag: GroupByItemResolutionDag,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolution_dag = resolution_dag

    def resolve_lookup(self) -> ResolvedSpecLookup:
        visitor = _ResolveCallParameterSetToSpecMappingSetVisitor(manifest_lookup=self._manifest_lookup)

        return self._resolution_dag.sink_node.accept(visitor)
