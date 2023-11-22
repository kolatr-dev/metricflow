from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterCallParameterSets,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.protocols import WhereFilter
from dbt_semantic_interfaces.references import MetricReference
from more_itertools import is_sorted
from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import DagTraversalPathTracker
from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag, ResolutionDagSinkNode
from metricflow.query.group_by_item.resolution_nodes.any_model_resolution_node import AnyModelGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_nodes.measure_resolution_node import MeasureGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode
from metricflow.query.issues.invalid_where import WhereFilterParsingIssue
from metricflow.query.issues.issues_base import (
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
class WhereFilterLocation:
    metric_references: Tuple[MetricReference, ...]

    def __post_init__(self) -> None:  # noqa: D
        assert is_sorted(self.metric_references)

    @staticmethod
    def for_query(metric_references: Sequence[MetricReference]) -> WhereFilterLocation:  # noqa: D
        return WhereFilterLocation(metric_references=tuple(sorted(metric_references)))

    @staticmethod
    def for_metric(metric_reference: MetricReference) -> WhereFilterLocation:  # noqa: D
        return WhereFilterLocation(metric_references=(metric_reference,))


@dataclass(frozen=True)
class ResolvedSpecLookUpKey:
    filter_location: WhereFilterLocation
    call_parameter_set: CallParameterSet

    @staticmethod
    def from_parameters(
        filter_location: WhereFilterLocation, call_parameter_set: CallParameterSet
    ) -> ResolvedSpecLookUpKey:
        return ResolvedSpecLookUpKey(
            filter_location=filter_location,
            call_parameter_set=call_parameter_set,
        )

    @staticmethod
    def for_metric(metric_reference: MetricReference, call_parameter_set: CallParameterSet) -> ResolvedSpecLookUpKey:
        return ResolvedSpecLookUpKey(
            filter_location=WhereFilterLocation.for_metric(
                metric_reference,
            ),
            call_parameter_set=call_parameter_set,
        )

    @staticmethod
    def for_query(
        metrics_in_query: Sequence[MetricReference], call_parameter_set: CallParameterSet
    ) -> ResolvedSpecLookUpKey:
        return ResolvedSpecLookUpKey(
            filter_location=WhereFilterLocation.for_query(metrics_in_query),
            call_parameter_set=call_parameter_set,
        )


@dataclass(frozen=True)
class WhereFilterSpecResolution(PathPrefixable):
    lookup_key: ResolvedSpecLookUpKey
    resolution_path: MetricFlowQueryResolutionPath
    resolved_spec: Optional[LinkableInstanceSpec]

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> WhereFilterSpecResolution:
        return WhereFilterSpecResolution(
            lookup_key=self.lookup_key,
            resolution_path=self.resolution_path.with_path_prefix(path_prefix_node),
            resolved_spec=self.resolved_spec,
        )


@dataclass(frozen=True)
class FilterSpecResolutionLookUp(Mergeable, PathPrefixable):
    spec_resolutions: Tuple[WhereFilterSpecResolution, ...]
    issue_set: MetricFlowQueryResolutionIssueSet

    def get_spec_resolutions(
        self, resolved_spec_lookup_key: ResolvedSpecLookUpKey
    ) -> Sequence[WhereFilterSpecResolution]:
        return tuple(
            resolution for resolution in self.spec_resolutions if resolution.lookup_key == resolved_spec_lookup_key
        )

    def spec_resolution_exists(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> bool:
        return len(self.get_spec_resolutions(resolved_spec_lookup_key)) > 0

    def checked_resolved_spec(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> LinkableInstanceSpec:
        resolutions = self.get_spec_resolutions(resolved_spec_lookup_key)
        if len(resolutions) != 1:
            raise RuntimeError(
                f"Unable to find a resolved spec.\n\n"
                f"Expected 1 resolution for:\n\n"
                f"{indent_log_line(mf_pformat(resolved_spec_lookup_key))}\n\n"
                f"but got:\n\n"
                f"{indent_log_line(mf_pformat(resolutions))}.\n\n"
                f"All resolutions are:\n\n"
                f"{indent_log_line(mf_pformat(self.spec_resolutions))}"
            )

        resolution = resolutions[0]
        if resolution.resolved_spec is None:
            raise RuntimeError(
                f"Expected resolution with a resolved spec, but got:\n"
                f"{mf_pformat(resolution)}.\n"
                f"All resolutions are:\n"
                f"{mf_pformat(self.spec_resolutions)}"
            )

        return resolution.resolved_spec

    @override
    def merge(self, other: FilterSpecResolutionLookUp) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=self.spec_resolutions + other.spec_resolutions,
            issue_set=self.issue_set.merge(other.issue_set),
        )

    @override
    @classmethod
    def empty_instance(cls) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=(),
            issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=tuple(
                resolution.with_path_prefix(path_prefix_node) for resolution in self.spec_resolutions
            ),
            issue_set=self.issue_set.with_path_prefix(path_prefix_node),
        )


class _ResolveWhereFilterSpecVisitor(GroupByItemResolutionNodeVisitor[FilterSpecResolutionLookUp]):
    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._manifest_lookup = manifest_lookup
        self._path_from_start_node_tracker = DagTraversalPathTracker()

    @staticmethod
    def _map_filter_parameter_set_to_pattern(
        filter_location: WhereFilterLocation,
        filter_call_parameter_sets: FilterCallParameterSets,
        spec_resolution_lookup_so_far: FilterSpecResolutionLookUp,
    ) -> Dict[CallParameterSet, SpecPattern]:
        call_parameter_set_to_spec_pattern: Dict[CallParameterSet, SpecPattern] = {}
        for dimension_call_parameter_set in filter_call_parameter_sets.dimension_call_parameter_sets:
            lookup_key = ResolvedSpecLookUpKey(
                filter_location=filter_location,
                call_parameter_set=dimension_call_parameter_set,
            )
            if spec_resolution_lookup_so_far.spec_resolution_exists(lookup_key):
                logger.info(
                    f"Skipping resolution for {dimension_call_parameter_set} at {filter_location} since it has already"
                    f"been resolved to:\n\n"
                    f"{indent_log_line(mf_pformat(spec_resolution_lookup_so_far.get_spec_resolutions(lookup_key)))}"
                )
                continue
            if dimension_call_parameter_set not in call_parameter_set_to_spec_pattern:
                call_parameter_set_to_spec_pattern[
                    dimension_call_parameter_set
                ] = DimensionPattern.from_call_parameter_set(dimension_call_parameter_set)
        for time_dimension_call_parameter_set in filter_call_parameter_sets.time_dimension_call_parameter_sets:
            lookup_key = ResolvedSpecLookUpKey(
                filter_location=filter_location,
                call_parameter_set=time_dimension_call_parameter_set,
            )
            if spec_resolution_lookup_so_far.spec_resolution_exists(lookup_key):
                logger.info(
                    f"Skipping resolution for {time_dimension_call_parameter_set} at {filter_location} since it has "
                    f"been resolved to:\n"
                    f"{mf_pformat(spec_resolution_lookup_so_far.get_spec_resolutions(lookup_key))}"
                )
            if time_dimension_call_parameter_set not in call_parameter_set_to_spec_pattern:
                call_parameter_set_to_spec_pattern[
                    time_dimension_call_parameter_set
                ] = TimeDimensionPattern.from_call_parameter_set(time_dimension_call_parameter_set)
        for entity_call_parameter_set in filter_call_parameter_sets.entity_call_parameter_sets:
            lookup_key = ResolvedSpecLookUpKey(
                filter_location=filter_location,
                call_parameter_set=entity_call_parameter_set,
            )
            if spec_resolution_lookup_so_far.spec_resolution_exists(lookup_key):
                logger.info(
                    f"Skipping resolution for {entity_call_parameter_set} at {filter_location} since it has "
                    f"been resolved to:\n"
                    f"{mf_pformat(spec_resolution_lookup_so_far.get_spec_resolutions(lookup_key))}"
                )

            if entity_call_parameter_set not in call_parameter_set_to_spec_pattern:
                call_parameter_set_to_spec_pattern[entity_call_parameter_set] = EntityPattern.from_call_parameter_set(
                    entity_call_parameter_set
                )

        return call_parameter_set_to_spec_pattern

    @override
    def visit_measure_node(self, node: MeasureGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        with self._path_from_start_node_tracker.track_node_visit(node):
            return FilterSpecResolutionLookUp.empty_instance()

    @override
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[FilterSpecResolutionLookUp] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self).with_path_prefix(node))
            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge)

            return resolved_spec_lookup_so_far.merge(
                self._resolve_resolutions_for_where_filters(
                    resolution_node=node,
                    resolution_path=resolution_path,
                    resolved_spec_lookup_so_far=resolved_spec_lookup_so_far,
                    filter_location=WhereFilterLocation.for_metric(node.metric_reference),
                    where_filters=self._get_where_filters_at_metric_node(node),
                )
            )

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[FilterSpecResolutionLookUp] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self).with_path_prefix(node))
            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge)

            return resolved_spec_lookup_so_far.merge(
                self._resolve_resolutions_for_where_filters(
                    resolution_node=node,
                    resolution_path=resolution_path,
                    resolved_spec_lookup_so_far=resolved_spec_lookup_so_far,
                    filter_location=WhereFilterLocation.for_query(node.metrics_in_query),
                    where_filters=node.where_filter_intersection.where_filters,
                )
            )

    @override
    def visit_any_model_node(self, node: AnyModelGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        with self._path_from_start_node_tracker.track_node_visit(node):
            return FilterSpecResolutionLookUp.empty_instance()

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

    def _resolve_resolutions_for_where_filters(
        self,
        resolution_node: ResolutionDagSinkNode,
        resolution_path: MetricFlowQueryResolutionPath,
        resolved_spec_lookup_so_far: FilterSpecResolutionLookUp,
        filter_location: WhereFilterLocation,
        where_filters: Sequence[WhereFilter],
    ) -> FilterSpecResolutionLookUp:
        results_to_merge: List[FilterSpecResolutionLookUp] = []
        resolution_dag = GroupByItemResolutionDag(
            sink_node=resolution_node,
        )
        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )
        call_parameter_set_to_pattern: Dict[CallParameterSet, SpecPattern] = {}
        issue_sets_to_merge: List[MetricFlowQueryResolutionIssueSet] = []
        for where_filter in where_filters:
            try:
                filter_call_parameter_sets = where_filter.call_parameter_sets
            except Exception as e:
                issue_sets_to_merge.append(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        WhereFilterParsingIssue.from_parameters(
                            where_filter=where_filter,
                            parse_exception=e,
                            query_resolution_path=resolution_path,
                        )
                    )
                )
                continue

            call_parameter_set_to_pattern.update(
                _ResolveWhereFilterSpecVisitor._map_filter_parameter_set_to_pattern(
                    filter_location=filter_location,
                    filter_call_parameter_sets=filter_call_parameter_sets,
                    spec_resolution_lookup_so_far=resolved_spec_lookup_so_far,
                )
            )

        resolutions: List[WhereFilterSpecResolution] = []
        for call_parameter_set, spec_pattern in call_parameter_set_to_pattern.items():
            group_by_item_resolution = group_by_item_resolver.resolve_matching_item_for_where_filter(
                spec_pattern=spec_pattern,
                resolution_node=resolution_node,
            )
            issue_sets_to_merge.append(group_by_item_resolution.issue_set)
            if group_by_item_resolution.spec is None:
                continue
            resolutions.append(
                WhereFilterSpecResolution(
                    lookup_key=ResolvedSpecLookUpKey(
                        filter_location=filter_location,
                        call_parameter_set=call_parameter_set,
                    ),
                    resolution_path=resolution_path,
                    resolved_spec=group_by_item_resolution.spec,
                )
            )

        results_to_merge.append(
            FilterSpecResolutionLookUp(
                spec_resolutions=tuple(resolutions),
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
        )

        return FilterSpecResolutionLookUp.merge_iterable(results_to_merge).merge(
            FilterSpecResolutionLookUp(
                spec_resolutions=(),
                issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
            )
        )


class WhereFilterSpecResolver:
    def __init__(  # noqa: D
        self,
        manifest_lookup: SemanticManifestLookup,
        resolution_dag: GroupByItemResolutionDag,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolution_dag = resolution_dag

    def resolve_lookup(self) -> FilterSpecResolutionLookUp:
        visitor = _ResolveWhereFilterSpecVisitor(manifest_lookup=self._manifest_lookup)

        return self._resolution_dag.sink_node.accept(visitor)
