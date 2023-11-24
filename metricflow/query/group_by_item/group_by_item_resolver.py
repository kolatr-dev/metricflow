from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.call_parameter_sets import TimeDimensionCallParameterSet
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import (
    PushDownResult,
    _PushDownGroupByItemCandidatesVisitor,
)
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag, ResolutionDagSinkNode
from metricflow.query.issues.ambiguous_group_by_item import AmbiguousGroupByItemIssue
from metricflow.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
    MetricFlowQueryResolutionPath,
)
from metricflow.specs.patterns.base_time_grain import BaseTimeGrainPattern
from metricflow.specs.patterns.entity_link_pattern import TimeDimensionPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec, LinkableSpecSet


@dataclass(frozen=True)
class GroupByItemResolution:
    spec: Optional[LinkableInstanceSpec]
    issue_set: MetricFlowQueryResolutionIssueSet


@dataclass(frozen=True)
class AvailableGroupByItemsResolution:
    specs: Tuple[LinkableInstanceSpec, ...]
    issue_set: MetricFlowQueryResolutionIssueSet


class GroupByItemResolver:
    def __init__(  # noqa: D
        self,
        manifest_lookup: SemanticManifestLookup,
        resolution_dag: GroupByItemResolutionDag,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolution_dag = resolution_dag

    def resolve_matching_item_for_querying(
        self,
        spec_pattern: SpecPattern,
    ) -> GroupByItemResolution:
        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(spec_pattern,),
        )

        push_down_result: PushDownResult = self._resolution_dag.sink_node.accept(push_down_visitor)

        if push_down_result.candidate_set.num_candidates == 0:
            return GroupByItemResolution(
                spec=None,
                issue_set=push_down_result.issue_set,
            )

        push_down_result = push_down_result.filter_candidates_by_pattern(
            BaseTimeGrainPattern(),
        )
        if push_down_result.candidate_set.num_candidates > 1:
            return GroupByItemResolution(
                spec=None,
                issue_set=push_down_result.issue_set.merge(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        AmbiguousGroupByItemIssue.create(
                            candidate_set=push_down_result.candidate_set,
                            query_resolution_path=MetricFlowQueryResolutionPath.from_path_item(
                                self._resolution_dag.sink_node
                            ),
                        )
                    )
                ),
            )

        return GroupByItemResolution(spec=push_down_result.candidate_set.specs[0], issue_set=push_down_result.issue_set)

    def resolve_matching_item_for_where_filter(
        self,
        spec_pattern: SpecPattern,
        resolution_node: ResolutionDagSinkNode,
    ) -> GroupByItemResolution:
        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(
                BaseTimeGrainPattern(),
                spec_pattern,
            ),
        )

        push_down_result: PushDownResult = resolution_node.accept(push_down_visitor)

        if push_down_result.candidate_set.num_candidates == 0:
            return GroupByItemResolution(
                spec=None,
                issue_set=push_down_result.issue_set,
            )

        if push_down_result.candidate_set.num_candidates > 1:
            return GroupByItemResolution(
                spec=None,
                issue_set=push_down_result.issue_set.merge(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        AmbiguousGroupByItemIssue.create(
                            candidate_set=push_down_result.candidate_set,
                            query_resolution_path=MetricFlowQueryResolutionPath.from_path_item(
                                self._resolution_dag.sink_node
                            ),
                        )
                    )
                ),
            )

        return GroupByItemResolution(spec=push_down_result.candidate_set.specs[0], issue_set=push_down_result.issue_set)

    def resolve_available_items(
        self,
        resolution_node: Optional[ResolutionDagSinkNode] = None,
    ) -> AvailableGroupByItemsResolution:
        if resolution_node is None:
            resolution_node = self._resolution_dag.sink_node

        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(),
        )

        push_down_result: PushDownResult = resolution_node.accept(push_down_visitor)

        return AvailableGroupByItemsResolution(
            specs=push_down_result.candidate_set.specs,
            issue_set=push_down_result.issue_set,
        )

    def resolve_metric_time_grain(self) -> TimeGranularity:
        metric_time_grain_resolution = self.resolve_matching_item_for_querying(
            spec_pattern=TimeDimensionPattern.from_call_parameter_set(
                TimeDimensionCallParameterSet(
                    entity_path=(),
                    time_dimension_reference=TimeDimensionReference(element_name=METRIC_TIME_ELEMENT_NAME),
                )
            ),
        )
        metric_time_spec_set = (
            LinkableSpecSet.from_specs((metric_time_grain_resolution.spec,))
            if metric_time_grain_resolution.spec is not None
            else LinkableSpecSet.empty_instance()
        )
        if len(metric_time_spec_set.time_dimension_specs) != 1:
            raise RuntimeError(
                f"The grain for {repr(METRIC_TIME_ELEMENT_NAME)} could not be resolved. Got issues:\n\n"
                f"{indent_log_line(mf_pformat(metric_time_grain_resolution.issue_set))}"
            )
        return metric_time_spec_set.time_dimension_specs[0].time_granularity
