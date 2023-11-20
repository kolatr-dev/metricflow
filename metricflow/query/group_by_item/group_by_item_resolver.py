from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Union

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import (
    PushDownResult,
    _PushDownGroupByItemCandidatesVisitor,
)
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)
from metricflow.specs.patterns.base_time_grain import BaseTimeGrainPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec


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
        resolution_node: Optional[Union[MetricGroupByItemResolutionNode, QueryGroupByItemResolutionNode]] = None,
    ) -> GroupByItemResolution:
        if resolution_node is None:
            resolution_node = self._resolution_dag.sink_node

        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(spec_pattern,),
        )

        push_down_result: PushDownResult = resolution_node.accept(push_down_visitor)

        if push_down_result.candidate_set.num_candidates == 0:
            return GroupByItemResolution(
                spec=None,
                # TODO: Add a parent issue set
                issue_set=push_down_result.issue_set,
            )

        push_down_result = push_down_result.filter_candidates_by_pattern(
            BaseTimeGrainPattern(),
        )

        if push_down_result.candidate_set.num_candidates > 1:
            # TODO: Implement
            raise NotImplementedError(f"Need to handle result:\n{mf_pformat(push_down_result)}")

        return GroupByItemResolution(spec=push_down_result.candidate_set.specs[0], issue_set=push_down_result.issue_set)

    def resolve_matching_item_for_where_filter(
        self,
        spec_pattern: SpecPattern,
        resolution_node: Union[MetricGroupByItemResolutionNode, QueryGroupByItemResolutionNode],
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
            # TODO: Implement
            raise NotImplementedError(f"Need to handle result: {push_down_result}")

        return GroupByItemResolution(spec=push_down_result.candidate_set.specs[0], issue_set=push_down_result.issue_set)

    def resolve_available_items(
        self,
    ) -> AvailableGroupByItemsResolution:
        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(),
        )

        push_down_result: PushDownResult = self._resolution_dag.sink_node.accept(push_down_visitor)

        return AvailableGroupByItemsResolution(
            specs=push_down_result.candidate_set.specs,
            issue_set=push_down_result.issue_set,
        )
