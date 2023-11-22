from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow.query.group_by_item.resolution_nodes.any_model_resolution_node import AnyModelGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_nodes.measure_resolution_node import MeasureGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.metric_resolution_node import MetricGroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.issues.no_common import NoCommonItemsInParents
from metricflow.query.issues.no_matching_at_root import NoMatchingGroupByItemsAtRoot
from metricflow.query.issues.no_matching_none_date_part import NoCandidatesWithNoneDatePartIssue
from metricflow.specs.patterns.base_time_grain import BaseTimeGrainPattern
from metricflow.specs.patterns.none_date_part import NoneDatePartPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PushDownResult:
    candidate_set: GroupByItemCandidateSet
    issue_set: MetricFlowQueryResolutionIssueSet

    def __post_init__(self) -> None:  # noqa: D
        # If there are no errors, there should be a candidate spec in each candidate set.
        # If there are errors, there shouldn't be any candidate sets.
        assert (not self.issue_set.has_errors and not self.candidate_set.is_empty) or (
            self.issue_set.has_errors and self.candidate_set.is_empty
        )

    def filter_candidates_by_pattern(self, spec_pattern: SpecPattern) -> PushDownResult:
        return PushDownResult(
            candidate_set=self.candidate_set.filter_candidates_by_pattern(spec_pattern),
            issue_set=self.issue_set,
        )

    def filter_candidates_by_patterns(self, spec_patterns: Sequence[SpecPattern]) -> PushDownResult:
        if len(spec_patterns) == 0:
            return self

        candidate_set = self.candidate_set
        for spec_pattern in spec_patterns:
            candidate_set = candidate_set.filter_candidates_by_pattern(spec_pattern)

        return PushDownResult(
            candidate_set=candidate_set,
            issue_set=self.issue_set,
        )


class DagTraversalPathTracker:
    def __init__(self) -> None:  # noqa: D
        self._current_path: List[GroupByItemResolutionNode] = []

    @contextmanager
    def track_node_visit(self, node: GroupByItemResolutionNode) -> Iterator[MetricFlowQueryResolutionPath]:
        self._current_path.append(node)
        yield MetricFlowQueryResolutionPath(tuple(self._current_path))
        self._current_path.pop(-1)


class _PushDownGroupByItemCandidatesVisitor(GroupByItemResolutionNodeVisitor[PushDownResult]):
    def __init__(  # noqa: D
        self,
        manifest_lookup: SemanticManifestLookup,
        source_spec_patterns: Sequence[SpecPattern] = (),
        with_any_property: Optional[Set[LinkableElementProperties]] = None,
        without_any_property: Optional[Set[LinkableElementProperties]] = None,
    ) -> None:
        self._semantic_manifest_lookup = manifest_lookup
        self._source_spec_patterns = tuple(source_spec_patterns)
        self._path_from_start_node_tracker = DagTraversalPathTracker()
        self._with_any_property = with_any_property
        self._without_any_property = without_any_property

    @override
    def visit_measure_node(self, node: MeasureGroupByItemResolutionNode) -> PushDownResult:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            logger.info(f"Handling {node.ui_description}")
            specs_available_for_measure: Sequence[LinkableInstanceSpec] = tuple(
                self._semantic_manifest_lookup.metric_lookup.group_by_item_specs_for_measure(
                    measure_reference=node.measure_reference,
                    with_any_of=self._with_any_property,
                    without_any_of=self._without_any_property,
                )
            )
            metric = self._semantic_manifest_lookup.metric_lookup.get_metric(node.child_metric_reference)

            patterns_to_apply: Tuple[SpecPattern, ...] = ()
            if metric.type is MetricType.SIMPLE or metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
                pass
            elif metric.type is MetricType.CUMULATIVE:
                # To handle the restriction that cumulative metrics can only be queried at the base grain, it's
                # easiest to handle that by applying the pattern to remove non-base grain time dimension specs at the
                # measure node and generate the issue here if there's nothing that matches.
                # This can be more cleanly handled once we add additional context to the LinkableInstanceSpec.
                patterns_to_apply = (
                    # From comment in ValidLinkableSpecResolver:
                    #   It's possible to aggregate measures to coarser time granularities
                    #   (except with cumulative metrics).
                    BaseTimeGrainPattern(only_apply_for_metric_time=True),
                    # From comment in previous query parser:
                    #   Cannot extract date part for cumulative metrics.
                    NoneDatePartPattern(),
                )
            else:
                assert_values_exhausted(metric.type)

            patterns_to_apply = patterns_to_apply + self._source_spec_patterns
            matching_specs = specs_available_for_measure

            for pattern_to_apply in patterns_to_apply:
                matching_specs = pattern_to_apply.match(matching_specs)

            logger.info(
                f"For {node.ui_description}:\n"
                + indent_log_line(
                    "After applying patterns:\n"
                    + indent_log_line(mf_pformat(patterns_to_apply))
                    + "\n"
                    + "to inputs, matches are:\n"
                    + indent_log_line(mf_pformat(matching_specs))
                )
            )

            if len(matching_specs) == 0:
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        NoMatchingGroupByItemsAtRoot.create(
                            parent_issues=(),
                            query_resolution_path=current_traversal_path,
                            candidate_specs=specs_available_for_measure,
                        )
                    ),
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    measure_paths=(current_traversal_path,),
                    specs=tuple(matching_specs),
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet(),
            )

    def _merge_push_down_results_from_parents(
        self,
        push_down_results_from_parents: Dict[GroupByItemResolutionNode, PushDownResult],
        current_traversal_path: MetricFlowQueryResolutionPath,
    ) -> PushDownResult:
        merged_issue_set: MetricFlowQueryResolutionIssueSet = MetricFlowQueryResolutionIssueSet.merge_iterable(
            parent_candidate_set.issue_set for parent_candidate_set in push_down_results_from_parents.values()
        )

        if merged_issue_set.has_errors:
            return PushDownResult(
                candidate_set=GroupByItemCandidateSet.empty_instance(),
                issue_set=merged_issue_set,
            )

        parent_candidate_sets = tuple(
            parent_candidate_set.candidate_set for parent_candidate_set in push_down_results_from_parents.values()
        )
        intersected_candidate_set = GroupByItemCandidateSet.intersection(
            path_from_leaf_node=current_traversal_path, candidate_sets=parent_candidate_sets
        )

        if intersected_candidate_set.is_empty:
            return PushDownResult(
                candidate_set=intersected_candidate_set,
                issue_set=merged_issue_set.add_issue(
                    NoCommonItemsInParents.create(
                        query_resolution_path=current_traversal_path,
                        parent_node_to_candidate_set={
                            parent_node: push_down_result.candidate_set
                            for parent_node, push_down_result in push_down_results_from_parents.items()
                        },
                        parent_issues=(),
                    )
                ),
            )
        return PushDownResult(
            candidate_set=intersected_candidate_set,
            issue_set=merged_issue_set,
        )

    @override
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> PushDownResult:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            merged_result_from_parents = self._merge_push_down_results_from_parents(
                push_down_results_from_parents={
                    parent_node: parent_node.accept(self) for parent_node in node.parent_nodes
                },
                current_traversal_path=current_traversal_path,
            )
            logger.info(
                f"Handling {node.ui_description} with candidates from parents:\n"
                + indent_log_line(mf_pformat(merged_result_from_parents.candidate_set.specs))
            )
            if merged_result_from_parents.candidate_set.is_empty:
                return merged_result_from_parents

            metric = self._semantic_manifest_lookup.metric_lookup.get_metric(node.metric_reference)

            patterns_to_apply: Sequence[SpecPattern] = ()
            if metric.type is MetricType.SIMPLE or metric.type is MetricType.CUMULATIVE:
                pass
            elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
                for input_metric in metric.input_metrics:
                    if input_metric.offset_to_grain:
                        # From comment in previous query parser:
                        # "Cannot extract date part for metrics with offset_to_grain."
                        patterns_to_apply = (NoneDatePartPattern(),)
                        break
            else:
                assert_values_exhausted(metric.type)

            candidate_specs: Sequence[LinkableInstanceSpec] = merged_result_from_parents.candidate_set.specs
            issue_sets_to_merge = [merged_result_from_parents.issue_set]

            if len(patterns_to_apply) == 0:
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet(
                        specs=tuple(candidate_specs),
                        measure_paths=merged_result_from_parents.candidate_set.measure_paths,
                        path_from_leaf_node=current_traversal_path,
                    ),
                    issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
                )

            matched_specs = candidate_specs
            for pattern_to_apply in patterns_to_apply:
                matched_specs = pattern_to_apply.match(matched_specs)

            logger.info(
                f"For {node.ui_description}:\n"
                + indent_log_line(
                    "After applying patterns:\n"
                    + indent_log_line(mf_pformat(patterns_to_apply))
                    + "\n"
                    + "to inputs, outputs are:\n"
                    + indent_log_line(mf_pformat(matched_specs))
                )
            )

            if len(matched_specs) == 0:
                issue_sets_to_merge.append(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        NoCandidatesWithNoneDatePartIssue.create(
                            query_resolution_path=current_traversal_path,
                            candidate_specs=candidate_specs,
                            parent_issues=(),
                        )
                    )
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    specs=tuple(matched_specs),
                    measure_paths=merged_result_from_parents.candidate_set.measure_paths
                    if len(matched_specs) > 0
                    else (),
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
            )

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> PushDownResult:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            merged_result_from_parents = self._merge_push_down_results_from_parents(
                push_down_results_from_parents={
                    parent_node: parent_node.accept(self) for parent_node in node.parent_nodes
                },
                current_traversal_path=current_traversal_path,
            )

            logger.info(
                f"Handling {node.ui_description} with candidates from parents:\n"
                + indent_log_line(mf_pformat(merged_result_from_parents.candidate_set.specs))
            )

            return merged_result_from_parents

    @override
    def visit_any_model_node(self, node: AnyModelGroupByItemResolutionNode) -> PushDownResult:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            logger.info(f"Handling {node.ui_description}")
            # This is a case for distinct dimension values from semantic models.
            candidate_specs = self._semantic_manifest_lookup.metric_lookup.group_by_item_specs_for_no_metrics_query()
            matched_specs = candidate_specs
            for spec_pattern in self._source_spec_patterns:
                matched_specs = spec_pattern.match(matched_specs)

            if len(matched_specs) == 0:
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        NoMatchingGroupByItemsAtRoot.create(
                            parent_issues=(),
                            query_resolution_path=current_traversal_path,
                            candidate_specs=candidate_specs,
                        ),
                    ),
                )
            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    specs=tuple(matched_specs),
                    measure_paths=(current_traversal_path,),
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
