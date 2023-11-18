# from __future__ import annotations
#
# from typing import Sequence
#
# from typing_extensions import override
#
# from metricflow.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
# from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import (
#     PushDownResult,
#     PushDownResultMergeBehavior,
# )
# from metricflow.query.issues.insufficient_specificity import InsufficientSpecificityForGroupByItem
# from metricflow.query.issues.issues_base import MetricFlowQueryIssueSet, MetricFlowQueryResolutionPath
# from metricflow.specs.patterns.spec_pattern import SpecPattern
# from metricflow.specs.specs import LinkableInstanceSpec
#
#
# class MergeCandidatesForWhereBehavior(PushDownResultMergeBehavior):
#     def _common_specs_in_candidate_sets(
#         self, candidate_sets: Sequence[GroupByItemCandidateSet]
#     ) -> Sequence[LinkableInstanceSpec]:
#         raise NotImplementedError
#
#     def _all_specs_in_candidate_sets(
#         self, candidate_sets: Sequence[GroupByItemCandidateSet]
#     ) -> Sequence[LinkableInstanceSpec]:
#         raise NotImplementedError
#
#     @override
#     def merge_push_down_candidate_sets_from_parents(
#         self,
#         spec_pattern: SpecPattern,
#         query_resolution_path: MetricFlowQueryResolutionPath,
#         push_down_results_from_parents: Sequence[PushDownResult],
#     ) -> PushDownResult:
#         parent_candidate_sets = []
#
#         for push_down_result_from_parent in push_down_results_from_parents:
#             for parent_candidate_set in push_down_result_from_parent.candidate_sets:
#                 parent_candidate_sets.append(parent_candidate_set)
#
#         specs_same_in_candidate_sets = set(self._common_specs_in_candidate_sets(parent_candidate_sets)) == set(
#             self._all_specs_in_candidate_sets(parent_candidate_sets)
#         )
#
#         if specs_same_in_candidate_sets:
#             return PushDownResult(
#                 candidate_sets=parent_candidate_sets,
#                 issue_set=MetricFlowQueryIssueSet(),
#             )
#
#         return PushDownResult(
#             candidate_sets=(),
#             issue_set=MetricFlowQueryIssueSet(
#                 issues=(
#                     InsufficientSpecificityForGroupByItem.create(
#                         spec_pattern=spec_pattern,
#                         candidate_sets=parent_candidate_sets,
#                     ),
#                 )
#             ),
#         )
