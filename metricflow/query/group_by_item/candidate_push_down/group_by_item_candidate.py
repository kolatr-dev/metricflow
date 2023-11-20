from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Iterable, Tuple

from typing_extensions import override

from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionPath
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec, LinkableSpecSet


@dataclass(frozen=True)
class GroupByItemCandidateSet(PathPrefixable):
    specs: Tuple[LinkableInstanceSpec, ...]
    measure_paths: Tuple[MetricFlowQueryResolutionPath, ...]
    path_from_leaf_node: MetricFlowQueryResolutionPath

    def __post_init__(self) -> None:  # noqa: D
        assert (len(self.specs) > 0 and len(self.measure_paths) > 0) or (
            len(self.specs) == 0 and len(self.measure_paths) == 0
        )

    @staticmethod
    def intersection(
        path_from_leaf_node: MetricFlowQueryResolutionPath, candidate_sets: Iterable[GroupByItemCandidateSet]
    ) -> GroupByItemCandidateSet:
        specs_as_sets = tuple(set(candidate_set.specs) for candidate_set in candidate_sets)
        common_specs = set.intersection(*specs_as_sets)
        if len(common_specs) == 0:
            return GroupByItemCandidateSet.empty_instance()

        measure_paths = tuple(
            itertools.chain.from_iterable(candidate_set.measure_paths for candidate_set in candidate_sets)
        )

        return GroupByItemCandidateSet(
            specs=tuple(common_specs), measure_paths=measure_paths, path_from_leaf_node=path_from_leaf_node
        )

    @property
    def is_empty(self) -> bool:  # noqa: D
        return len(self.specs) == 0

    @property
    def num_candidates(self) -> int:  # noqa: D
        return len(self.specs)

    @staticmethod
    def empty_instance() -> GroupByItemCandidateSet:  # noqa: D
        return GroupByItemCandidateSet(
            specs=(), measure_paths=(), path_from_leaf_node=MetricFlowQueryResolutionPath.empty_instance()
        )

    @property
    def spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet.from_specs(self.specs)

    def filter_candidates_by_pattern(
        self,
        spec_pattern: SpecPattern,
    ) -> GroupByItemCandidateSet:
        matching_specs = tuple(spec_pattern.match(self.specs))
        if len(matching_specs) == 0:
            return GroupByItemCandidateSet.empty_instance()

        return GroupByItemCandidateSet(
            specs=matching_specs, measure_paths=self.measure_paths, path_from_leaf_node=self.path_from_leaf_node
        )

    @override
    def with_path_prefix(self, path_prefix: GroupByItemResolutionNode) -> GroupByItemCandidateSet:
        return GroupByItemCandidateSet(
            specs=self.specs,
            measure_paths=tuple(path.with_path_prefix(path_prefix) for path in self.measure_paths),
            path_from_leaf_node=self.path_from_leaf_node.with_path_prefix(path_prefix),
        )
