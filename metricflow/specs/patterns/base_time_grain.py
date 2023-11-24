from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Set

from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow.specs.patterns.metric_time_pattern import MetricTimePattern
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    LinkableInstanceSpec,
    LinkableSpecSet,
    TimeDimensionSpec,
    TimeDimensionSpecComparisonKey,
    TimeDimensionSpecField,
)


class BaseTimeGrainPattern(SpecPattern):
    def __init__(self, only_apply_for_metric_time: bool = False) -> None:
        """Initializer.

        Args:
            only_apply_for_metric_time: If set, only remove time dimension specs with a non-base grain if it's for
            metric time.
            TODO: Remove this and use a composition of patterns.
        """
        self._only_apply_for_metric_time = only_apply_for_metric_time

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        if self._only_apply_for_metric_time:
            metric_time_specs = MetricTimePattern().match(candidate_specs)
            other_specs = tuple(spec for spec in candidate_specs if spec not in metric_time_specs)

            return other_specs + tuple(BaseTimeGrainPattern(only_apply_for_metric_time=False).match(metric_time_specs))

        spec_set = LinkableSpecSet.from_specs(InstanceSpecSet.from_specs(candidate_specs).linkable_specs)

        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[TimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, List[TimeDimensionSpec]] = defaultdict(list)
        for time_dimension_spec in spec_set.time_dimension_specs:
            spec_key = time_dimension_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(time_dimension_spec.time_granularity)
            spec_key_to_specs[spec_key].append(time_dimension_spec)

        matched_time_dimension_specs: List[TimeDimensionSpec] = []
        for spec_key, time_grains in spec_key_to_grains.items():
            matched_time_dimension_specs.append(spec_key_to_specs[spec_key][0].with_grain(min(time_grains)))

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs + tuple(matched_time_dimension_specs) + spec_set.entity_specs
        )

        return matching_specs
