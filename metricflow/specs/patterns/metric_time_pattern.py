from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    LinkableSpecSet,
    TimeDimensionSpec,
)


class MetricTimePattern(SpecPattern):
    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[TimeDimensionSpec]:
        spec_set = LinkableSpecSet.from_specs(InstanceSpecSet.from_specs(candidate_specs).linkable_specs)
        return tuple(
            time_dimension_spec
            for time_dimension_spec in spec_set.time_dimension_specs
            if time_dimension_spec.element_name == METRIC_TIME_ELEMENT_NAME
        )
