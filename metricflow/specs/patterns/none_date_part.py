from __future__ import annotations

from typing import List, Sequence

from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import (
    LinkableInstanceSpec,
    LinkableSpecSet,
    LinkableSpecSetTransform,
)


class NoneDatePartPattern(SpecPattern):
    def match(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        return _RetainOnlyNoneDatePartTransform().transform(LinkableSpecSet.from_specs(candidate_specs))


class _RetainOnlyNoneDatePartTransform(LinkableSpecSetTransform[Sequence[LinkableInstanceSpec]]):
    @override
    def transform(self, spec_set: LinkableSpecSet) -> Sequence[LinkableInstanceSpec]:
        specs_to_return: List[LinkableInstanceSpec] = []

        for time_dimension_spec in spec_set.time_dimension_specs:
            if time_dimension_spec.date_part is None:
                specs_to_return.append(time_dimension_spec)
        specs_to_return.extend(spec_set.dimension_specs)
        specs_to_return.extend(spec_set.entity_specs)

        return specs_to_return
