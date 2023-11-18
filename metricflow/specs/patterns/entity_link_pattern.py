from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec, LinkableSpecSet, TimeDimensionSpec

logger = logging.getLogger(__name__)


class ParameterSetField(Enum):
    """The fields of the EntityLinkPatternParameterSet classes used for matching in the EntityLinkPattern."""

    ELEMENT_NAME = "element_name"
    ENTITY_LINKS = "entity_links"
    TIME_GRANULARITY = "time_granularity"
    DATE_PART = "date_part"


@dataclass(frozen=True)
class EntityLinkPatternParameterSet:
    """See EntityPathPattern for more details."""

    fields_to_compare: Tuple[ParameterSetField, ...]

    # The name of the element in the semantic model
    element_name: Optional[str] = None
    # The entities used for joining semantic models.
    entity_links: Optional[Tuple[EntityReference, ...]] = None
    # If specified, match only time dimensions with the following properties.
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None


@dataclass(frozen=True)
class EntityLinkPattern(SpecPattern):
    """A pattern that matches group-by-items using the entity link path specification.

    The entity link path specifies how a group-by-item for a metric query should be constructed. The group-by-item
    is obtained by joining the semantic model containing the measure to a semantic model containing the group-by-
    item using a specified entity. Additional semantic models can be joined using additional entities to obtain the
    group-by-item. The series of entities that are used form the entity path. Since the entity path does not specify
    which semantic models need to be used, additional resolution is done in later stages to generate the necessary SQL.
    """

    parameter_set: EntityLinkPatternParameterSet

    @override
    def match(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        matching_specs: List[LinkableInstanceSpec] = []

        # Using some Python introspection magic to figure out specs that match the listed fields.
        keys_to_keep = set(field_to_compare.value for field_to_compare in self.parameter_set.fields_to_compare)
        pattern_parameters_as_dict = asdict(self.parameter_set)
        # Checks that EntityLinkPatternParameterSetField is valid wrt to the parameter set.
        for key_to_keep in keys_to_keep:
            if key_to_keep not in pattern_parameters_as_dict:
                raise RuntimeError(f"{key_to_keep} is not a valid field in {self.parameter_set}")

        for spec in candidate_specs:
            spec_as_dict = {key: value for key, value in asdict(spec).items() if key in keys_to_keep}
            pattern_parameters_as_dict = {
                key: value for key, value in pattern_parameters_as_dict.items() if key in keys_to_keep
            }

            if spec_as_dict == pattern_parameters_as_dict:
                matching_specs.append(spec)

        return matching_specs


@dataclass(frozen=True)
class DimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches dimensions / time dimensions."""

    @override
    def match(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = LinkableSpecSet.from_specs(candidate_specs)
        filtered_specs: Sequence[LinkableInstanceSpec] = spec_set.dimension_specs + spec_set.time_dimension_specs
        return super().match(filtered_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D
        dimension_call_parameter_set: DimensionCallParameterSet,
    ) -> DimensionPattern:
        return DimensionPattern(
            parameter_set=EntityLinkPatternParameterSet(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=dimension_call_parameter_set.dimension_reference.element_name,
                entity_links=dimension_call_parameter_set.entity_path,
            )
        )


@dataclass(frozen=True)
class TimeDimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches time dimensions."""

    @override
    def match(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = LinkableSpecSet.from_specs(candidate_specs)
        return super().match(spec_set.time_dimension_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D
        time_dimension_call_parameter_set: TimeDimensionCallParameterSet,
    ) -> TimeDimensionPattern:
        fields_to_compare: List[ParameterSetField] = [
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
        ]

        if time_dimension_call_parameter_set.time_granularity is not None:
            fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

        return TimeDimensionPattern(
            parameter_set=EntityLinkPatternParameterSet(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                entity_links=time_dimension_call_parameter_set.entity_path,
                time_granularity=time_dimension_call_parameter_set.time_granularity,
            )
        )

    @staticmethod
    def from_time_dimension_spec(time_dimension_spec: TimeDimensionSpec) -> TimeDimensionPattern:  # noqa: D
        return TimeDimensionPattern(
            parameter_set=EntityLinkPatternParameterSet(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                    ParameterSetField.TIME_GRANULARITY,
                ),
                element_name=time_dimension_spec.element_name,
                entity_links=time_dimension_spec.entity_links,
                time_granularity=time_dimension_spec.time_granularity,
            )
        )


@dataclass(frozen=True)
class EntityPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches entities."""

    @override
    def match(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = LinkableSpecSet.from_specs(candidate_specs)
        return super().match(spec_set.entity_specs)

    @staticmethod
    def from_call_parameter_set(entity_call_parameter_set: EntityCallParameterSet) -> DimensionPattern:  # noqa: D
        return DimensionPattern(
            parameter_set=EntityLinkPatternParameterSet(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=entity_call_parameter_set.entity_reference.element_name,
                entity_links=entity_call_parameter_set.entity_path,
            )
        )
