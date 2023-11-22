from __future__ import annotations

from typing import List, Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.query.group_by_item.resolve_filters.filter_to_pattern import (
    ResolvedSpecApplicabilityKey,
    ResolvedSpecLookup,
)
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import LinkableInstanceSpec


class WhereFilterRenderer:
    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        resolved_spec_lookup: ResolvedSpecLookup,
        metric_references: Sequence[MetricReference],
    ) -> None:
        self._column_associated_resolver = column_association_resolver
        self._resolved_spec_lookup = resolved_spec_lookup
        self._metric_references = tuple(sorted(metric_references))
        self._rendered_specs: List[LinkableInstanceSpec] = []

    def dimension_call_jinja_function(self, name: str, entity_path: Sequence[str] = ()) -> str:
        structured_name = StructuredLinkableSpecName.from_name(name)
        element_name = structured_name.element_name

        if structured_name.entity_prefix is not None:
            entity_path_items = tuple(entity_path) + (structured_name.entity_prefix,)
        else:
            entity_path_items = tuple(entity_path)

        resolved_spec_key = ResolvedSpecApplicabilityKey(
            metric_references=self._metric_references,
            call_parameter_set=DimensionCallParameterSet(
                entity_path=tuple(EntityReference(path_item) for path_item in entity_path_items),
                dimension_reference=DimensionReference(element_name=element_name),
            ),
        )
        mappings = self._resolved_spec_lookup.get_mappings(resolved_spec_key)

        if len(mappings) != 1:
            raise ValueError(
                f"Did not get exactly one spec for: {resolved_spec_key}. Got {mappings}. All mappings are:\n"
                f"{mf_pformat(self._resolved_spec_lookup)}"
            )
        resolved_spec = mappings[0].resolved_spec

        if resolved_spec is None:
            raise RuntimeError(f"Did not get a resolved spec: {mappings[0]}")

        self._rendered_specs.append(resolved_spec)
        return self._column_associated_resolver.resolve_spec(resolved_spec).column_name

    def time_dimension_jinja_function(
        self,
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        date_part_name: Optional[str] = None,
    ) -> str:
        structured_name = StructuredLinkableSpecName.from_name(time_dimension_name)
        element_name = structured_name.element_name

        if structured_name.entity_prefix is not None:
            entity_path_items = tuple(entity_path) + (structured_name.entity_prefix,)
        else:
            entity_path_items = tuple(entity_path)

        resolved_spec_key = ResolvedSpecApplicabilityKey(
            metric_references=self._metric_references,
            call_parameter_set=TimeDimensionCallParameterSet(
                entity_path=tuple(EntityReference(path_item) for path_item in entity_path_items),
                time_dimension_reference=TimeDimensionReference(element_name),
                time_granularity=TimeGranularity(time_granularity_name) if time_granularity_name is not None else None,
                date_part=DatePart(date_part_name) if date_part_name is not None else None,
            ),
        )

        mappings = self._resolved_spec_lookup.get_mappings(resolved_spec_key)

        if len(mappings) != 1:
            raise ValueError(f"Did not get exactly one spec for: {resolved_spec_key}. Got {mappings}")
        resolved_spec = mappings[0].resolved_spec

        if resolved_spec is None:
            raise RuntimeError(f"Did not get a resolved spec: {mappings[0]}")

        self._rendered_specs.append(resolved_spec)
        return self._column_associated_resolver.resolve_spec(resolved_spec).column_name

    def entity_jinja_function(self, name: str, entity_path: Sequence[str] = ()) -> str:
        structured_name = StructuredLinkableSpecName.from_name(name)
        element_name = structured_name.element_name

        if structured_name.entity_prefix is not None:
            entity_path_items = tuple(entity_path) + (structured_name.entity_prefix,)
        else:
            entity_path_items = tuple(entity_path)

        resolved_spec_key = ResolvedSpecApplicabilityKey(
            metric_references=self._metric_references,
            call_parameter_set=EntityCallParameterSet(
                entity_path=tuple(EntityReference(path_item) for path_item in entity_path_items),
                entity_reference=EntityReference(element_name=element_name),
            ),
        )
        mappings = self._resolved_spec_lookup.get_mappings(resolved_spec_key)

        if len(mappings) != 1:
            raise ValueError(f"Did not get exactly one spec for: {resolved_spec_key}. Got {mappings}")
        resolved_spec = mappings[0].resolved_spec

        if resolved_spec is None:
            raise RuntimeError(f"Did not get a resolved spec: {mappings[0]}")

        self._rendered_specs.append(resolved_spec)
        return self._column_associated_resolver.resolve_spec(resolved_spec).column_name

    @property
    def rendered_specs(self) -> Sequence[LinkableInstanceSpec]:
        return self._rendered_specs
