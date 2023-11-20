from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.references import EntityReference, MetricReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.protocols.query_parameter import (
    DimensionOrEntityQueryParameter,
    InputOrderByParameter,
    TimeDimensionQueryParameter,
)
from metricflow.protocols.query_parameter import SavedQueryParameter as SavedQueryParameterProtocol
from metricflow.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForGroupBy,
    ResolverInputForMetric,
    ResolverInputForOrderBy,
)
from metricflow.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)


@dataclass(frozen=True)
class TimeDimensionParameter(ProtocolHint[TimeDimensionQueryParameter]):
    """Time dimension requested in a query."""

    def _implements_protocol(self) -> TimeDimensionQueryParameter:
        return self

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        parsed_name = StructuredLinkableSpecName.from_name(self.name)
        if parsed_name.time_granularity:
            raise ValueError("Must use object syntax for `grain` parameter if `date_part` is requested.")

    @property
    def query_resolver_input(self) -> ResolverInputForGroupBy:
        fields_to_compare = [
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        ]
        if self.grain is not None:
            fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

        name_structure = StructuredLinkableSpecName.from_name(self.name.lower())

        return ResolverInputForGroupBy(
            input_obj=self,
            naming_scheme=ObjectBuilderNamingScheme(),
            spec_pattern=EntityLinkPattern(
                EntityLinkPatternParameterSet(
                    fields_to_compare=tuple(fields_to_compare),
                    element_name=name_structure.element_name,
                    entity_links=tuple(EntityReference(link_name) for link_name in name_structure.entity_link_names),
                    time_granularity=self.grain,
                    date_part=self.date_part,
                )
            ),
        )


@dataclass(frozen=True)
class DimensionOrEntityParameter(ProtocolHint[DimensionOrEntityQueryParameter]):
    """Group by parameter requested in a query.

    Might represent an entity or a dimension.
    """

    name: str

    @override
    def _implements_protocol(self) -> DimensionOrEntityQueryParameter:
        return self

    @property
    def query_resolver_input(self) -> ResolverInputForGroupBy:  # noqa: D
        name_structure = StructuredLinkableSpecName.from_name(self.name.lower())

        return ResolverInputForGroupBy(
            input_obj=self,
            naming_scheme=ObjectBuilderNamingScheme(),
            spec_pattern=EntityLinkPattern(
                EntityLinkPatternParameterSet(
                    fields_to_compare=(ParameterSetField.ELEMENT_NAME, ParameterSetField.ENTITY_LINKS),
                    element_name=name_structure.element_name,
                    entity_links=tuple(EntityReference(link_name) for link_name in name_structure.entity_link_names),
                    time_granularity=None,
                    date_part=None,
                )
            ),
        )


@dataclass(frozen=True)
class MetricParameter:
    """Metric requested in a query."""

    name: str

    @property
    def query_resolver_input(self) -> ResolverInputForMetric:
        return ResolverInputForMetric(
            input_obj=self,
            metric_reference=MetricReference(element_name=self.name.lower()),
        )


@dataclass(frozen=True)
class OrderByParameter:
    """Order by requested in a query."""

    order_by: InputOrderByParameter
    descending: bool = False

    @property
    def ui_description(self) -> str:
        if self.descending:
            return f"-{self.order_by}"
        return f"{self.order_by}"

    @property
    def query_resolver_input(self) -> ResolverInputForOrderBy:
        return ResolverInputForOrderBy(
            input_item_to_order=self.order_by.query_resolver_input,
            descending=self.descending,
        )


@dataclass(frozen=True)
class SavedQueryParameter(ProtocolHint[SavedQueryParameterProtocol]):
    """Dataclass implementation of SavedQueryParameterProtocol."""

    name: str

    @override
    def _implements_protocol(self) -> SavedQueryParameterProtocol:
        return self
