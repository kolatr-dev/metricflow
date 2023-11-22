from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple, Union

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.protocols.query_parameter import GroupByParameter, MetricQueryParameter
from metricflow.specs.patterns.spec_pattern import SpecPattern


class MetricFlowQueryResolverInput(ABC):
    @property
    @abstractmethod
    def ui_description(self) -> str:
        raise NotImplementedError

    def ui_description_naming_scheme(self) -> Optional[QueryItemNamingScheme]:
        return None


# class ResolverInputForMetric(MetricFlowQueryResolverInput, ABC):
#     @property
#     @abstractmethod
#     def metric_reference(self) -> MetricReference:
#         raise NotImplementedError


@dataclass(frozen=True)
class ResolverInputForMetric(MetricFlowQueryResolverInput):
    input_obj: Union[MetricQueryParameter, str]
    metric_reference: MetricReference

    @property
    @override
    def ui_description(self) -> str:
        return repr(self.input_obj)


# class ResolverInputForGroupBy(MetricFlowQueryResolverInput, ABC):
#     @property
#     @abstractmethod
#     def naming_scheme(self) -> QueryItemNamingScheme:
#         raise NotImplementedError
#
#     @property
#     @abstractmethod
#     def spec_pattern(self) -> SpecPattern:
#         raise NotImplementedError


@dataclass(frozen=True)
class ResolverInputForGroupBy(MetricFlowQueryResolverInput):
    input_obj: Union[GroupByParameter, str]
    naming_scheme: QueryItemNamingScheme
    spec_pattern: SpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return repr(self.input_obj)

    @override
    def ui_description_naming_scheme(self) -> Optional[QueryItemNamingScheme]:
        return self.naming_scheme


@dataclass(frozen=True)
class ResolverInputForOrderBy(MetricFlowQueryResolverInput):
    """Describes the order direction for one of the metrics or group by items."""

    input_item_to_order: Union[ResolverInputForMetric, ResolverInputForGroupBy, NonMatchingInput]
    descending: bool

    @property
    @override
    def ui_description(self) -> str:
        return self.input_item_to_order.ui_description

    @override
    def ui_description_naming_scheme(self) -> Optional[QueryItemNamingScheme]:
        return self.input_item_to_order.ui_description_naming_scheme()

    # @property
    # @abstractmethod
    # def input_item(self) -> Union[ResolverInputForMetric, ResolverInputForGroupBy, InvalidStringForOrderBy]:
    #     raise NotImplementedError
    #
    # @property
    # @abstractmethod
    # def descending(self) -> bool:
    #     raise NotImplementedError


@dataclass(frozen=True)
class NonMatchingInput(MetricFlowQueryResolverInput):
    input_obj: str

    @property
    def ui_description(self) -> str:
        return repr(self.input_obj)

    @property
    def naming_scheme(self) -> Optional[QueryItemNamingScheme]:
        return None


@dataclass(frozen=True)
class ResolverInputForLimit(MetricFlowQueryResolverInput):
    limit: Optional[int]

    @property
    @override
    def ui_description(self) -> str:
        return str(self.limit)

    # @property
    # @abstractmethod
    # def limit(self) -> Optional[int]:
    #     raise NotImplementedError


@dataclass(frozen=True)
class ResolverInputForWhereFilterIntersection(MetricFlowQueryResolverInput):
    where_filter_intersection: WhereFilterIntersection

    @property
    @override
    def ui_description(self) -> str:
        # TODO: Improve description.
        return (
            "WhereFilter("
            + mf_pformat(
                [where_filter.where_sql_template for where_filter in self.where_filter_intersection.where_filters]
            )
            + ")"
        )

    def ui_description_naming_scheme(self) -> Optional[QueryItemNamingScheme]:
        return ObjectBuilderNamingScheme()


@dataclass(frozen=True)
class ResolverInputForQuery(MetricFlowQueryResolverInput):
    metric_inputs: Tuple[ResolverInputForMetric, ...]
    group_by_item_inputs: Tuple[ResolverInputForGroupBy, ...]
    filter_input: ResolverInputForWhereFilterIntersection
    order_by_item_inputs: Tuple[ResolverInputForOrderBy, ...]
    limit_input: ResolverInputForLimit

    @property
    @override
    def ui_description(self) -> str:
        return (
            f"Query({repr([metric_input.metric_reference.element_name for metric_input in self.metric_inputs])}, "
            f"{repr([group_by_item_input.input_obj for group_by_item_input in self.group_by_item_inputs])}"
        )
