from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple, Union

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.metric_scheme import MetricNamingScheme
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.protocols.query_parameter import GroupByParameter, MetricQueryParameter, OrderByQueryParameter
from metricflow.specs.patterns.metric_pattern import MetricSpecPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern


@dataclass(frozen=True)
class InputPatternDescription:
    naming_scheme: QueryItemNamingScheme
    spec_pattern: SpecPattern


# class MetricFlowQueryResolverInput(ABC):
#     @property
#     @abstractmethod
#     def ui_description(self) -> str:
#         raise NotImplementedError


class MetricFlowQueryResolverInput(ABC):
    # @property
    # def naming_scheme(self) -> QueryItemNamingScheme:
    #     raise NotImplementedError
    #
    # @property
    # @abstractmethod
    # def spec_pattern(self) -> SpecPattern:
    #     raise NotImplementedError

    @property
    @abstractmethod
    def ui_description(self) -> str:
        raise NotImplementedError

    @property
    def input_pattern_description(self) -> Optional[InputPatternDescription]:
        return None


@dataclass(frozen=True)
class InvalidStringInput(MetricFlowQueryResolverInput):
    input_obj: str

    @property
    def ui_description(self) -> str:
        return self.input_obj


@dataclass(frozen=True)
class ResolverInputForMetric(MetricFlowQueryResolverInput):
    input_obj: Union[MetricQueryParameter, str]
    naming_scheme: MetricNamingScheme
    spec_pattern: MetricSpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)

    @property
    @override
    def input_pattern_description(self) -> InputPatternDescription:
        return InputPatternDescription(
            naming_scheme=self.naming_scheme,
            spec_pattern=self.spec_pattern,
        )


# class ResolverInputForGroupBy(NamedResolverInput, ABC):
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
class ResolverInputForGroupByItem(MetricFlowQueryResolverInput):
    input_obj: Union[GroupByParameter, str]
    input_obj_naming_scheme: QueryItemNamingScheme
    spec_pattern: SpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)

    # @property
    # @override
    # def naming_scheme(self) -> QueryItemNamingScheme:
    #     return self.input_obj_naming_scheme

    @property
    @override
    def input_pattern_description(self) -> InputPatternDescription:
        return InputPatternDescription(
            naming_scheme=self.input_obj_naming_scheme,
            spec_pattern=self.spec_pattern,
        )


@dataclass(frozen=True)
class ResolverInputForOrderByItem(MetricFlowQueryResolverInput):
    """An input that describes the ordered item.

    The challenge with order-by items is that it may not be obvious how to match an order-by item to a metric or a
    group-by item in the query. When the query inputs were entirely strings, this was easy because the order-by item
    could be resolved with an equality check. However, when the query inputs could be a string or a *QueryParameter
    object, the equality check is not possible. e.g. consider the case:

        group-by item: TimeDimension("creation_time"), order-by item: "creation_time".

    Instead, the approach is to resolve the metrics / group-by items into concrete spec objects, and then use the
    SpecPattern generated from the order-by item input to match to those.

    possible_inputs is necessary because at parse time for string inputs, it's ambiguous whether the order-by
    item is for a metric or a group-by-item since there could be overlap in the input spaces for the metric and
    group-by-item naming schemes.
    """

    input_obj: Union[str, OrderByQueryParameter]
    possible_inputs: Tuple[Union[ResolverInputForMetric, ResolverInputForGroupByItem], ...]
    descending: bool

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)


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


@dataclass(frozen=True)
class ResolverInputForQuery(MetricFlowQueryResolverInput):
    metric_inputs: Tuple[ResolverInputForMetric, ...]
    group_by_item_inputs: Tuple[ResolverInputForGroupByItem, ...]
    filter_input: ResolverInputForWhereFilterIntersection
    order_by_item_inputs: Tuple[ResolverInputForOrderByItem, ...]
    limit_input: ResolverInputForLimit

    @property
    def ui_description(self) -> str:
        return (
            f"Query({repr([metric_input.ui_description for metric_input in self.metric_inputs])}, "
            f"{repr([group_by_item_input.input_obj for group_by_item_input in self.group_by_item_inputs])}"
        )
