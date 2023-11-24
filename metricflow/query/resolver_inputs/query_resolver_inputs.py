from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple, Union

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.metric_scheme import MetricNamingScheme
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.protocols.query_parameter import GroupByParameter, MetricQueryParameter
from metricflow.specs.patterns.metric_pattern import MetricSpecPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.query_param_implementations import OrderByParameter


class MetricFlowQueryResolverInput(ABC):
    @property
    @abstractmethod
    def ui_description(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class InvalidStringInput(MetricFlowQueryResolverInput):
    input_obj: str

    @property
    def ui_description(self) -> str:
        return self.input_obj


class NamedResolverInput(MetricFlowQueryResolverInput, ABC):
    @property
    def naming_scheme(self) -> QueryItemNamingScheme:
        raise NotImplementedError

    @property
    @abstractmethod
    def spec_pattern(self) -> SpecPattern:
        raise NotImplementedError


@dataclass(frozen=True)
class ResolverInputForMetric(NamedResolverInput):
    input_obj: Union[MetricQueryParameter, str]
    naming_scheme: MetricNamingScheme
    spec_pattern: MetricSpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)


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
class ResolverInputForGroupBy(NamedResolverInput):
    input_obj: Union[GroupByParameter, str]
    input_obj_naming_scheme: QueryItemNamingScheme
    spec_pattern: SpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)

    @property
    @override
    def naming_scheme(self) -> QueryItemNamingScheme:
        return self.input_obj_naming_scheme


@dataclass(frozen=True)
class ResolverInputForOrderBy(MetricFlowQueryResolverInput):
    """An input that describes the ordered item.

    The spec patterns in this object are used to match to one of the metric / group-by-item specs of the query. This is
    necessary because with different input object types, an equality mapping between the group-by item and the order-by
    item won't work. e.g. group-by item: "TimeDimension("metric_time__day"), order-by item: "metric_time__day".
    """

    input_obj: Union[str, OrderByParameter]
    possible_inputs: Tuple[NamedResolverInput, ...]
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
    group_by_item_inputs: Tuple[ResolverInputForGroupBy, ...]
    filter_input: ResolverInputForWhereFilterIntersection
    order_by_item_inputs: Tuple[ResolverInputForOrderBy, ...]
    limit_input: ResolverInputForLimit

    @property
    def ui_description(self) -> str:
        return (
            f"Query({repr([metric_input.ui_description for metric_input in self.metric_inputs])}, "
            f"{repr([group_by_item_input.input_obj for group_by_item_input in self.group_by_item_inputs])}"
        )
