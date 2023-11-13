from __future__ import annotations

import datetime
import logging
from typing import List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols import SavedQuery
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import ReadSqlSourceNode
from metricflow.filters.merge_where import merge_to_single_where_filter
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.protocols.query_parameter import (
    GroupByParameter,
    MetricQueryParameter,
    OrderByQueryParameter,
    SavedQueryParameter,
)
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.python_object import parse_object_builder_naming_scheme
from metricflow.specs.query_param_implementations import MetricParameter
from metricflow.specs.specs import (
    MetricFlowQuerySpec,
)

logger = logging.getLogger(__name__)


class MetricFlowQueryParser:
    """Parse input strings from the user into a metric query specification.

    Definitions:
    element name - the name of an element (measure, dimension, entity) in a semantic model, or a metric name.
    qualified name - an element name with prefixes and suffixes added to it that further describe transformations or
    conditions for the element to retrieve. e.g. "ds__month" is the "ds" time dimension at the "month" granularity. Or
    "user_id__country" is the "country" dimension that is retrieved by joining "user_id" to the measure semantic model.
    TODO: Add fuzzy match results
    TODO: Add time dimension specs with a date part to ValidLinkableSpecResolver.
    """

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        model: SemanticManifestLookup,
        read_nodes: Sequence[ReadSqlSourceNode],
        node_output_resolver: DataflowPlanNodeOutputDataSetResolver,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._model = model
        self._metric_lookup = model.metric_lookup
        self._semantic_model_lookup = model.semantic_model_lookup
        self._node_output_resolver = node_output_resolver
        self._read_nodes = read_nodes

    def parse_and_validate_saved_query(
        self,
        saved_query_parameter: SavedQueryParameter,
        where_filter: Optional[WhereFilter],
        limit: Optional[int],
        order_by_names: Optional[Sequence[str]],
        order_by_parameters: Optional[Sequence[OrderByQueryParameter]],
    ) -> MetricFlowQuerySpec:
        """Parse and validate a query using parameters from a pre-defined / saved query.

        Additional parameters act in conjunction with the parameters in the saved query.
        """
        saved_query = self._get_saved_query(saved_query_parameter)

        # Merge interface could streamline this.
        where_filters: List[WhereFilter] = []
        if saved_query.query_params.where is not None:
            where_filters.extend(saved_query.query_params.where.where_filters)
        if where_filter is not None:
            where_filters.append(where_filter)

        return self.parse_and_validate_query(
            metrics=tuple(MetricParameter(name=metric_name) for metric_name in saved_query.query_params.metrics),
            group_by=tuple(
                parse_object_builder_naming_scheme(group_by_item_name)
                for group_by_item_name in saved_query.query_params.group_by
            ),
            where_constraint=merge_to_single_where_filter(PydanticWhereFilterIntersection(where_filters=where_filters)),
            limit=limit,
            order_by_names=order_by_names,
            order_by=order_by_parameters,
        )

    def _get_saved_query(self, saved_query_parameter: SavedQueryParameter) -> SavedQuery:
        matching_saved_queries = [
            saved_query
            for saved_query in self._model.semantic_manifest.saved_queries
            if saved_query.name == saved_query_parameter.name
        ]

        if len(matching_saved_queries) != 1:
            known_saved_query_names = sorted(
                saved_query.name for saved_query in self._model.semantic_manifest.saved_queries
            )
            raise InvalidQueryException(
                f"Did not find saved query `{saved_query_parameter.name}` in known saved queries:\n"
                f"{pformat_big_objects(known_saved_query_names)}"
            )

        return matching_saved_queries[0]

    def parse_and_validate_query(
        self,
        metric_names: Optional[Sequence[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        group_by_names: Optional[Sequence[str]] = None,
        group_by: Optional[Tuple[GroupByParameter, ...]] = None,
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[WhereFilter] = None,
        where_constraint_str: Optional[str] = None,
        order_by_names: Optional[Sequence[str]] = None,
        order_by: Optional[Sequence[OrderByQueryParameter]] = None,
    ) -> MetricFlowQuerySpec:
        """Parse the query into spec objects, validating them in the process.

        e.g. make sure that the given metric is a valid metric name.
        """
        raise NotImplementedError

    # def _validate_no_metric_time_dimension_query(
    #     self, metric_references: Sequence[MetricReference], time_dimension_specs: Sequence[TimeDimensionSpec]
    # ) -> None:
    #     """Validate if all requested metrics are queryable without grouping by metric_time."""
    #     if any([spec.reference == DataSet.metric_time_dimension_reference() for spec in time_dimension_specs]):
    #         return
    #
    #     for metric_reference in metric_references:
    #         metric = self._metric_lookup.get_metric(metric_reference)
    #         if metric.type == MetricType.CUMULATIVE:
    #             # Cumulative metrics configured with a window/grain_to_date cannot be queried without a dimension.
    #             if metric.type_params.window or metric.type_params.grain_to_date:
    #                 raise UnableToSatisfyQueryError(
    #                     f"Metric {metric.name} is a cumulative metric specified with a window/grain_to_date "
    #                     f"which must be queried with the dimension 'metric_time'.",
    #                 )
    #         elif metric.type == MetricType.DERIVED:
    #             for input_metric in metric.type_params.metrics or []:
    #                 if input_metric.offset_window or input_metric.offset_to_grain:
    #                     raise UnableToSatisfyQueryError(
    #                         f"Metric '{metric.name}' is a derived metric that contains input metrics with "
    #                         "an `offset_window` or `offset_to_grain` which must be queried with the "
    #                         "dimension 'metric_time'."
    #                     )

    # def _validate_date_part(
    #     self, metric_references: Sequence[MetricReference], time_dimension_specs: Sequence[TimeDimensionSpec]
    # ) -> None:
    #     """Validate that date parts can be used for metrics.
    #
    #     TODO: figure out expected behavior for date part with these types of metrics.
    #     """
    #     date_part_requested = False
    #     for time_dimension_spec in time_dimension_specs:
    #         if time_dimension_spec.date_part:
    #             date_part_requested = True
    #             if time_dimension_spec.date_part.to_int() < time_dimension_spec.time_granularity.to_int():
    #                 raise RequestTimeGranularityException(
    #                     f"Date part {time_dimension_spec.date_part.name} is not compatible with time granularity "
    #                     f"{time_dimension_spec.time_granularity.name}. Compatible granularities include: "
    #                     f"{[granularity.name for granularity in time_dimension_spec.date_part.compatible_granularities]}"
    #                 )
    #     if date_part_requested:
    #         for metric_reference in metric_references:
    #             metric = self._metric_lookup.get_metric(metric_reference)
    #             if metric.type == MetricType.CUMULATIVE:
    #                 raise UnableToSatisfyQueryError("Cannot extract date part for cumulative metrics.")
    #             elif metric.type == MetricType.DERIVED:
    #                 for input_metric in metric.type_params.metrics or []:
    #                     if input_metric.offset_to_grain:
    #                         raise UnableToSatisfyQueryError(
    #                             "Cannot extract date part for metrics with offset_to_grain."
    #                         )

    # def _verify_resolved_granularity_for_date_part(
    #     self,
    #     requested_dimension_structured_name: StructuredLinkableSpecName,
    #     partial_time_dimension_spec: PartialTimeDimensionSpec,
    #     metric_references: Sequence[MetricReference],
    # ) -> None:
    #     """Enforce that any granularity value associated with a date part query is the minimum.
    #
    #     By default, we will always ensure that a date_part query request uses the minimum granularity.
    #     However, there are some interfaces where the user must pass in a granularity, so we need a check to
    #     ensure that the correct value was passed in.
    #     """
    #     resolved_granularity = self._time_granularity_solver.find_minimum_granularity_for_partial_time_dimension_spec(
    #         partial_time_dimension_spec=partial_time_dimension_spec, metric_references=metric_references
    #     )
    #     if resolved_granularity != requested_dimension_structured_name.time_granularity:
    #         raise RequestTimeGranularityException(
    #             f"When applying a date part to dimension '{requested_dimension_structured_name.qualified_name}' with "
    #             f"metrics {[metric.element_name for metric in metric_references]}, only {resolved_granularity.name} "
    #             "granularity can be used."
    #         )

    # def _get_invalid_linkable_specs(
    #     self,
    #     metric_references: Tuple[MetricReference, ...],
    #     dimension_specs: Tuple[DimensionSpec, ...],
    #     time_dimension_specs: Tuple[TimeDimensionSpec, ...],
    #     entity_specs: Tuple[EntitySpec, ...],
    # ) -> List[LinkableInstanceSpec]:
    #     """Checks that each requested linkable instance can be retrieved for the given metric."""
    #     invalid_linkable_specs: List[LinkableInstanceSpec] = []
    #     # TODO: distinguish between dimensions that invalid via typo vs ambiguous join path
    #     valid_linkable_specs = self._metric_lookup.element_specs_for_metrics(metric_references=list(metric_references))
    #
    #     for dimension_spec in dimension_specs:
    #         if dimension_spec not in valid_linkable_specs:
    #             invalid_linkable_specs.append(dimension_spec)
    #
    #     for entity_spec in entity_specs:
    #         if entity_spec not in valid_linkable_specs:
    #             invalid_linkable_specs.append(entity_spec)
    #
    #     for time_dimension_spec in time_dimension_specs:
    #         time_dimension_spec_without_date_part = time_dimension_spec
    #         if time_dimension_spec.date_part:
    #             # TODO: remove this workaround & add date_part specs to validation paths.
    #             time_dimension_spec_without_date_part = TimeDimensionSpec(
    #                 element_name=time_dimension_spec.element_name,
    #                 entity_links=time_dimension_spec.entity_links,
    #                 time_granularity=time_dimension_spec.time_granularity,
    #                 aggregation_state=time_dimension_spec.aggregation_state,
    #             )
    #         if (
    #             time_dimension_spec_without_date_part not in valid_linkable_specs
    #             # Because the metric time dimension is a virtual dimension that's not in the model, it won't be included
    #             # in valid_linkable_specs.
    #             and time_dimension_spec.reference != DataSet.metric_time_dimension_reference()
    #         ):
    #             invalid_linkable_specs.append(time_dimension_spec)
    #
    #     return invalid_linkable_specs
