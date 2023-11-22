from __future__ import annotations

import datetime
import logging
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols import SavedQuery
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.assert_one_arg import assert_at_most_one_arg_set
from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.filters.merge_where import merge_to_single_where_filter
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.formatting import indent_log_line
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.dunder_scheme import DunderNamingScheme
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.protocols.query_parameter import (
    GroupByParameter,
    MetricQueryParameter,
    OrderByQueryParameter,
    SavedQueryParameter,
)
from metricflow.query.issues.group_by_item_parsing_issue import GroupByItemNameParsingIssue
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_resolution import InputToIssueSetMapping, InputToIssueSetMappingItem
from metricflow.query.query_resolver import MetricFlowQueryResolver
from metricflow.query.resolver_inputs.query_resolver_inputs import (
    MetricFlowQueryResolverInput,
    NonMatchingInput,
    ResolverInputForGroupBy,
    ResolverInputForLimit,
    ResolverInputForMetric,
    ResolverInputForOrderBy,
    ResolverInputForQuery,
    ResolverInputForWhereFilterIntersection,
)
from metricflow.query.resolver_inputs.string_inputs import (
    InvalidStringInput,
    StringInputForOrderBy,
    StringResolverInputForGroupBy,
    StringResolverInputForMetric,
)
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.patterns.metric_time_pattern import MetricTimePattern
from metricflow.specs.python_object import parse_object_builder_naming_scheme
from metricflow.specs.query_param_implementations import MetricParameter
from metricflow.specs.specs import (
    MetricFlowQuerySpec,
    TimeDimensionSpec,
)
from metricflow.time.time_granularity import (
    adjust_to_end_of_period,
    adjust_to_start_of_period,
    is_period_end,
    is_period_start,
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
    ) -> None:
        self._manifest_lookup = model
        self._naming_schemes = (
            ObjectBuilderNamingScheme(),
            DunderNamingScheme(),
        )

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
            for saved_query in self._manifest_lookup.semantic_manifest.saved_queries
            if saved_query.name == saved_query_parameter.name
        ]

        if len(matching_saved_queries) != 1:
            known_saved_query_names = sorted(
                saved_query.name for saved_query in self._manifest_lookup.semantic_manifest.saved_queries
            )
            raise InvalidQueryException(
                f"Did not find saved query `{saved_query_parameter.name}` in known saved queries:\n"
                f"{pformat_big_objects(known_saved_query_names)}"
            )

        return matching_saved_queries[0]

    @staticmethod
    def _metric_time_granularity(time_dimension_specs: Sequence[TimeDimensionSpec]) -> Optional[TimeGranularity]:
        metric_time_specs = MetricTimePattern().match(time_dimension_specs)
        if len(metric_time_specs) == 0:
            return None

        return min(
            tuple(spec.time_granularity for spec in metric_time_specs),
            key=lambda time_granularity: time_granularity.to_int(),
        )

    def _adjust_time_constraint(
        self, time_dimension_specs_in_query: Sequence[TimeDimensionSpec], time_constraint: TimeRangeConstraint
    ) -> TimeRangeConstraint:
        metric_time_granularity = MetricFlowQueryParser._metric_time_granularity(time_dimension_specs_in_query)
        if metric_time_granularity is None:
            return time_constraint

        """Change the time range so that the ends are at the ends of the appropriate time granularity windows.

        e.g. [2020-01-15, 2020-2-15] with MONTH granularity -> [2020-01-01, 2020-02-29]
        """
        constraint_start = time_constraint.start_time
        constraint_end = time_constraint.end_time

        start_ts = pd.Timestamp(time_constraint.start_time)
        if not is_period_start(metric_time_granularity, start_ts):
            constraint_start = adjust_to_start_of_period(metric_time_granularity, start_ts).to_pydatetime()

        end_ts = pd.Timestamp(time_constraint.end_time)
        if not is_period_end(metric_time_granularity, end_ts):
            constraint_end = adjust_to_end_of_period(metric_time_granularity, end_ts).to_pydatetime()

        if constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        if constraint_end > TimeRangeConstraint.ALL_TIME_END():
            constraint_end = TimeRangeConstraint.ALL_TIME_END()

        return TimeRangeConstraint(start_time=constraint_start, end_time=constraint_end)

    @staticmethod
    def _parse_order_by_names(
        order_by_names: Sequence[str],
        resolver_inputs_for_metrics: Sequence[ResolverInputForMetric],
        resolver_inputs_for_group_by: Sequence[ResolverInputForGroupBy],
    ) -> Sequence[ResolverInputForOrderBy]:
        resolver_input_for_order: List[ResolverInputForOrderBy] = []
        for order_by_name in order_by_names:
            descending = False
            corresponding_query_item_name = order_by_name
            if order_by_name[0] == "-":
                descending = True
                corresponding_query_item_name = order_by_name[1:]

            order_by_name_is_invalid = True
            for resolver_input_for_metrics in resolver_inputs_for_metrics:
                if resolver_input_for_metrics.input_obj == corresponding_query_item_name:
                    resolver_input_for_order.append(
                        StringInputForOrderBy(
                            input_item_to_order=resolver_input_for_metrics,
                            descending=descending,
                            input_str=order_by_name,
                        )
                    )
                    order_by_name_is_invalid = False
            for resolver_input_for_group_by in resolver_inputs_for_group_by:
                if resolver_input_for_group_by.input_obj == corresponding_query_item_name:
                    resolver_input_for_order.append(
                        StringInputForOrderBy(
                            input_item_to_order=resolver_input_for_group_by,
                            descending=descending,
                            input_str=order_by_name,
                        )
                    )
                    order_by_name_is_invalid = False

            if order_by_name_is_invalid:
                resolver_input_for_order.append(
                    ResolverInputForOrderBy(
                        input_item_to_order=NonMatchingInput(input_obj=order_by_name),
                        descending=False,
                    )
                )

        return resolver_input_for_order

    @staticmethod
    def _parse_order_by(
        order_by: Sequence[OrderByQueryParameter],
    ) -> Sequence[ResolverInputForOrderBy]:
        return tuple(order_by_query_parameter.query_resolver_input for order_by_query_parameter in order_by)

        # resolver_input_for_order = []
        # for order_by_query_parameter in order_by:
        #     item_to_order_by = order_by_query_parameter.order_by
        #     order_by_query_parameter_is_invalid = True
        #     descending = order_by_query_parameter.descending
        #     for resolver_input_for_metrics in resolver_inputs_for_metrics:
        #         if resolver_input_for_metrics.input_obj == item_to_order_by:
        #             resolver_input_for_order.append(
        #                 StringInputForOrderBy(
        #                     input_item_to_order=resolver_input_for_metrics,
        #                     descending=descending,
        #                     input_str=order_by_name,
        #                 )
        #             )
        #             order_by_query_parameter_is_invalid = False
        #     for resolver_input_for_group_by in resolver_inputs_for_group_by:
        #         if resolver_input_for_group_by.input_obj == item_to_order_by:
        #             resolver_input_for_order.append(
        #                 StringInputForOrderBy(
        #                     input_item_to_order=resolver_input_for_group_by,
        #                     descending=descending,
        #                     input_str=order_by_name,
        #                 )
        #             )
        #             order_by_query_parameter_is_invalid = False
        #
        #     if order_by_query_parameter_is_invalid:
        #         resolver_input_for_order.append(NonMatchingInput(input_obj=order_by_name))
        #
        # return resolver_input_for_order

    @staticmethod
    def _error_message(
        input_to_issue_set: InputToIssueSetMapping,
    ) -> Optional[str]:
        lines: List[str] = ["Got errors while resolving the query."]
        issue_counter = 0

        for item in input_to_issue_set.items:
            resolver_input = item.resolver_input
            issue_set = item.issue_set

            if not issue_set.has_errors:
                continue

            lines.append(f"\nQuery input: {resolver_input.ui_description} has errors:")
            issue_set_lines: List[str] = []
            for error_issue in issue_set.errors:
                issue_counter += 1
                issue_set_lines.extend(
                    [
                        f"Error Issue #{issue_counter}:\n",
                        error_issue.ui_description(resolver_input),
                    ]
                )

                if len(error_issue.query_resolution_path.resolution_path_nodes) > 0:
                    issue_set_lines.extend(
                        [
                            "\nIssue Location:\n",
                            error_issue.query_resolution_path.ui_description,
                        ]
                    )

            lines.extend(indent_log_line(issue_set_line) for issue_set_line in issue_set_lines)

        return "\n".join(lines)

    def _raise_exception_if_there_are_errors(
        self,
        input_to_issue_set: InputToIssueSetMapping,
    ) -> None:
        if not input_to_issue_set.merged_issue_set.has_errors:
            return

        raise InvalidQueryException(self._error_message(input_to_issue_set=input_to_issue_set))

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
        include_time_range_constraint: bool = True,
    ) -> MetricFlowQuerySpec:
        """Parse the query into spec objects, validating them in the process.

        e.g. make sure that the given metric is a valid metric name.
        """
        assert_at_most_one_arg_set(metric_names=metric_names, metrics=metrics)
        assert_at_most_one_arg_set(group_by_names=group_by_names, group_by=group_by)
        assert_at_most_one_arg_set(order_by_names=order_by_names, order_by=order_by)
        assert_at_most_one_arg_set(where_constraint=where_constraint, where_constraint_str=where_constraint_str)

        metric_names = metric_names or ()
        metrics = metrics or ()

        group_by_names = group_by_names or ()
        group_by = group_by or ()

        order_by_names = order_by_names or ()
        order_by = order_by or ()

        if time_constraint_start is None:
            time_constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
            logger.info(f"time_constraint_start was None, so it was set to {time_constraint_start}")
        if time_constraint_end is None:
            time_constraint_end = TimeRangeConstraint.ALL_TIME_END()
            logger.info(f"time_constraint_end was None, so it was set to {time_constraint_end}")

        time_constraint = TimeRangeConstraint(
            start_time=time_constraint_start,
            end_time=time_constraint_end,
        )

        resolver_inputs_for_metrics: List[ResolverInputForMetric] = []

        for metric_name in metric_names:
            resolver_inputs_for_metrics.append(StringResolverInputForMetric.from_str(metric_name))
        for metric_query_parameter in metrics:
            resolver_inputs_for_metrics.append(metric_query_parameter.query_resolver_input)

        input_to_issue_set: List[InputToIssueSetMappingItem] = []

        resolver_inputs_for_group_by: List[ResolverInputForGroupBy] = []
        for group_by_name in group_by_names:
            resolver_input: Optional[MetricFlowQueryResolverInput] = None
            for naming_scheme in self._naming_schemes:
                if naming_scheme.input_str_follows_scheme(group_by_name):
                    spec_pattern = naming_scheme.spec_pattern(group_by_name)
                    resolver_input = StringResolverInputForGroupBy(
                        input_obj=group_by_name,
                        input_obj_naming_scheme=naming_scheme,
                        spec_pattern=spec_pattern,
                    )
                    resolver_inputs_for_group_by.append(resolver_input)
                    break
            if resolver_input is None:
                resolver_input = InvalidStringInput(group_by_name)
                input_to_issue_set.append(
                    InputToIssueSetMappingItem(
                        resolver_input=resolver_input,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            GroupByItemNameParsingIssue.from_parameters(
                                input_str=group_by_name,
                            )
                        ),
                    )
                )

            logger.info(
                "Converted group-by-item input:\n"
                + indent_log_line(f"Input: {repr(group_by_name)}")
                + "\n"
                + indent_log_line(f"Resolver Input: {mf_pformat(resolver_input)}")
            )

        for group_by_parameter in group_by:
            resolver_input_for_group_by_parameter = group_by_parameter.query_resolver_input
            resolver_inputs_for_group_by.append(resolver_input_for_group_by_parameter)
            logger.info(
                "Converted group-by-item input:\n"
                + indent_log_line(f"Input: {repr(group_by_parameter)}")
                + "\n"
                + indent_log_line(f"Resolver Input: {mf_pformat(resolver_input_for_group_by_parameter)}")
            )

        where_filters: List[PydanticWhereFilter] = []

        if where_constraint is not None:
            where_filters.append(PydanticWhereFilter(where_sql_template=where_constraint.where_sql_template))
        if where_constraint_str is not None:
            where_filters.append(PydanticWhereFilter(where_sql_template=where_constraint_str))
        resolver_input_for_filter = ResolverInputForWhereFilterIntersection(
            where_filter_intersection=PydanticWhereFilterIntersection(where_filters=where_filters)
        )

        self._raise_exception_if_there_are_errors(
            input_to_issue_set=InputToIssueSetMapping(items=tuple(input_to_issue_set)),
        )

        query_resolver = MetricFlowQueryResolver(
            manifest_lookup=self._manifest_lookup,
        )

        resolver_inputs_for_order_by: List[ResolverInputForOrderBy] = []
        resolver_inputs_for_order_by.extend(
            MetricFlowQueryParser._parse_order_by_names(
                order_by_names=order_by_names,
                resolver_inputs_for_metrics=resolver_inputs_for_metrics,
                resolver_inputs_for_group_by=resolver_inputs_for_group_by,
            )
        )
        resolver_inputs_for_order_by.extend(MetricFlowQueryParser._parse_order_by(order_by=order_by))

        resolver_input_for_limit = ResolverInputForLimit(limit=limit)

        resolver_input_for_query = ResolverInputForQuery(
            metric_inputs=tuple(resolver_inputs_for_metrics),
            group_by_item_inputs=tuple(resolver_inputs_for_group_by),
            order_by_item_inputs=tuple(resolver_inputs_for_order_by),
            limit_input=resolver_input_for_limit,
            filter_input=resolver_input_for_filter,
        )

        logger.info("Resolver input for query is:\n" + indent_log_line(mf_pformat(resolver_input_for_query)))

        query_resolution = query_resolver.resolve_query(resolver_input_for_query)

        logger.info("Query resolution is:\n" + indent_log_line(mf_pformat(query_resolution)))

        self._raise_exception_if_there_are_errors(
            input_to_issue_set=query_resolution.input_to_issue_set,
        )

        query_spec = query_resolution.checked_query_spec

        if include_time_range_constraint:
            time_constraint = self._adjust_time_constraint(
                time_dimension_specs_in_query=query_spec.time_dimension_specs,
                time_constraint=time_constraint,
            )
            logger.info(f"Time constraint after adjustment is: {time_constraint}")

            return query_spec.with_time_range_constraint(time_constraint)

        return query_spec

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
