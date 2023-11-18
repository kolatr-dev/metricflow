"""Test cases:

where filters in:
    query
    metric filter
    derived metric input metric

cases:
    metric
    derived metric with input metrics
    query with metrics

sources:
    different grains
    same grains

spec cases:
    valid ambiguous
    invalid ambiguous
    valid specific
    invalid specific

modules:
    query resolver
    dataflow builer
"""
from __future__ import annotations

import copy
import itertools
import logging
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Sequence, Tuple

import pytest
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.test.model.modify.modify_input_measure_filter import ModifyInputMeasureFilterTransform
from metricflow.test.model.modify.modify_input_metric_filter import ModifyInputMetricFilterTransform2
from metricflow.test.model.modify.modify_metric_filter import ModifyMetricFilterTransform

logger = logging.getLogger(__name__)


class FilterParentTimeGrainCongruence(Enum):
    SAME_GRAIN = auto()
    DIFFERENT_GRAIN = auto()


class FilterAmbiguity(Enum):
    SPECIFIC = auto()
    AMBIGUOUS = auto()


class FilterValidity(Enum):
    VALID = auto()
    INVALID = auto()


@dataclass(frozen=True)
class FilterAmbiguityCase:
    parent_time_grain_congruence: FilterParentTimeGrainCongruence
    filter_ambiguity: FilterAmbiguity
    filter_validity: FilterValidity


class FilterLocation(Enum):
    FILTER_IN_QUERY_FOR_SIMPLE_METRICS = auto()
    FILTER_IN_SIMPLE_METRIC = auto()
    FILTER_IN_DERIVED_METRIC = auto()
    FILTER_IN_INPUT_MEASURE = auto()
    FILTER_IN_INPUT_METRIC = auto()


def add_filter_to_input_measures(
    semantic_manifest: PydanticSemanticManifest, metric_reference: MetricReference, filter_sql: str
) -> None:
    ModifyInputMeasureFilterTransform(
        metric_reference=metric_reference,
        where_filter_intersection=PydanticWhereFilterIntersection(
            where_filters=[PydanticWhereFilter(where_sql_template=filter_sql)]
        ),
    ).transform_model(
        semantic_manifest=semantic_manifest,
    )


def add_filter_to_input_metrics(
    semantic_manifest: PydanticSemanticManifest, metric_reference: MetricReference, filter_sql: str
) -> None:
    ModifyInputMetricFilterTransform2(
        metric_reference=metric_reference,
        where_filter_intersection=PydanticWhereFilterIntersection(
            where_filters=[PydanticWhereFilter(where_sql_template=filter_sql)]
        ),
    ).transform_model(
        semantic_manifest=semantic_manifest,
    )


def add_filter_to_metric(
    semantic_manifest: PydanticSemanticManifest, metric_reference: MetricReference, filter_sql: str
) -> None:
    ModifyMetricFilterTransform(
        metric_reference=metric_reference,
        where_filter_intersection=PydanticWhereFilterIntersection(
            where_filters=[PydanticWhereFilter(where_sql_template=filter_sql)]
        ),
    ).transform_model(
        semantic_manifest=semantic_manifest,
    )


@dataclass(frozen=True)
class AmbiguousFilterQueryCase:
    filter_location: FilterLocation
    filter_ambiguity_case: FilterAmbiguityCase
    semantic_manifest: PydanticSemanticManifest
    metrics_to_query: Tuple[MetricReference, ...]
    query_filter: WhereFilterIntersection


def build_ambiguous_filter_case(
    ambiguous_resolution_manifest: PydanticSemanticManifest,
    filter_location: FilterLocation,
    filter_ambiguity_case: FilterAmbiguityCase,
) -> AmbiguousFilterQueryCase:
    # Create a copy since this will modify the manifest to include a filter.
    ambiguous_resolution_manifest = copy.deepcopy(ambiguous_resolution_manifest)

    ambiguous_filter_sql = "{{ TimeDimension(" + repr(METRIC_TIME_ELEMENT_NAME) + ") }} > '2020-01-01'"
    year_filter_sql = (
        "{{"
        + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.YEAR.value)})"
        + "}} > '2020-01-01'"
    )
    day_filter_sql = (
        "{{"
        + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.DAY.value)})"
        + "}} > '2020-01-01'"
    )

    filter_sql_candidates = set()
    if filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.AMBIGUOUS:
        filter_sql_candidates.add(ambiguous_filter_sql)
    elif filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.SPECIFIC:
        filter_sql_candidates.add(day_filter_sql)
        filter_sql_candidates.add(year_filter_sql)
    else:
        assert_values_exhausted(filter_ambiguity_case.filter_ambiguity)

    if filter_ambiguity_case.filter_validity is FilterValidity.VALID:
        filter_sql_candidates.difference_update({day_filter_sql})
    elif filter_ambiguity_case.filter_validity is FilterValidity.INVALID:
        filter_sql_candidates.difference_update({year_filter_sql})
    else:
        assert_values_exhausted(filter_ambiguity_case.filter_validity)

    assert (
        len(filter_sql_candidates) == 1
    ), f"Could not resolve to a single filter for {filter_ambiguity_case}. Got: {filter_sql_candidates}"
    filter_sql = list(filter_sql_candidates)[0]

    query: Tuple[MetricReference, ...] = ()
    query_filter_sql: Optional[str] = None
    if filter_location is FilterLocation.FILTER_IN_QUERY_FOR_SIMPLE_METRICS:
        query_filter_sql = filter_sql
        if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_GRAIN:
            query = (MetricReference("monthly_metric_0"), MetricReference("monthly_metric_1"))
        elif filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN:
            query = (MetricReference("monthly_metric_0"), MetricReference("yearly_metric_0"))
        else:
            assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
    elif filter_location is FilterLocation.FILTER_IN_INPUT_MEASURE:
        metric_reference = MetricReference("monthly_metric_0")
        query = (metric_reference,)
        if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_GRAIN:
            add_filter_to_input_measures(ambiguous_resolution_manifest, metric_reference, filter_sql)
        elif filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN:
            raise RuntimeError(
                "An input measure only applies to simple metrics, and a simple metric only has a single measure as a "
                "parent, so it can't be of different grains."
            )
        else:
            assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
    elif filter_location is FilterLocation.FILTER_IN_INPUT_METRIC:
        if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_GRAIN:
            metric_reference = MetricReference("derived_metric_with_same_parent_time_grains")
            query = (metric_reference,)
            add_filter_to_input_metrics(ambiguous_resolution_manifest, metric_reference, filter_sql)
        elif filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN:
            raise NotImplementedError
        else:
            assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
    elif filter_location is FilterLocation.FILTER_IN_SIMPLE_METRIC:
        if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_GRAIN:
            metric_reference = MetricReference("monthly_metric_0")
            query = (metric_reference,)
            add_filter_to_metric(ambiguous_resolution_manifest, metric_reference, filter_sql)
        elif filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN:
            raise RuntimeError("A simple metric has a single measure as a parent, so it can't be of different grains.")
        else:
            assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)

    elif filter_location is FilterLocation.FILTER_IN_DERIVED_METRIC:
        if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_GRAIN:
            metric_reference = MetricReference("derived_metric_with_same_parent_time_grains")
            query = (metric_reference,)
            add_filter_to_metric(ambiguous_resolution_manifest, metric_reference, filter_sql)
        elif filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN:
            metric_reference = MetricReference("derived_metric_with_different_parent_time_grains")
            query = (metric_reference,)
            add_filter_to_metric(ambiguous_resolution_manifest, metric_reference, filter_sql)
        else:
            assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
    else:
        assert_values_exhausted(filter_location)

    return AmbiguousFilterQueryCase(
        filter_location=filter_location,
        filter_ambiguity_case=filter_ambiguity_case,
        semantic_manifest=ambiguous_resolution_manifest,
        metrics_to_query=query,
        query_filter=PydanticWhereFilterIntersection(
            where_filters=[PydanticWhereFilter(where_sql_template=query_filter_sql)]
            if query_filter_sql is not None
            else []
        ),
    )


@pytest.fixture
def ambiguous_filter_query_cases(
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> Sequence[AmbiguousFilterQueryCase]:
    # parent_time_grain_congruence_values = (FilterParentTimeGrainCongruence.DIFFERENT_GRAIN,)
    parent_time_grain_congruence_values = tuple(FilterParentTimeGrainCongruence)
    filter_ambiguity_values = tuple(FilterAmbiguity)
    filter_validity_values = tuple(FilterValidity)

    filter_ambiguity_cases = []
    for ambiguity_case_args in itertools.product(
        parent_time_grain_congruence_values,
        filter_ambiguity_values,
        filter_validity_values,
    ):
        filter_ambiguity_cases.append(
            FilterAmbiguityCase(
                parent_time_grain_congruence=ambiguity_case_args[0],
                filter_ambiguity=ambiguity_case_args[1],
                filter_validity=ambiguity_case_args[2],
            )
        )

    filter_locations = tuple(FilterLocation)
    ambiguous_filter_query_cases: List[AmbiguousFilterQueryCase] = []

    for build_ambiguous_filter_case_args in itertools.product(
        filter_locations,
        filter_ambiguity_cases,
    ):
        filter_location: FilterLocation = build_ambiguous_filter_case_args[0]
        filter_ambiguity_case: FilterAmbiguityCase = build_ambiguous_filter_case_args[1]

        # Simple metrics only have a single measure as a parent, so it can't have parents of different grains.
        if (
            filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN
            and filter_location in (FilterLocation.FILTER_IN_INPUT_MEASURE, FilterLocation.FILTER_IN_SIMPLE_METRIC)
        ):
            continue

        # If the parents are different grains, then an ambiguous filter can never be valid.
        if (
            filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN
            and filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.AMBIGUOUS
            and filter_ambiguity_case.filter_validity is FilterValidity.VALID
        ):
            continue
        # If the parents are same grains, then an ambiguous filter is always valid.
        if (
            filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_GRAIN
            and filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.AMBIGUOUS
            and filter_ambiguity_case.filter_validity is FilterValidity.INVALID
        ):
            continue

        # This case has not yet been implemented.
        if (
            filter_location.FILTER_IN_INPUT_METRIC
            and filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.DIFFERENT_GRAIN
        ):
            continue
        ambiguous_filter_query_cases.append(
            build_ambiguous_filter_case(
                ambiguous_resolution_manifest=ambiguous_resolution_manifest,
                filter_location=filter_location,
                filter_ambiguity_case=filter_ambiguity_case,
            )
        )

    return ambiguous_filter_query_cases


# @pytest.fixture
# def ambiguous_filter_cases(ambiguous_resolution_manifest: SemanticManifest) -> Sequence[AmbiguousFilterQueryCase]:


# class ResolutionType(Enum):
#     QUERY_FOR_METRICS: auto()
#     INPUTS_FOR_SIMPLE_METRIC: auto()
#     INPUTS_FOR_DERIVED_METRIC: auto()
#
#
# class InputValidity(Enum):
#     VALID = auto()
#     INVALID = auto()
#
#
# class ResolutionMetricType(Enum):
#     SIMPLE = auto()
#     DERIVED = auto()
#
#
# @dataclass(frozen=True)
# class SourceAndInputCombination:
#     source_congruence: SourceCongruence
#     input_ambiguity: InputAmbiguity
#     input_validity: InputValidity
#     source_0_grain: TimeGranularity
#     source_1_grain: TimeGranularity
#     filter_sql: str
#
#
# @dataclass(frozen=True)
# class QueryAndMetricCombination:
#     resolution_type: ResolutionType
#     filter_locations: Iterable[FilterLocation]
#
#
# @dataclass(frozen=True)
# class SourceGrainConfiguration:
#     source_0: TimeGranularity
#     source_1: TimeGranularity
#
#
# @dataclass(frozen=True)
# class SourceGrainAndInputConfiguration:
#     source_0_grain: TimeGranularity
#     source_1_grain: TimeGranularity
#     filter_sql: str
#
#
# def build_manifest(test_case: SourceAndInputCombination) -> SemanticManifest:
#     ambiguous_filter_sql = "{{ TimeDimension(" + repr(METRIC_TIME_ELEMENT_NAME) + ") }}"
#     specific_filter_sql = (
#         "{{" + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.MONTH.value)})" + "}}"
#     )
#     SourceAndInputCombination(
#         source_congruence=SourceCongruence.SAME_GRAIN,
#         input_ambiguity=InputAmbiguity.AMBIGUOUS,
#         input_validity=InputValidity.VALID,
#         source_0_grain=TimeGranularity.MONTH,
#         source_1_grain=TimeGranularity.MONTH,
#         filter_sql=ambiguous_filter_sql,
#     )
#
#     SourceAndInputCombination(
#         source_congruence=SourceCongruence.SAME_GRAIN,
#         input_ambiguity=InputAmbiguity.SPECIFIC,
#         input_validity=InputValidity.VALID,
#         source_0_grain=TimeGranularity.MONTH,
#         source_1_grain=TimeGranularity.MONTH,
#         filter_sql=specific_filter_sql,
#     )
#
#     SourceAndInputCombination(
#         source_congruence=SourceCongruence.SAME_GRAIN,
#         input_ambiguity=InputAmbiguity.SPECIFIC,
#         input_validity=InputValidity.INVALID,
#         source_0_grain=TimeGranularity.MONTH,
#         source_1_grain=TimeGranularity.YEAR,
#         filter_sql=specific_filter_sql,
#     )
#
#     SourceAndInputCombination(
#         source_congruence=SourceCongruence.DIFFERENT_GRAIN,
#         input_ambiguity=InputAmbiguity.AMBIGUOUS,
#         input_validity=InputValidity.INVALID,
#         source_0_grain=TimeGranularity.MONTH,
#         source_1_grain=TimeGranularity.YEAR,
#         filter_sql=ambiguous_filter_sql,
#     )
#
#     SourceAndInputCombination(
#         source_congruence=SourceCongruence.DIFFERENT_GRAIN,
#         input_ambiguity=InputAmbiguity.SPECIFIC,
#         input_validity=InputValidity.VALID,
#         source_0_grain=TimeGranularity.MONTH,
#         source_1_grain=TimeGranularity.DAY,
#         filter_sql=specific_filter_sql,
#     )
#
#     SourceAndInputCombination(
#         source_congruence=SourceCongruence.DIFFERENT_GRAIN,
#         input_ambiguity=InputAmbiguity.SPECIFIC,
#         input_validity=InputValidity.INVALID,
#         source_0_grain=TimeGranularity.YEAR,
#         source_1_grain=TimeGranularity.YEAR,
#         filter_sql=specific_filter_sql,
#     )
#
#     QueryAndMetricCombination(
#         resolution_type=ResolutionType.QUERY_FOR_METRICS, filter_locations=(FilterLocation.QUERY, FilterLocation.METRIC)
#     )
#
#     QueryAndMetricCombination(
#         resolution_type=ResolutionType.INPUTS_FOR_SIMPLE_METRIC,
#         filter_locations=(FilterLocation.INPUT_MEASURE, FilterLocation.METRIC),
#     )
#
#     QueryAndMetricCombination(
#         resolution_type=ResolutionType.INPUTS_FOR_DERIVED_METRIC,
#         filter_locations=(FilterLocation.INPUT_METRIC, FilterLocation.METRIC),
#     )
#
#     if test_case.input_ambiguity is InputAmbiguity.AMBIGUOUS:
#         "{{ TimeDimension(" + repr(METRIC_TIME_ELEMENT_NAME) + ") }}"
#     elif test_case.input_ambiguity is InputAmbiguity.SPECIFIC:
#         ("{{" + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.MONTH.value)})" + "}}")
#     else:
#         assert_values_exhausted(test_case.input_ambiguity)
#
#     if test_case.input_validity is InputValidity.VALID:
#         if test_case.source_congruence is SourceCongruence.SAME_GRAIN:
#             SourceGrainConfiguration(
#                 source_0=TimeGranularity.MONTH,
#                 source_1=TimeGranularity.MONTH,
#             )
#         elif test_case.source_congruence is SourceCongruence.DIFFERENT_GRAIN:
#             raise NotImplementedError
#         else:
#             assert_values_exhausted(test_case.source_congruence)
#     elif test_case.input_validity is InputValidity.INVALID:
#         if test_case.source_congruence is SourceCongruence.SAME_GRAIN:
#             raise NotImplementedError
#         elif test_case.source_congruence is SourceCongruence.DIFFERENT_GRAIN:
#             SourceGrainConfiguration(
#                 source_0=TimeGranularity.DAY,
#                 source_1=TimeGranularity.MONTH,
#             )
#         else:
#             assert_values_exhausted(test_case.source_congruence)
#     raise NotImplementedError
