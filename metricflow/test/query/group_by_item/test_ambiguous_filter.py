"""
Test cases:

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
from dataclasses import dataclass
from enum import Enum, auto

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.type_enums import TimeGranularity


class FilterLocation(Enum):
    QUERY: auto()
    METRIC: auto()
    INPUT_METRIC: auto()


class ResolutionType(Enum):
    QUERY_FOR_METRICS: auto()
    INPUTS_FOR_SIMPLE_METRIC: auto()
    INPUTS_FOR_DERIVED_METRIC: auto()


class SourceCongruence(Enum):
    SAME_GRAIN = auto()
    DIFFERENT_GRAIN = auto()


class InputAmbiguity(Enum):
    SPECIFIC = auto()
    AMBIGUOUS = auto()


class InputValidity(Enum):
    VALID = auto()
    INVALID = auto()


@dataclass(frozen=True)
class SourceAndInputCombination:
    source_congruence: SourceCongruence
    input_ambiguity: InputAmbiguity
    input_validity: InputValidity
    source_0_grain: TimeGranularity
    source_1_grain: TimeGranularity
    filter_sql: str


class QueryAndMetricCombination:
    


@dataclass(frozen=True)
class SourceGrainConfiguration:
    source_0: TimeGranularity
    source_1: TimeGranularity


@dataclass(frozen=True)
class SourceGrainAndInputConfiguration:
    source_0_grain: TimeGranularity
    source_1_grain: TimeGranularity
    filter_sql: str


def build_manifest(test_case: SourceAndInputCombination) -> SemanticManifest:
    ambiguous_filter_sql = "{{ TimeDimension(" + repr(METRIC_TIME_ELEMENT_NAME) + ") }}"
    specific_filter_sql = (
            "{{" + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.MONTH.value)})" + "}}"
    )
    SourceAndInputCombination(
        source_congruence=SourceCongruence.SAME_GRAIN,
        input_ambiguity=InputAmbiguity.AMBIGUOUS,
        input_validity=InputValidity.VALID,
        source_0_grain=TimeGranularity.MONTH,
        source_1_grain=TimeGranularity.MONTH,
        filter_sql=ambiguous_filter_sql,
    )

    SourceAndInputCombination(
        source_congruence=SourceCongruence.SAME_GRAIN,
        input_ambiguity=InputAmbiguity.SPECIFIC,
        input_validity=InputValidity.VALID,
        source_0_grain=TimeGranularity.MONTH,
        source_1_grain=TimeGranularity.MONTH,
        filter_sql=specific_filter_sql,
    )

    SourceAndInputCombination(
        source_congruence=SourceCongruence.SAME_GRAIN,
        input_ambiguity=InputAmbiguity.SPECIFIC,
        input_validity=InputValidity.INVALID,
        source_0_grain=TimeGranularity.MONTH,
        source_1_grain=TimeGranularity.YEAR,
        filter_sql=specific_filter_sql,
    )

    SourceAndInputCombination(
        source_congruence=SourceCongruence.DIFFERENT_GRAIN,
        input_ambiguity=InputAmbiguity.AMBIGUOUS,
        input_validity=InputValidity.INVALID,
        source_0_grain=TimeGranularity.MONTH,
        source_1_grain=TimeGranularity.YEAR,
        filter_sql=ambiguous_filter_sql,
    )

    SourceAndInputCombination(
        source_congruence=SourceCongruence.DIFFERENT_GRAIN,
        input_ambiguity=InputAmbiguity.SPECIFIC,
        input_validity=InputValidity.VALID,
        source_0_grain=TimeGranularity.MONTH,
        source_1_grain=TimeGranularity.DAY,
        filter_sql=specific_filter_sql,
    )

    SourceAndInputCombination(
        source_congruence=SourceCongruence.DIFFERENT_GRAIN,
        input_ambiguity=InputAmbiguity.SPECIFIC,
        input_validity=InputValidity.INVALID,
        source_0_grain=TimeGranularity.YEAR,
        source_1_grain=TimeGranularity.YEAR,
        filter_sql=specific_filter_sql,
    )

    if test_case.input_ambiguity is InputAmbiguity.AMBIGUOUS:
        filter_sql = "{{ TimeDimension(" + repr(METRIC_TIME_ELEMENT_NAME) + ") }}"
    elif test_case.input_ambiguity is InputAmbiguity.SPECIFIC:
        filter_sql = (
            "{{" + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.MONTH.value)})" + "}}"
        )
    else:
        assert_values_exhausted(test_case.input_ambiguity)

    if test_case.input_validity is InputValidity.VALID:
        if test_case.source_congruence is SourceCongruence.SAME_GRAIN:
            source_grain_configuration = SourceGrainConfiguration(
                source_0=TimeGranularity.MONTH,
                source_1=TimeGranularity.MONTH,
            )
        elif test_case.source_congruence is SourceCongruence.DIFFERENT_GRAIN:
            raise NotImplementedError
        else:
            assert_values_exhausted(test_case.source_congruence)
    elif test_case.input_validity is InputValidity.INVALID:
        if test_case.source_congruence is SourceCongruence.SAME_GRAIN:
            raise NotImplementedError
        elif test_case.source_congruence is SourceCongruence.DIFFERENT_GRAIN:
            source_grain_configuration = SourceGrainConfiguration(
                source_0=TimeGranularity.DAY,
                source_1=TimeGranularity.MONTH,
            )
        else:
            assert_values_exhausted(test_case.source_congruence)
    raise NotImplementedError