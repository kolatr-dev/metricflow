from __future__ import annotations

import logging
from typing import Dict, Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.metric import PydanticMetricInput
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolve_filters.filter_to_pattern import (
    FilterSpecResolutionLookUp,
    WhereFilterSpecResolver,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.model.modify.copy_manifest import copy_manifest
from metricflow.test.model.modify.modify_input_metric_filter import ModifyInputMetricFilterTransform
from metricflow.test.model.modify.modify_metric_filter import ModifyMetricFilterTransform
from metricflow.test.query.group_by_item.conftest import AmbiguousResolutionQueryId, build_resolution_dag
from metricflow.test.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def assert_call_parameter_set_mapping_snapshot_equal(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    resolution_result: FilterSpecResolutionLookUp,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result",
        obj=mf_pformat(resolution_result, include_none_object_fields=False),
    )


@pytest.mark.parametrize("dag_case_id", [case_id.value for case_id in AmbiguousResolutionQueryId])
def test_specs_in_query_where_filter_resolution(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    naming_scheme: QueryItemNamingScheme,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
    dag_case_id: str,
) -> None:
    case_id = AmbiguousResolutionQueryId(dag_case_id)
    resolution_dag = resolution_dags[case_id]

    spec_pattern_resolver = WhereFilterSpecResolver(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
        resolution_dag=resolution_dag,
    )

    resolution_result = spec_pattern_resolver.resolve_lookup()

    assert_call_parameter_set_mapping_snapshot_equal(
        request=request, mf_test_session_state=mf_test_session_state, resolution_result=resolution_result
    )


def check_resolution_with_filter(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    semantic_manifest: PydanticSemanticManifest,
    transform_rule: SemanticManifestTransformRule[PydanticSemanticManifest],
    queried_metrics: Sequence[MetricReference],
) -> None:
    modified_manifest = copy_manifest(
        semantic_manifest=semantic_manifest,
        transform_rule=transform_rule,
    )

    manifest_lookup = SemanticManifestLookup(modified_manifest)
    resolution_dag = build_resolution_dag(
        manifest_lookup=manifest_lookup,
        queried_metrics=queried_metrics,
    )

    spec_pattern_resolver = WhereFilterSpecResolver(
        manifest_lookup=manifest_lookup,
        resolution_dag=resolution_dag,
    )

    resolution_result = spec_pattern_resolver.resolve_lookup()

    assert_call_parameter_set_mapping_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        resolution_result=resolution_result,
    )


def test_filter_resolution_for_valid_metric_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> None:
    check_resolution_with_filter(
        request=request,
        mf_test_session_state=mf_test_session_state,
        semantic_manifest=ambiguous_resolution_manifest,
        transform_rule=ModifyMetricFilterTransform(
            metric_reference=MetricReference(element_name="derived_metric_with_same_parent_time_grains"),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(
                        where_sql_template=("{{ TimeDimension('" + METRIC_TIME_ELEMENT_NAME + "') }} > '2020-01-01'")
                    ),
                ]
            ),
        ),
        queried_metrics=(MetricReference(element_name="derived_metric_with_same_parent_time_grains"),),
    )


def test_filter_resolution_for_invalid_metric_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> None:
    check_resolution_with_filter(
        request=request,
        mf_test_session_state=mf_test_session_state,
        semantic_manifest=ambiguous_resolution_manifest,
        transform_rule=ModifyMetricFilterTransform(
            metric_reference=MetricReference(element_name="derived_metric_with_different_parent_time_grains"),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(
                        where_sql_template=("{{ TimeDimension('" + METRIC_TIME_ELEMENT_NAME + "') }} > '2020-01-01'")
                    ),
                ]
            ),
        ),
        queried_metrics=(MetricReference(element_name="derived_metric_with_different_parent_time_grains"),),
    )


def test_filter_resolution_for_valid_metric_input_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> None:
    check_resolution_with_filter(
        request=request,
        mf_test_session_state=mf_test_session_state,
        semantic_manifest=ambiguous_resolution_manifest,
        transform_rule=ModifyInputMetricFilterTransform(
            metric_reference=MetricReference(element_name="metric_derived_from_homogeneous_derived_metric"),
            metric_input=PydanticMetricInput(
                name="derived_metric_with_same_parent_time_grains",
            ),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(
                        where_sql_template=("{{ TimeDimension('" + METRIC_TIME_ELEMENT_NAME + "') }} > '2020-01-01'")
                    ),
                ]
            ),
        ),
        queried_metrics=(MetricReference(element_name="metric_derived_from_homogeneous_derived_metric"),),
    )


def test_filter_resolution_for_invalid_metric_input_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> None:
    check_resolution_with_filter(
        request=request,
        mf_test_session_state=mf_test_session_state,
        semantic_manifest=ambiguous_resolution_manifest,
        transform_rule=ModifyInputMetricFilterTransform(
            metric_reference=MetricReference(element_name="metric_derived_from_heterogeneous_derived_metric"),
            metric_input=PydanticMetricInput(
                name="derived_metric_with_different_parent_time_grains",
            ),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(
                        where_sql_template=("{{ TimeDimension('" + METRIC_TIME_ELEMENT_NAME + "') }} > '2020-01-01'")
                    ),
                ]
            ),
        ),
        queried_metrics=(MetricReference(element_name="metric_derived_from_heterogeneous_derived_metric"),),
    )
