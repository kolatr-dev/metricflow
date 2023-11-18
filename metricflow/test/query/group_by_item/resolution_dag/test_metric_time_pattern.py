from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.dag_builder import GroupByItemResolutionDagBuilder
from metricflow.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_resolution_dag_snapshot_equal

METRIC_TIME_SPEC_PATTERN = EntityLinkPattern(
    parameter_set=EntityLinkPatternParameterSet.from_parameters(
        element_name=METRIC_TIME_ELEMENT_NAME,
        entity_links=(),
        time_granularity=None,
        date_part=None,
        fields_to_compare=(
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
        ),
    )
)


def test_base_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
) -> None:
    resolution_dag_builder = GroupByItemResolutionDagBuilder(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
    )

    assert_resolution_dag_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        resolution_dag=resolution_dag_builder.build(
            metric_references=(MetricReference("monthly_metric_0"),),
            where_filter_intersection=None,
        ),
    )


def test_two_metrics_with_different_time_grains(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
) -> None:
    resolution_dag_builder = GroupByItemResolutionDagBuilder(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
    )

    assert_resolution_dag_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        resolution_dag=resolution_dag_builder.build(
            metric_references=(
                MetricReference("monthly_metric_0"),
                MetricReference("yearly_metric_0"),
            ),
            where_filter_intersection=None,
        ),
    )


def test_derived_metric_with_different_parent_metric_time_grains(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
) -> None:
    resolution_dag_builder = GroupByItemResolutionDagBuilder(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
    )

    assert_resolution_dag_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        resolution_dag=resolution_dag_builder.build(
            metric_references=(MetricReference("derived_metric_with_different_parent_time_grains"),),
            where_filter_intersection=None,
        ),
    )
