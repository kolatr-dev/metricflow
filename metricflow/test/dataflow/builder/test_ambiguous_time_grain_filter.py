from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal

logger = logging.getLogger(__name__)


@pytest.mark.sql_engine_snapshot
def test_ambiguous_time_dimension_in_query_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    query_parser = MetricFlowQueryParser(
        column_association_resolver=DunderColumnAssociationResolver(ambiguous_resolution_manifest_lookup),
        model=ambiguous_resolution_manifest_lookup,
    )
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("monthly_metric_0", "monthly_metric_1"),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraint_str="{{ TimeDimension('metric_time') }} = '2020-01-01'",
        include_time_range_constraint=False,
    )

    dataflow_plan_builder = DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.ambiguous_resolution_source_nodes,
        read_nodes=tuple(consistent_id_object_repository.ambiguous_resolution_read_nodes.values()),
        semantic_manifest_lookup=ambiguous_resolution_manifest_lookup,
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )
