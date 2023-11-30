from __future__ import annotations

import logging
from typing import Dict

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
    WhereFilterSpecResolver,
)
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query.group_by_item.conftest import AmbiguousResolutionQueryId
from metricflow.test.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def assert_filter_spec_lookup_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    filter_spec_lookup: FilterSpecResolutionLookUp,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result",
        obj=mf_pformat(filter_spec_lookup),
    )


@pytest.mark.parametrize("dag_case_id", [case_id.value for case_id in AmbiguousResolutionQueryId])
def test_spec_lookup_for_query_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    naming_scheme: QueryItemNamingScheme,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
    dag_case_id: str,
) -> None:
    """Checks to see how a filter to a query resolves in various cases.

    All cases have an ambiguous metric_time like

        "{{ TimeDimension('" + METRIC_TIME_ELEMENT_NAME + "') }} > '2020-01-01'"

    but whether that can be resolved depends on the query / metric case.
    """
    case_id = AmbiguousResolutionQueryId(dag_case_id)
    resolution_dag = resolution_dags[case_id]

    spec_pattern_resolver = WhereFilterSpecResolver(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
        resolution_dag=resolution_dag,
    )

    resolution_result = spec_pattern_resolver.resolve_lookup()

    assert_filter_spec_lookup_equal(
        request=request, mf_test_session_state=mf_test_session_state, filter_spec_lookup=resolution_result
    )
