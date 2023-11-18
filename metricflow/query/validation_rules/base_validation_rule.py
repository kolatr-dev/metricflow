from __future__ import annotations

from abc import ABC, abstractmethod

from dbt_semantic_interfaces.protocols import Metric
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery


class PostResolutionQueryValidationRule(ABC):
    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._manifest_lookup = manifest_lookup

    def _get_metric(self, metric_reference: MetricReference) -> Metric:
        return self._manifest_lookup.metric_lookup.get_metric(metric_reference)

    @abstractmethod
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        raise NotImplementedError
