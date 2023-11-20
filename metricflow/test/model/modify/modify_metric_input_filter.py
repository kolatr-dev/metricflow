from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.metric import PydanticMetricInput
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule

logger = logging.getLogger(__name__)


class ModifyMetricInputFilterTransform(SemanticManifestTransformRule[PydanticSemanticManifest]):
    def __init__(
        self,
        metric_reference: MetricReference,
        metric_input: PydanticMetricInput,
        where_filter_intersection: PydanticWhereFilterIntersection,
    ) -> None:
        self._metric_reference = metric_reference
        self._metric_input = metric_input
        self._where_filter_intersection = where_filter_intersection

    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        updated_metric_inputs = []

        for metric in semantic_manifest.metrics:
            if MetricReference(element_name=metric.name) != self._metric_reference:
                continue
            for metric_input in metric.input_metrics:
                if metric_input == self._metric_input:
                    metric_input.filter = self._where_filter_intersection
                    updated_metric_inputs.append(metric_input)

        if len(updated_metric_inputs) != 1:
            raise RuntimeError(f"Did not update exactly 1 metric input. Updated: {updated_metric_inputs}")

        return semantic_manifest
