from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantics.metric_lookup import MetricLookup


@dataclass(frozen=True)
class MetricInputLocation:
    parent_metric_reference: MetricReference
    metric_input_index: int

    def get_metric_input(self, metric_lookup: MetricLookup) -> MetricInput:
        metric = metric_lookup.get_metric(self.parent_metric_reference)
        if metric.input_metrics is None or len(metric.input_metrics) <= self.metric_input_index:
            raise ValueError(f"The metric input index is invalid for metric: {metric}")

        return metric.input_metrics[self.metric_input_index]
