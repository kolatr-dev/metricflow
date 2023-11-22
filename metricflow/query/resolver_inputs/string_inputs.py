from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.query.resolver_inputs.query_resolver_inputs import (
    MetricFlowQueryResolverInput,
    ResolverInputForGroupBy,
    ResolverInputForMetric,
    ResolverInputForOrderBy,
)


@dataclass(frozen=True)
class StringResolverInputForMetric(ResolverInputForMetric):
    @staticmethod
    def from_str(metric_name: str) -> StringResolverInputForMetric:
        return StringResolverInputForMetric(
            input_obj=metric_name, metric_reference=MetricReference(element_name=metric_name.lower())
        )


@dataclass(frozen=True)
class StringResolverInputForGroupBy(ResolverInputForGroupBy):
    @property
    @override
    def ui_description(self) -> str:
        return repr(self.input_obj)


@dataclass(frozen=True)
class InvalidStringForOrderBy(ResolverInputForOrderBy):
    input_str: str

    @property
    def ui_description(self) -> str:
        return self.input_str


@dataclass(frozen=True)
class StringInputForOrderBy(ResolverInputForOrderBy):
    input_str: str

    @property
    @override
    def ui_description(self) -> str:
        return self.input_str


@dataclass(frozen=True)
class InvalidStringInput(MetricFlowQueryResolverInput):
    input_str: str

    @property
    @override
    def ui_description(self) -> str:
        return repr(self.input_str)
