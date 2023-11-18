from __future__ import annotations

from typing import List, Sequence

from metricflow.specs.specs import LinkableInstanceSpec


class RenderedSpecTracker:
    def __init__(self) -> None:  # noqa: D
        self._rendered_specs: List[LinkableInstanceSpec] = []

    def record_rendered_spec(self, spec: LinkableInstanceSpec) -> None:
        self._rendered_specs.append(spec)

    @property
    def rendered_specs(self) -> Sequence[LinkableInstanceSpec]:
        return self._rendered_specs
