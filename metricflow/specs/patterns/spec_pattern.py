from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from metricflow.specs.specs import LinkableInstanceSpec


class SpecPattern(ABC):
    """A pattern is used to select specs from a group of candidate specs based on class-defined criteria.

    This could be named SpecFilter as well, but a filter is often used in the context of the WhereFilter.
    """

    @abstractmethod
    def match(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        """Given a sequence of candidate specs, return the ones that match this pattern."""
        raise NotImplementedError
