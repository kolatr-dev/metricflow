from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Sequence

from metricflow.specs.specs import InstanceSpec


class SpecPattern(ABC):
    """A pattern is used to select specs from a group of candidate specs based on class-defined criteria.

    This could be named SpecFilter as well, but a filter is often used in the context of the WhereFilter.
    """

    @abstractmethod
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        """Given candidate specs, return the ones that match this pattern."""
        raise NotImplementedError

    def matches_any(self, candidate_specs: Sequence[InstanceSpec]) -> bool:
        """Returns true if this spec matches any of the given specs."""
        return len(self.match(candidate_specs)) > 0

    def partially_match(self, candidate_specs: Sequence[InstanceSpec], max_items: int) -> Sequence[InstanceSpec]:
        """For generating suggestions, return the specs that most closely match this pattern.

        The specs are returned in the order of closeness, with the closest matches first.
        """
        return self.match(candidate_specs)[:max_items]

    @property
    def input_obj_str(self) -> Optional[str]:
        """If this pattern was generated from user input, return the string associated with the input.

        This is used for generating suggestions via edit distance for error messages.
        """
        return None
