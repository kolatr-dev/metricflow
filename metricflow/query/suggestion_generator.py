from __future__ import annotations

import logging
from typing import Optional, Sequence

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.similarity import top_fuzzy_matches
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import InstanceSpec

logger = logging.getLogger(__name__)


class QueryItemSuggestionGenerator:
    """Returns specs that partially match a spec pattern created from user input. Used for suggestions in errors.

    Since suggestions are needed for group-by-items specified in the query and in where filters, an optional candidate
    filter can be specified to limit suggestions to the ones valid for the entire query. For use with where filters,
    a candidate filter is not needed as any available spec at a resolution node can be used.
    """

    def __init__(  # noqa: D
        self, input_naming_scheme: QueryItemNamingScheme, input_str: str, candidate_filter: Optional[SpecPattern]
    ) -> None:
        self._input_naming_scheme = input_naming_scheme
        self._input_str = input_str
        self._candidate_filter = candidate_filter

    def input_suggestions(
        self,
        candidate_specs: Sequence[InstanceSpec],
        max_suggestions: int = 6,
    ) -> Sequence[str]:
        """Return the best specs that match the given pattern from candidate_specs and match the candidate_filer."""
        if self._candidate_filter is not None:
            candidate_specs = self._candidate_filter.match(candidate_specs)

        # Use edit distance to figure out the closest matches, so convert the specs to strings.
        spec_str_to_spec = {}
        for candidate_spec in candidate_specs:
            candidate_spec_str = self._input_naming_scheme.input_str(candidate_spec)

            if candidate_spec_str is None:
                continue

            spec_str_to_spec[candidate_spec_str] = candidate_spec

        fuzzy_matches = top_fuzzy_matches(
            item=self._input_str,
            candidate_items=tuple(spec_str_to_spec.keys()),
            max_matches=max_suggestions,
        )

        # TODO: Remove before PR.
        logger.error(f"Matches are :\n{mf_pformat(fuzzy_matches)}")

        top_similarity_strs = tuple(scored_item.item_str for scored_item in fuzzy_matches)

        return top_similarity_strs
