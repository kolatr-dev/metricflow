from __future__ import annotations

from enum import Enum


class IdPrefix(Enum):
    """Enumerates the prefixes used for generating IDs.

    TODO: Move all ID prefixes here.
    """

    # Group by item resolution
    GROUP_BY_ITEM_RESOLUTION_DAG = "gbir"
    MERGE_AT_QUERY = "maq"
    MERGE_AT_METRIC = "mamet"
    SOURCE_FROM_MEASURE = "ramea"
    SELECT_CANDIDATE = "sc"
