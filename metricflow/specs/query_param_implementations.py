from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.specs.query_interface import QueryParameter
from metricflow.time.date_part import DatePart


@dataclass(frozen=True)
class TimeDimensionParameter:
    """Time dimension requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        parsed_name = StructuredLinkableSpecName.from_name(self.name)
        if parsed_name.time_granularity:
            raise ValueError("Must use object syntax for `grain` parameter if `date_part` is requested.")


@dataclass(frozen=True)
class DimensionParameter:
    """Categorical dimension requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        if self.grain or self.date_part:
            raise TypeError("Grain and date_part not supported on categorical DimensionParameter.")


@dataclass(frozen=True)
class EntityParameter:
    """Entity requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        if self.grain or self.date_part:
            raise TypeError("Grain and date_part not supported on EntityParameter.")


@dataclass(frozen=True)
class MetricParameter:
    """Metric requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        if self.grain or self.date_part:
            raise TypeError("Grain and date_part not supported on MetricParameter.")


@dataclass(frozen=True)
class OrderByParameter:
    """Order by requested in a query."""

    order_by: QueryParameter
    descending: bool = False