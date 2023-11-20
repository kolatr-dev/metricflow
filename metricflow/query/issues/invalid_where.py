from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.call_parameter_sets import ParseWhereFilterException
from dbt_semantic_interfaces.protocols import WhereFilter
from typing_extensions import override

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionPath,
)


@dataclass(frozen=True)
class InvalidWhereFilterFormatIssue(MetricFlowQueryResolutionIssue):
    where_filter: WhereFilter
    parse_exception: ParseWhereFilterException

    @staticmethod
    def from_parameters(
        where_filter: WhereFilter,
        parse_exception: ParseWhereFilterException,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidWhereFilterFormatIssue:
        return InvalidWhereFilterFormatIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            where_filter=where_filter,
            parse_exception=parse_exception,
        )

    @override
    def ui_description(self, naming_scheme: Optional[QueryItemNamingScheme]) -> str:
        return (
            f"The where_filter {repr(self.where_filter.where_sql_template)} does not follow a known format."
            f"Got exception:\n"
            f"{''.join(traceback.TracebackException.from_exception(self.parse_exception).format())}"
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> InvalidWhereFilterFormatIssue:
        return InvalidWhereFilterFormatIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            where_filter=self.where_filter,
            parse_exception=self.parse_exception,
        )
