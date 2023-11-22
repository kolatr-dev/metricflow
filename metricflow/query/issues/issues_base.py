from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence, Tuple

from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.resolver_inputs.query_resolver_inputs import MetricFlowQueryResolverInput


class MetricFlowQueryIssueType(Enum):
    """Errors prevent the query from running, where warnings do not.

    TODO: Add warning type.
    """

    ERROR = "ERROR"


@dataclass(frozen=True)
class MetricFlowQueryIssue:
    """An issue in the query that needs attention from the user."""

    issue_type: MetricFlowQueryIssueType
    parent_issues: Tuple[MetricFlowQueryIssue, ...]

    @abstractmethod
    def ui_description(self, naming_scheme: QueryItemNamingScheme) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class MetricFlowQueryResolutionPath(PathPrefixable):
    resolution_path_nodes: Tuple[GroupByItemResolutionNode, ...]

    @staticmethod
    def empty_instance() -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=(),
        )

    @property
    def last_item(self) -> GroupByItemResolutionNode:  # noqa: D
        return self.resolution_path_nodes[-1]

    @property
    def ui_description(self) -> str:
        # TODO: Use a type to enforce this.
        assert len(self.resolution_path_nodes) > 0
        descriptions = tuple(f"[Resolve {path_node.ui_description}]" for path_node in self.resolution_path_nodes)
        output = descriptions[0]

        for i, description in enumerate(descriptions[1:]):
            output += "\n"
            output += "  " * (i + 1)
            output += "-> "
            output += description

        return output

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(resolution_path_nodes=(path_prefix_node,) + self.resolution_path_nodes)

    @override
    def __str__(self) -> str:
        items = [self.__class__.__name__, "(", ", ".join(tuple(str(node) for node in self.resolution_path_nodes)), ")"]
        return "".join(items)

    @staticmethod
    def from_path_item(node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=(node,),
        )


@dataclass(frozen=True)
class MetricFlowQueryResolutionIssue(PathPrefixable, ABC):
    issue_type: MetricFlowQueryIssueType
    parent_issues: Tuple[MetricFlowQueryResolutionIssue, ...]
    query_resolution_path: MetricFlowQueryResolutionPath

    @abstractmethod
    def ui_description(self, associated_input: Optional[MetricFlowQueryResolverInput]) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class MetricFlowQueryResolutionIssueSet(Mergeable, PathPrefixable):
    """The result of resolving query inputs to specs."""

    issues: Tuple[MetricFlowQueryResolutionIssue, ...] = ()

    @override
    def merge(self, other: MetricFlowQueryResolutionIssueSet) -> MetricFlowQueryResolutionIssueSet:  # noqa: D
        return MetricFlowQueryResolutionIssueSet(issues=tuple(self.issues) + tuple(other.issues))

    @override
    @classmethod
    def empty_instance(cls) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet()

    @property
    def errors(self) -> Sequence[MetricFlowQueryResolutionIssue]:  # noqa: D
        return tuple(issue for issue in self.issues if issue.issue_type is MetricFlowQueryIssueType.ERROR)

    @property
    def has_errors(self) -> bool:  # noqa: D
        return len(self.errors) > 0

    def add_issue(self, issue: MetricFlowQueryResolutionIssue) -> MetricFlowQueryResolutionIssueSet:  # noqa: D
        return MetricFlowQueryResolutionIssueSet(issues=tuple(self.issues) + (issue,))

    @staticmethod
    def from_issue(issue: MetricFlowQueryResolutionIssue) -> MetricFlowQueryResolutionIssueSet:  # noqa: D
        return MetricFlowQueryResolutionIssueSet(issues=(issue,))

    @staticmethod
    def from_issues(issues: Sequence[MetricFlowQueryResolutionIssue]) -> MetricFlowQueryResolutionIssueSet:  # noqa: D
        return MetricFlowQueryResolutionIssueSet(issues=tuple(issues))

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet(
            issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.issues),
        )

    @property
    def has_issues(self) -> bool:  # noqa: D
        return len(self.issues) > 0
