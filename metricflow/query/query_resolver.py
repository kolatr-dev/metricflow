from __future__ import annotations

import logging
from typing import Dict, List, Mapping, Sequence

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.dag.dag_to_text import dag_as_text
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.dag_builder import GroupByItemResolutionDagBuilder
from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolution, GroupByItemResolver
from metricflow.query.group_by_item.resolution_dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolution_nodes.query_resolution_node import QueryGroupByItemResolutionNode
from metricflow.query.group_by_item.resolve_filters.filter_to_pattern import (
    FilterSpecResolutionLookUp,
    WhereFilterSpecResolver,
)
from metricflow.query.issues.invalid_limit import InvalidLimitIssue
from metricflow.query.issues.invalid_metric import InvalidMetricIssue
from metricflow.query.issues.invalid_order import InvalidOrderByItemIssue
from metricflow.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
    MetricFlowQueryResolutionPath,
)
from metricflow.query.query_resolution import (
    InputToIssueSetMapping,
    InputToIssueSetMappingItem,
    MetricFlowQueryResolution,
)
from metricflow.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForGroupBy,
    ResolverInputForLimit,
    ResolverInputForMetric,
    ResolverInputForOrderBy,
    ResolverInputForQuery,
)
from metricflow.query.validation_rules.query_validator import PostResolutionQueryValidator
from metricflow.specs.specs import (
    LinkableInstanceSpec,
    LinkableSpecSet,
    MetricFlowQuerySpec,
    MetricSpec,
    OrderBySpec,
)

logger = logging.getLogger(__name__)


class MetricFlowQueryResolver:
    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:
        self._manifest_lookup = manifest_lookup
        self._post_resolution_query_validator = PostResolutionQueryValidator(
            manifest_lookup=self._manifest_lookup,
        )

    def _resolve_group_by_item_input(
        self, resolution_dag: GroupByItemResolutionDag, group_by_item_input: ResolverInputForGroupBy
    ) -> GroupByItemResolution:
        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )

        return group_by_item_resolver.resolve_matching_item_for_querying(
            spec_pattern=group_by_item_input.spec_pattern,
        )

    def _resolve_metric_inputs(
        self,
        metric_inputs: Sequence[ResolverInputForMetric],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> Mapping[ResolverInputForMetric, MetricFlowQueryResolutionIssueSet]:
        input_to_issue_set = {}
        for metric_input in metric_inputs:
            metric_reference = metric_input.metric_reference
            if metric_reference not in self._manifest_lookup.metric_lookup.metric_references:
                input_to_issue_set[metric_input] = MetricFlowQueryResolutionIssueSet.from_issue(
                    InvalidMetricIssue.create(
                        invalid_metric_reference=metric_reference,
                        candidate_metric_references=self._manifest_lookup.metric_lookup.metric_references,
                        query_resolution_path=query_resolution_path,
                    )
                )
        return input_to_issue_set

    def _resolve_metric_input(
        self,
        metric_input: ResolverInputForMetric,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        metric_reference = metric_input.metric_reference
        if metric_reference not in self._manifest_lookup.metric_lookup.metric_references:
            return MetricFlowQueryResolutionIssueSet.from_issue(
                InvalidMetricIssue.create(
                    invalid_metric_reference=metric_reference,
                    candidate_metric_references=self._manifest_lookup.metric_lookup.metric_references,
                    query_resolution_path=query_resolution_path,
                )
            )

        return MetricFlowQueryResolutionIssueSet.empty_instance()

    @staticmethod
    def _order_by_item_in_inputs(
        order_by_item_input: ResolverInputForOrderBy,
        metric_inputs: Sequence[ResolverInputForMetric],
        group_by_item_inputs: Sequence[ResolverInputForGroupBy],
    ) -> bool:
        for metric_input in metric_inputs:
            if order_by_item_input.input_item_to_order.input_obj == metric_input.input_obj:
                return True

        for group_by_item_input in group_by_item_inputs:
            if order_by_item_input.input_item_to_order.input_obj == group_by_item_input.input_obj:
                return True

        return False

    def _resolve_order_by(
        self,
        metric_inputs: Sequence[ResolverInputForMetric],
        group_by_item_inputs: Sequence[ResolverInputForGroupBy],
        order_by_item_input: ResolverInputForOrderBy,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        if not self._order_by_item_in_inputs(order_by_item_input, metric_inputs, group_by_item_inputs):
            return MetricFlowQueryResolutionIssueSet.from_issue(
                InvalidOrderByItemIssue.create(
                    order_by_item_input=order_by_item_input,
                    query_resolution_path=query_resolution_path,
                )
            )
        return MetricFlowQueryResolutionIssueSet.empty_instance()

    @staticmethod
    def _resolve_limit(
        limit_input: ResolverInputForLimit, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> MetricFlowQueryResolutionIssueSet:
        limit = limit_input.limit
        if limit is not None and limit < 0:
            return MetricFlowQueryResolutionIssueSet.from_issue(
                InvalidLimitIssue.from_parameters(limit=limit, query_resolution_path=query_resolution_path),
            )
        return MetricFlowQueryResolutionIssueSet.empty_instance()

    def _resolve_where(
        self,
        resolution_dag: GroupByItemResolutionDag,
    ) -> FilterSpecResolutionLookUp:
        where_filter_linkable_spec_resolver = WhereFilterSpecResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )

        return where_filter_linkable_spec_resolver.resolve_lookup()

    def _match_order_by_input_to_spec(
        self,
        group_by_item_input_to_resolved_spec: Dict[ResolverInputForGroupBy, LinkableInstanceSpec],
        order_by_item_inputs: Sequence[ResolverInputForOrderBy],
        metric_inputs: Sequence[ResolverInputForMetric],
    ) -> Sequence[OrderBySpec]:
        order_by_specs = []

        for order_by_input in order_by_item_inputs:
            order_by_input_has_no_match = True
            for metric_input in metric_inputs:
                if order_by_input.input_item_to_order.input_obj == metric_input.input_obj:
                    order_by_specs.append(
                        OrderBySpec(
                            instance_spec=MetricSpec.from_reference(metric_input.metric_reference),
                            descending=order_by_input.descending,
                        )
                    )
                    order_by_input_has_no_match = False
                    break
            if not order_by_input_has_no_match:
                continue

            for group_by_item_input in group_by_item_input_to_resolved_spec.keys():
                if order_by_input.input_item_to_order.input_obj == group_by_item_input.input_obj:
                    order_by_specs.append(
                        OrderBySpec(
                            instance_spec=group_by_item_input_to_resolved_spec[group_by_item_input],
                            descending=order_by_input.descending,
                        )
                    )
                    order_by_input_has_no_match = False

            if order_by_input_has_no_match:
                raise RuntimeError(
                    f"There should have been a match for {order_by_input} as it was previously checked, "
                    f"but did not find one."
                )

        return order_by_specs

    def resolve_query(self, resolver_input_for_query: ResolverInputForQuery) -> MetricFlowQueryResolution:
        metric_inputs = resolver_input_for_query.metric_inputs
        group_by_item_inputs = resolver_input_for_query.group_by_item_inputs
        order_by_item_inputs = resolver_input_for_query.order_by_item_inputs
        limit_input = resolver_input_for_query.limit_input
        filter_input = resolver_input_for_query.filter_input

        # Describe the current resolution path for generating issues as a single query node.
        query_resolution_path = MetricFlowQueryResolutionPath.from_path_item(
            QueryGroupByItemResolutionNode(
                parent_nodes=(),
                metrics_in_query=tuple(metric_input.metric_reference for metric_input in metric_inputs),
                where_filter_intersection=filter_input.where_filter_intersection,
            )
        )

        input_to_issue_set_mapping_items: List[InputToIssueSetMappingItem] = []

        # Resolve metrics.
        for metric_input in metric_inputs:
            issue_set = self._resolve_metric_input(metric_input, query_resolution_path)
            if issue_set.has_issues:
                input_to_issue_set_mapping_items.append(
                    InputToIssueSetMappingItem(
                        resolver_input=metric_input,
                        issue_set=issue_set,
                    )
                )
        metric_references = tuple(metric_input.metric_reference for metric_input in metric_inputs)

        # Resolve order by.
        for order_by_input in order_by_item_inputs:
            issue_set = self._resolve_order_by(
                metric_inputs=metric_inputs,
                group_by_item_inputs=group_by_item_inputs,
                order_by_item_input=order_by_input,
                query_resolution_path=query_resolution_path,
            )
            if issue_set.has_issues:
                input_to_issue_set_mapping_items.append(
                    InputToIssueSetMappingItem(
                        resolver_input=order_by_input,
                        issue_set=issue_set,
                    )
                )

        # Resolve limit
        limit_issue_set = self._resolve_limit(
            limit_input=limit_input,
            query_resolution_path=query_resolution_path,
        )

        if limit_issue_set.has_issues:
            input_to_issue_set_mapping_items.append(
                InputToIssueSetMappingItem(
                    resolver_input=limit_input,
                    issue_set=limit_issue_set,
                )
            )

        # Early stop before resolving further as with invalid metrics, it's difficult to do a good validation.
        # Also including any issues with the order by doesn't hurt.
        if InputToIssueSetMapping(tuple(input_to_issue_set_mapping_items)).has_errors:
            return MetricFlowQueryResolution(
                query_spec=None,
                resolution_dag=None,
                where_filter_resolved_spec_lookup=FilterSpecResolutionLookUp.empty_instance(),
                input_to_issue_set=InputToIssueSetMapping(tuple(input_to_issue_set_mapping_items)),
            )

        # Resolve group by items.
        resolution_dag_builder = GroupByItemResolutionDagBuilder(
            manifest_lookup=self._manifest_lookup,
        )
        resolution_dag = resolution_dag_builder.build(
            metric_references=metric_references,
            where_filter_intersection=filter_input.where_filter_intersection,
        )
        logger.info(f"Resolution DAG is:\n{dag_as_text(resolution_dag)}")

        group_by_item_input_to_resolved_spec: Dict[ResolverInputForGroupBy, LinkableInstanceSpec] = {}
        for group_by_item_input in group_by_item_inputs:
            resolution = self._resolve_group_by_item_input(
                resolution_dag=resolution_dag, group_by_item_input=group_by_item_input
            )
            if resolution.issue_set.has_issues:
                input_to_issue_set_mapping_items.append(
                    InputToIssueSetMappingItem(resolver_input=group_by_item_input, issue_set=resolution.issue_set)
                )
            if resolution.spec is not None:
                group_by_item_input_to_resolved_spec[group_by_item_input] = resolution.spec

        # Resolve where.
        resolved_spec_lookup = self._resolve_where(
            resolution_dag=resolution_dag,
        )

        filter_input_issue_set = resolved_spec_lookup.issue_set

        if filter_input_issue_set.has_issues:
            input_to_issue_set_mapping_items.append(
                InputToIssueSetMappingItem(resolver_input=filter_input, issue_set=filter_input_issue_set)
            )

        # Return if there are any errors since additional fields would not be useful.
        if InputToIssueSetMapping(tuple(input_to_issue_set_mapping_items)).has_errors:
            return MetricFlowQueryResolution(
                query_spec=None,
                resolution_dag=resolution_dag,
                where_filter_resolved_spec_lookup=resolved_spec_lookup,
                input_to_issue_set=InputToIssueSetMapping(tuple(input_to_issue_set_mapping_items)),
            )

        # No errors.
        linkable_spec_set = LinkableSpecSet.from_specs(tuple(group_by_item_input_to_resolved_spec.values()))
        logger.info(f"Group-by-items were resolved to:\n{mf_pformat(linkable_spec_set.as_tuple)}")

        # Figure out the order by specs by matching up the inputs
        order_by_specs = self._match_order_by_input_to_spec(
            metric_inputs=metric_inputs,
            group_by_item_input_to_resolved_spec=group_by_item_input_to_resolved_spec,
            order_by_item_inputs=order_by_item_inputs,
        )

        # Run post-resolution validation rules to generate issues that are generated at the query-level.
        query_level_issue_set = self._post_resolution_query_validator.validate_query(
            resolution_dag=resolution_dag,
            resolver_input_for_query=resolver_input_for_query,
        )

        if query_level_issue_set.has_issues:
            input_to_issue_set_mapping_items.append(
                InputToIssueSetMappingItem(
                    resolver_input=resolver_input_for_query,
                    issue_set=query_level_issue_set,
                )
            )

        return MetricFlowQueryResolution(
            query_spec=MetricFlowQuerySpec(
                metric_specs=tuple(
                    MetricSpec.from_reference(metric_reference) for metric_reference in metric_references
                ),
                dimension_specs=linkable_spec_set.dimension_specs,
                entity_specs=linkable_spec_set.entity_specs,
                time_dimension_specs=linkable_spec_set.time_dimension_specs,
                order_by_specs=tuple(order_by_specs),
                limit=limit_input.limit,
                filter_intersection=filter_input.where_filter_intersection,
                filter_spec_resolution_lookup=resolved_spec_lookup,
            ),
            resolution_dag=resolution_dag,
            where_filter_resolved_spec_lookup=resolved_spec_lookup,
            input_to_issue_set=InputToIssueSetMapping(tuple(input_to_issue_set_mapping_items)),
        )
