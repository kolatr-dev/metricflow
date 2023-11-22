# from __future__ import annotations
#
# import logging
#
# from dbt_semantic_interfaces.pretty_print import pformat_big_objects
#
# from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolution
# from metricflow.query.resolver_inputs.string_inputs import StringResolverInputForGroupBy
#
# logger = logging.getLogger(__name__)
#
#
# def group_by_item_resolution_to_snapshot_text(
#     result: GroupByItemResolution,
#     resolver_input: StringResolverInputForGroupBy,
# ) -> str:
#     naming_scheme = resolver_input.naming_scheme
#     dict_to_snapshot = {
#         "spec": naming_scheme.input_str(result.spec) if result.spec is not None else None,
#     }
#
#     for i, issue in enumerate(result.issue_set.issues):
#         dict_to_snapshot[f"issue_{i}_description"] = issue.ui_description(naming_scheme)
#         dict_to_snapshot[f"issue_{i}_location"] = issue.query_resolution_path.ui_description
#
#     return pformat_big_objects(**dict_to_snapshot)
