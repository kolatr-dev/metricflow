from __future__ import annotations

import logging
from typing import List, Optional, Sequence

import jinja2
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow.filters.filter_renderer import WhereFilterRenderer
from metricflow.query.group_by_item.resolve_filters.filter_to_pattern import ResolvedSpecLookup
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import LinkableSpecSet, WhereFilterSpec
from metricflow.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)


class RenderSqlTemplateException(Exception):  # noqa: D
    pass


class WhereSpecFactory:
    """Renders the SQL template in the WhereFilter and converts it to a WhereFilterSpec."""

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        resolved_spec_lookup: ResolvedSpecLookup,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = resolved_spec_lookup

    def create_from_where_filter_intersection(  # noqa: D
        self,
        metric_references: Sequence[MetricReference],
        where_filter_intersection: Optional[WhereFilterIntersection],
    ) -> Sequence[WhereFilterSpec]:
        if where_filter_intersection is None:
            return ()

        filter_specs: List[WhereFilterSpec] = []

        for where_filter in where_filter_intersection.where_filters:
            renderer = WhereFilterRenderer(
                column_association_resolver=self._column_association_resolver,
                resolved_spec_lookup=self._resolved_spec_lookup,
                metric_references=metric_references,
            )

            try:
                # If there was an error with the template, it should have been caught while resolving the specs for
                # the filters during query resolution.
                where_sql = jinja2.Template(where_filter.where_sql_template, undefined=jinja2.StrictUndefined).render(
                    {
                        "Dimension": renderer.dimension_call_jinja_function,
                        "TimeDimension": renderer.time_dimension_jinja_function,
                        "Entity": renderer.entity_jinja_function,
                    }
                )
            except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
                raise RenderSqlTemplateException(
                    f"Error while rendering Jinja template:\n{where_filter.where_sql_template}"
                ) from e
            filter_specs.append(
                WhereFilterSpec(
                    where_sql=where_sql,
                    bind_parameters=SqlBindParameters(),
                    linkable_spec_set=LinkableSpecSet.from_specs(renderer.rendered_specs),
                )
            )

        return filter_specs
