from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Sequence, Set, Tuple

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableElementType,
    LinkableEntity,
    LinkableMetric,
)
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_classes import (
    InstanceSpec,
    LinkableInstanceSpec,
)


@dataclass(frozen=True)
class LinkableElementSet(SemanticModelDerivation):
    """Container class for storing all linkable elements for a metric.

    TODO: There are similarities with LinkableSpecSet - consider consolidation.
    """

    path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = field(default_factory=dict)
    path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = field(default_factory=dict)
    path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Basic validation for ensuring consistency between path key type and value type."""
        mismatched_dimensions = tuple(
            path_key
            for path_key in self.path_key_to_linkable_dimensions.keys()
            if not path_key.element_type.is_dimension_type
        )
        mismatched_entities = tuple(
            path_key
            for path_key in self.path_key_to_linkable_entities
            if path_key.element_type is not LinkableElementType.ENTITY
        )
        mismatched_metrics = tuple(
            path_key
            for path_key in self.path_key_to_linkable_metrics
            if path_key.element_type is not LinkableElementType.METRIC
        )

        mismatched_elements = {
            "dimensions": mismatched_dimensions,
            "entities": mismatched_entities,
            "metrics": mismatched_metrics,
        }

        assert all(len(mismatches) == 0 for mismatches in mismatched_elements.values()), (
            f"Found one or more elements where the element type defined in the path key does not match the value "
            f"type! Mismatched elements: {mismatched_elements}"
        )

        # There shouldn't be a path key without any concrete items. Can be an issue as specs contained in this set are
        # generated from the path keys.
        for key, value in (
            tuple(self.path_key_to_linkable_dimensions.items())
            + tuple(self.path_key_to_linkable_entities.items())
            + tuple(self.path_key_to_linkable_metrics.items())
        ):
            assert len(value) > 0, f"{key} is empty"

        # There shouldn't be any duplicate specs.
        specs = self.specs
        deduped_specs = set(specs)
        assert len(deduped_specs) == len(specs)
        assert len(deduped_specs) == (
            len(self.path_key_to_linkable_dimensions)
            + len(self.path_key_to_linkable_entities)
            + len(self.path_key_to_linkable_metrics)
        )

        # Check time dimensions have the grain set.
        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            if path_key.element_type is LinkableElementType.TIME_DIMENSION:
                for linkable_dimension in linkable_dimensions:
                    assert (
                        linkable_dimension.time_granularity is not None
                    ), f"{path_key} has a dimension without the time granularity set: {linkable_dimension}"

    @staticmethod
    def merge_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Combine multiple sets together by the path key.

        If there are elements with the same join key and different element(s) in the tuple of values,
        those elements will be categorized as ambiguous.
        Note this does not deduplicate values, so there may be unambiguous merged sets that appear to have
        multiple values if all one does is a simple length check.
        """
        key_to_linkable_dimensions: Dict[ElementPathKey, List[LinkableDimension]] = defaultdict(list)
        key_to_linkable_entities: Dict[ElementPathKey, List[LinkableEntity]] = defaultdict(list)
        key_to_linkable_metrics: Dict[ElementPathKey, List[LinkableMetric]] = defaultdict(list)

        for linkable_element_set in linkable_element_sets:
            for path_key, linkable_dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                key_to_linkable_dimensions[path_key].extend(linkable_dimensions)
            for path_key, linkable_entities in linkable_element_set.path_key_to_linkable_entities.items():
                key_to_linkable_entities[path_key].extend(linkable_entities)
            for path_key, linkable_metrics in linkable_element_set.path_key_to_linkable_metrics.items():
                key_to_linkable_metrics[path_key].extend(linkable_metrics)

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(dimensions) for path_key, dimensions in key_to_linkable_dimensions.items()
            },
            path_key_to_linkable_entities={
                path_key: tuple(entities) for path_key, entities in key_to_linkable_entities.items()
            },
            path_key_to_linkable_metrics={
                path_key: tuple(metrics) for path_key, metrics in key_to_linkable_metrics.items()
            },
        )

    @staticmethod
    def intersection_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Find the intersection of all elements in the sets by path key.

        This will return the intersection of all path keys defined in the sets, but the union of elements associated
        with each path key. In other words, it filters out path keys (i.e., linkable specs) that are not referenced
        in every set in the input sequence, but it preserves all of the various potentially ambiguous LinkableElement
        instances associated with the path keys that remain.

        This is useful to figure out the common dimensions that are possible to query with multiple metrics. You would
        find the LinkableSpecSet for each metric in the query, then do an intersection of the sets.
        """
        if len(linkable_element_sets) == 0:
            return LinkableElementSet()
        elif len(linkable_element_sets) == 1:
            return linkable_element_sets[0]

        # Find path keys that are common to all LinkableElementSets.
        dimension_path_keys: List[Set[ElementPathKey]] = []
        entity_path_keys: List[Set[ElementPathKey]] = []
        metric_path_keys: List[Set[ElementPathKey]] = []
        for linkable_element_set in linkable_element_sets:
            dimension_path_keys.append(set(linkable_element_set.path_key_to_linkable_dimensions.keys()))
            entity_path_keys.append(set(linkable_element_set.path_key_to_linkable_entities.keys()))
            metric_path_keys.append(set(linkable_element_set.path_key_to_linkable_metrics.keys()))
        common_linkable_dimension_path_keys = set.intersection(*dimension_path_keys) if dimension_path_keys else set()
        common_linkable_entity_path_keys = set.intersection(*entity_path_keys) if entity_path_keys else set()
        common_linkable_metric_path_keys = set.intersection(*metric_path_keys) if metric_path_keys else set()
        # Create a new LinkableElementSet that only includes items where the path key is common to all sets.
        join_path_to_linkable_dimensions: Dict[ElementPathKey, Set[LinkableDimension]] = defaultdict(set)
        join_path_to_linkable_entities: Dict[ElementPathKey, Set[LinkableEntity]] = defaultdict(set)
        join_path_to_linkable_metrics: Dict[ElementPathKey, Set[LinkableMetric]] = defaultdict(set)

        for linkable_element_set in linkable_element_sets:
            for path_key, linkable_dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                if path_key in common_linkable_dimension_path_keys:
                    join_path_to_linkable_dimensions[path_key].update(linkable_dimensions)
            for path_key, linkable_entities in linkable_element_set.path_key_to_linkable_entities.items():
                if path_key in common_linkable_entity_path_keys:
                    join_path_to_linkable_entities[path_key].update(linkable_entities)
            for path_key, linkable_metrics in linkable_element_set.path_key_to_linkable_metrics.items():
                if path_key in common_linkable_metric_path_keys:
                    join_path_to_linkable_metrics[path_key].update(linkable_metrics)

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(
                    sorted(
                        dimensions,
                        key=lambda linkable_dimension: (
                            linkable_dimension.semantic_model_origin.semantic_model_name
                            if linkable_dimension.semantic_model_origin
                            else ""
                        ),
                    )
                )
                for path_key, dimensions in join_path_to_linkable_dimensions.items()
            },
            path_key_to_linkable_entities={
                path_key: tuple(
                    sorted(
                        entities, key=lambda linkable_entity: linkable_entity.semantic_model_origin.semantic_model_name
                    )
                )
                for path_key, entities in join_path_to_linkable_entities.items()
            },
            path_key_to_linkable_metrics={
                path_key: tuple(
                    sorted(
                        metrics, key=lambda linkable_metric: linkable_metric.join_by_semantic_model.semantic_model_name
                    )
                )
                for path_key, metrics in join_path_to_linkable_metrics.items()
            },
        )

    def filter(
        self,
        with_any_of: FrozenSet[LinkableElementProperty],
        without_any_of: FrozenSet[LinkableElementProperty] = frozenset(),
        without_all_of: FrozenSet[LinkableElementProperty] = frozenset(),
    ) -> LinkableElementSet:
        """Filter elements in the set.

        First, only elements with at least one property in the "with_any_of" set are retained. Then, any elements with
        a property in "without_any_of" set are removed. Lastly, any elements with all properties in without_all_of
        are removed.
        """
        key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            filtered_linkable_dimensions = tuple(
                linkable_dimension
                for linkable_dimension in linkable_dimensions
                if len(linkable_dimension.properties.intersection(with_any_of)) > 0
                and len(linkable_dimension.properties.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_dimension.properties.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_dimensions) > 0:
                key_to_linkable_dimensions[path_key] = filtered_linkable_dimensions

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            filtered_linkable_entities = tuple(
                linkable_entity
                for linkable_entity in linkable_entities
                if len(linkable_entity.properties.intersection(with_any_of)) > 0
                and len(linkable_entity.properties.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_entity.properties.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_entities) > 0:
                key_to_linkable_entities[path_key] = filtered_linkable_entities

        for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items():
            filtered_linkable_metrics = tuple(
                linkable_metric
                for linkable_metric in linkable_metrics
                if len(linkable_metric.properties.intersection(with_any_of)) > 0
                and len(linkable_metric.properties.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_metric.properties.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_metrics) > 0:
                key_to_linkable_metrics[path_key] = filtered_linkable_metrics

        return LinkableElementSet(
            path_key_to_linkable_dimensions=key_to_linkable_dimensions,
            path_key_to_linkable_entities=key_to_linkable_entities,
            path_key_to_linkable_metrics=key_to_linkable_metrics,
        )

    @property
    def only_unique_path_keys(self) -> LinkableElementSet:
        """Returns a set that only includes path keys that map to a single distinct element."""
        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(set(linkable_dimensions))
                for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items()
                if len(set(linkable_dimensions)) <= 1
            },
            path_key_to_linkable_entities={
                path_key: tuple(set(linkable_entities))
                for path_key, linkable_entities in self.path_key_to_linkable_entities.items()
                if len(set(linkable_entities)) <= 1
            },
            path_key_to_linkable_metrics={
                path_key: tuple(set(linkable_metrics))
                for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items()
                if len(set(linkable_metrics)) <= 1
            },
        )

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        semantic_model_references: Set[SemanticModelReference] = set()
        for linkable_dimensions in self.path_key_to_linkable_dimensions.values():
            for linkable_dimension in linkable_dimensions:
                semantic_model_references.update(linkable_dimension.derived_from_semantic_models)
        for linkable_entities in self.path_key_to_linkable_entities.values():
            for linkable_entity in linkable_entities:
                semantic_model_references.update(linkable_entity.derived_from_semantic_models)
        for linkable_metrics in self.path_key_to_linkable_metrics.values():
            for linkable_metric in linkable_metrics:
                semantic_model_references.update(linkable_metric.derived_from_semantic_models)

        return sorted(semantic_model_references, key=lambda reference: reference.semantic_model_name)

    @property
    def spec_count(self) -> int:
        """If this is mapped to spec objects, the number of specs that would be produced."""
        return (
            len(self.path_key_to_linkable_dimensions.keys())
            + len(self.path_key_to_linkable_entities.keys())
            + len(self.path_key_to_linkable_metrics.keys())
        )

    @property
    def specs(self) -> Sequence[LinkableInstanceSpec]:
        """Converts the items in a `LinkableElementSet` to their corresponding spec objects."""
        return tuple(
            path_key.spec
            for path_key in (
                tuple(self.path_key_to_linkable_dimensions.keys())
                + tuple(self.path_key_to_linkable_entities.keys())
                + tuple(self.path_key_to_linkable_metrics.keys())
            )
        )

    def filter_by_spec_patterns(self, spec_patterns: Sequence[SpecPattern]) -> LinkableElementSet:
        """Filter the elements in the set by the given spec patters.

        Returns a new set consisting of the elements in the `LinkableElementSet` that have a corresponding spec that
        match all the given spec patterns.
        """
        # Spec patterns need all specs to match properly e.g. `BaseTimeGrainPattern`.
        matching_specs: Sequence[InstanceSpec] = self.specs

        for spec_pattern in spec_patterns:
            matching_specs = spec_pattern.match(matching_specs)
        specs_to_include = set(matching_specs)

        path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            if path_key.spec in specs_to_include:
                path_key_to_linkable_dimensions[path_key] = linkable_dimensions

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            if path_key.spec in specs_to_include:
                path_key_to_linkable_entities[path_key] = linkable_entities

        for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items():
            if path_key.spec in specs_to_include:
                path_key_to_linkable_metrics[path_key] = linkable_metrics

        return LinkableElementSet(
            path_key_to_linkable_dimensions=path_key_to_linkable_dimensions,
            path_key_to_linkable_entities=path_key_to_linkable_entities,
            path_key_to_linkable_metrics=path_key_to_linkable_metrics,
        )
