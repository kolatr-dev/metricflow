from __future__ import annotations

from copy import deepcopy

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule


def copy_manifest(
    semantic_manifest: PydanticSemanticManifest,
    transform_rule: SemanticManifestTransformRule[PydanticSemanticManifest],
) -> PydanticSemanticManifest:
    copied_manifest = deepcopy(semantic_manifest)

    transformer = PydanticSemanticManifestTransformer()
    copied_manifest = transformer.transform(semantic_manifest, ((transform_rule,),))

    return copied_manifest
