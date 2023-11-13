from __future__ import annotations

import logging
import textwrap

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY

logger = logging.getLogger(__name__)


def test_literals() -> None:  # noqa: D
    assert mf_pformat(1) == "1"
    assert mf_pformat(1.0) == "1.0"
    assert mf_pformat("foo") == "'foo'"


def test_containers() -> None:  # noqa: D
    assert mf_pformat((1,)) == "(1,)"
    assert mf_pformat(((1, 2), 3)) == "((1, 2), 3)"
    assert mf_pformat([[1, 2], 3]) == "[[1, 2], 3]"
    assert mf_pformat({"a": ((1, 2), 3), (1, 2): 3}) == "{'a': ((1, 2), 3), (1, 2): 3}"


def test_classes() -> None:  # noqa: D
    assert "TimeDimensionSpec('metric_time', DAY)" == mf_pformat(
        MTD_SPEC_DAY,
        include_object_field_names=False,
        include_none_object_fields=False,
        include_empty_object_fields=False,
    )

    logger.error(
        "result is:\n"
        + mf_pformat(
            MTD_SPEC_DAY,
            include_object_field_names=True,
            include_none_object_fields=True,
            include_empty_object_fields=True,
        )
    )
    assert (
        textwrap.dedent(
            """\
            TimeDimensionSpec(
              element_name='metric_time',
              entity_links=(),
              time_granularity=DAY,
              date_part=None,
              aggregation_state=None,
            )
            """
        ).rstrip()
        == mf_pformat(
            MTD_SPEC_DAY,
            include_object_field_names=True,
            include_none_object_fields=True,
            include_empty_object_fields=True,
        )
    )

    assert "TimeDimensionSpec(element_name='metric_time', time_granularity=DAY)" == mf_pformat(MTD_SPEC_DAY)


def test_multi_line_key_value() -> None:
    """Test a dict where the key and value needs to be printed on multiple lines."""
    assert (
        textwrap.dedent(
            """\
            {
              (
                1,
                2,
                3,
              ): (
                4,
                5,
                6,
              ),
            }
            """
        ).rstrip()
        == mf_pformat(
            obj={(1, 2, 3): (4, 5, 6)},
            max_line_length=1,
        )
    )


def test_pydantic_model() -> None:  # noqa: D
    assert "PydanticDimension(name='foo', type=CATEGORICAL, is_partition=False)" == mf_pformat(
        PydanticDimension(name="foo", type=DimensionType.CATEGORICAL)
    )
