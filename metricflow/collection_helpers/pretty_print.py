from __future__ import annotations

import logging
import pprint
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, List, Optional, Sized, Union

from pydantic import BaseModel

from metricflow.formatting import indent_log_line

logger = logging.getLogger(__name__)


class MetricFlowPrettyFormatter:
    def __init__(  # noqa: D
        self,
        indent_prefix: str,
        max_line_length: int,
        include_object_field_names: bool,
        include_none_object_fields: bool,
        include_empty_object_fields: bool,
    ) -> None:
        self._indent_prefix = indent_prefix
        self._max_line_width = max_line_length
        self._include_object_field_names = include_object_field_names
        self._include_none_object_fields = include_none_object_fields
        self._include_empty_object_fields = include_empty_object_fields

    @staticmethod
    def _is_pydantic_base_model(obj: Any):  # type:ignore
        return isinstance(obj, BaseModel)

    def _handle_list_like_obj(self, list_like_obj: Union[list, tuple], remaining_line_width: Optional[int]) -> str:
        if isinstance(list_like_obj, list):
            left_enclose_str = "["
            right_enclose_str = "]"
        elif isinstance(list_like_obj, tuple):
            left_enclose_str = "("
            right_enclose_str = ")"
        else:
            raise RuntimeError(f"Unhandled type: {type(list_like_obj)}")

        if len(list_like_obj) == 0:
            return f"{left_enclose_str}{right_enclose_str}"

        items_as_str = tuple(self._handle_any_obj(list_item, remaining_line_width=None) for list_item in list_like_obj)

        line_items = [left_enclose_str]
        if len(items_as_str) > 0:
            line_items.extend([", ".join(items_as_str)])
            if len(items_as_str) == 1:
                line_items.append(",")
        line_items.append(right_enclose_str)
        result_without_width_limit = "".join(line_items)

        if remaining_line_width is None or len(result_without_width_limit) <= remaining_line_width:
            return result_without_width_limit

        items_as_str = tuple(
            self._handle_any_obj(
                list_item, remaining_line_width=max(0, remaining_line_width - len(self._indent_prefix))
            )
            for list_item in list_like_obj
        )
        item_block = ",\n".join(items_as_str)

        lines = [left_enclose_str]
        if len(item_block) > 0:
            # Add trailing comma if there are items.
            lines.append(indent_log_line(item_block, indent_prefix=self._indent_prefix) + ",")
        lines.append(right_enclose_str)
        return "\n".join(lines)

    def _handle_indented_key_value_item(  # type: ignore[misc]
        self,
        key: Any,
        value: Any,
        key_value_seperator: str,
        is_dataclass_like_object: bool,
        remaining_line_width: Optional[int],
    ) -> str:
        # Handle case if the string representation fits on one line.
        if is_dataclass_like_object:
            if not self._include_none_object_fields and value is None:
                return ""

        if remaining_line_width is None or remaining_line_width > 0:
            result_items_without_limit: List[str] = []
            if is_dataclass_like_object and self._include_object_field_names:
                result_items_without_limit.append(str(key))
            else:
                self._handle_any_obj(key, remaining_line_width=remaining_line_width)
            result_items_without_limit.append(key_value_seperator)
            result_items_without_limit.append(self._handle_any_obj(value, remaining_line_width=None))

            result_without_limit = "".join(result_items_without_limit)
            if remaining_line_width is None or len(result_without_limit) <= remaining_line_width:
                return result_without_limit

        # Handle multi-line case.
        result_lines: List[str] = []
        if is_dataclass_like_object and self._include_object_field_names:
            result_lines.append(str(key) + key_value_seperator.rstrip())
        else:
            key_lines = self._handle_any_obj(key, remaining_line_width=remaining_line_width).splitlines()
            if len(key_lines) > 1:
                result_lines.append(key_lines[0])
            result_lines.extend(key_lines[1:-1])
            result_lines.append(key_lines[-1] + key_value_seperator.rstrip())

        value_lines = self._handle_any_obj(
            value, remaining_line_width=max(0, remaining_line_width - len(self._indent_prefix))
        ).splitlines()

        if len(value_lines) > 1:
            result_lines[-1] = result_lines[-1] + value_lines[0]
            result_lines.append(indent_log_line("\n".join(value_lines[1:-1]), indent_prefix=self._indent_prefix))
            result_lines.append(value_lines[-1])
            return "\n".join(result_lines)

        result_lines.append(indent_log_line(value_lines[0], indent_prefix=self._indent_prefix))

        return "\n".join(result_lines)

    def _handle_mapping_like_obj(
        self,
        mapping: Mapping,
        left_enclose_str: str,
        key_value_seperator: str,
        right_enclose_str: str,
        is_dataclass_like_object: bool,
        include_empty_values: bool,
        remaining_line_width: Optional[int],
    ) -> str:
        if not include_empty_values:
            mapping = {
                key: value
                for key, value in mapping.items()
                if (isinstance(value, Sized) and len(value) > 0) or (not isinstance(value, Sized))
            }

        if len(mapping) == 0:
            return f"{left_enclose_str}{right_enclose_str}"
        # Handle case if the string representation fits on one line.
        if remaining_line_width is None or remaining_line_width > 0:
            comma_separated_items: List[str] = []
            for key, value in mapping.items():
                if is_dataclass_like_object and not self._include_none_object_fields and value is None:
                    continue
                key_value_str_items: List[str] = []

                if is_dataclass_like_object:
                    if self._include_object_field_names:
                        key_value_str_items.append(str(key))
                        key_value_str_items.append(key_value_seperator)
                else:
                    key_value_str_items.append(self._handle_any_obj(key, remaining_line_width=None))
                    key_value_str_items.append(key_value_seperator)
                key_value_str_items.append(self._handle_any_obj(value, remaining_line_width=None))
                comma_separated_items.append("".join(key_value_str_items))
            result_without_limit = "".join((left_enclose_str, ", ".join(comma_separated_items), right_enclose_str))

            if remaining_line_width is None or len(result_without_limit) <= remaining_line_width:
                return result_without_limit

        # Handle multi-line case.
        mapping_items_as_str = []
        for key, value in mapping.items():
            if is_dataclass_like_object and not self._include_none_object_fields and value is None:
                continue
            mapping_items_as_str.append(
                self._handle_indented_key_value_item(
                    key=key,
                    value=value,
                    key_value_seperator=key_value_seperator,
                    is_dataclass_like_object=is_dataclass_like_object,
                    remaining_line_width=max(0, remaining_line_width - len(self._indent_prefix)),
                )
            )
        lines = [left_enclose_str]
        if len(mapping_items_as_str) > 0:
            indented_block = indent_log_line(",\n".join(mapping_items_as_str), indent_prefix=self._indent_prefix)
            lines.append(indented_block + ",")
        lines.append(right_enclose_str)
        return "\n".join(lines)

    def _handle_any_obj(self, obj: Any, remaining_line_width: Optional[int]) -> str:  # type: ignore
        if isinstance(obj, Enum):
            return obj.name

        if isinstance(obj, (list, tuple)):
            return self._handle_list_like_obj(obj, remaining_line_width=remaining_line_width)

        if isinstance(obj, dict):
            return self._handle_mapping_like_obj(
                obj,
                left_enclose_str="{",
                key_value_seperator=": ",
                right_enclose_str="}",
                is_dataclass_like_object=False,
                remaining_line_width=remaining_line_width,
                include_empty_values=True,
            )

        if is_dataclass(obj):
            mapping = {field.name: getattr(obj, field.name) for field in fields(obj)}
            return self._handle_mapping_like_obj(
                mapping,
                left_enclose_str=type(obj).__name__ + "(",
                key_value_seperator="=",
                right_enclose_str=")",
                is_dataclass_like_object=True,
                remaining_line_width=remaining_line_width,
                include_empty_values=self._include_empty_object_fields,
            )

        if MetricFlowPrettyFormatter._is_pydantic_base_model(obj):
            mapping = {key: getattr(obj, key) for key in obj.dict().keys()}
            return self._handle_mapping_like_obj(
                mapping,
                left_enclose_str=type(obj).__name__ + "(",
                key_value_seperator="=",
                right_enclose_str=")",
                is_dataclass_like_object=True,
                remaining_line_width=remaining_line_width,
                include_empty_values=self._include_empty_object_fields,
            )

        # Any other object that's not handled.
        return pprint.pformat(obj, width=self._max_line_width, sort_dicts=False)

    def pretty_format(self, obj: Any) -> str:  # type: ignore[misc]
        return self._handle_any_obj(obj, remaining_line_width=self._max_line_width)


def mf_pformat(  # type: ignore
    obj: Any,
    max_line_length: int = 120,
    indent_prefix: str = "  ",
    include_object_field_names: bool = True,
    include_none_object_fields: bool = False,
    include_empty_object_fields: bool = False,
) -> str:
    """Print objects in a pretty way for logging / test snapshots.

    In Python 3.10, the pretty printer class will support dataclasses, so we can remove this once we're on
    3.10. Also tried the prettyprint package with dataclasses, but that prints full names for the classes
    e.g. a.b.MyClass and it also always added line breaks, even if an object could fit on one line, so
    preferring to not use that for compactness.

    e.g.
        metricflow.specs.DimensionSpec(
            element_name='country',
            entity_links=()
        ),

    Instead, the below will print something like:

        DimensionSpec(element_name='country', entity_links=())

    Also, this simplifies the object representation in some cases (e.g. Enums) and provides options for a more compact
    string.

    Args:
        obj: The object to convert to string.
        max_line_length: If the string representation is going to be longer than this, split into multiple lines.
        indent_prefix: The prefix to use for hierarchical indents.
        include_object_field_names: Include field names when printing objects - e.g. Foo(bar='baz') vs Foo('baz')
        include_none_object_fields: Include fields with a None value - e.g. Foo(bar=None) vs Foo()
        include_empty_object_fields: Include fields that are empty - e.g. Foo(bar=()) vs Foo()

    Returns:
        A string representation of the object that's useful for logging / debugging.
    """
    formatter = MetricFlowPrettyFormatter(
        indent_prefix=indent_prefix,
        max_line_length=max_line_length,
        include_object_field_names=include_object_field_names,
        include_none_object_fields=include_none_object_fields,
        include_empty_object_fields=include_empty_object_fields,
    )
    return formatter.pretty_format(obj)
