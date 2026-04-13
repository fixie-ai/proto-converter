"""Automatic conversion between compatible protocol buffer types."""

from proto_converter.converter import (
    ProtoConverter,
    convert,
    convert_field,
    get_converter,
    set_type_resolver,
)

__all__ = [
    "ProtoConverter",
    "convert",
    "convert_field",
    "get_converter",
    "set_type_resolver",
]
