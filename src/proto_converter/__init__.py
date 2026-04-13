"""Automatic conversion between compatible protocol buffer types."""

from proto_converter.converter import ProtoConverter
from proto_converter.converter import convert
from proto_converter.converter import convert_field
from proto_converter.converter import get_converter
from proto_converter.converter import set_type_resolver

__all__ = [
    "ProtoConverter",
    "convert",
    "convert_field",
    "get_converter",
    "set_type_resolver",
]
