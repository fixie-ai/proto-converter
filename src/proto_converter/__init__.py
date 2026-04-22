"""Automatic conversion between compatible protocol buffer types."""

from proto_converter.converter import ProtoConverter
from proto_converter.converter import add_module_resolver_rule
from proto_converter.converter import convert
from proto_converter.converter import convert_field
from proto_converter.converter import get_converter
from proto_converter.converter import remove_module_resolver_rule
from proto_converter.converter import set_module_resolver
from proto_converter.converter import set_type_resolver

__all__ = [
    "ProtoConverter",
    "add_module_resolver_rule",
    "convert",
    "convert_field",
    "get_converter",
    "remove_module_resolver_rule",
    "set_module_resolver",
    "set_type_resolver",
]
