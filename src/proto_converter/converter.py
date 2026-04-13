"""Automatic conversion between compatible protocol buffer types.

Converts fields with matching names and compatible types automatically. Fields that
can't be auto-converted must be handled explicitly via @convert_field or ignored via
IGNORED_FIELDS, otherwise construction raises NotImplementedError.

Nested message fields with different types are auto-converted if a converter exists
(or can be trivially created) for those types.
"""

from __future__ import annotations

import functools
import importlib
import logging
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Generic
from typing import TypeVar

from google.protobuf import any_pb2
from google.protobuf import descriptor as descriptor_mod
from google.protobuf import message
from google.protobuf import symbol_database

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=message.Message)
T = TypeVar("T", bound=message.Message)

FieldDescriptor = descriptor_mod.FieldDescriptor

_registry: dict[tuple[type[message.Message], type[message.Message]], ProtoConverter[Any, Any]] = {}

# User-installable hook for resolving a protobuf Descriptor to its Python class.
# If set, called before the default importlib-based resolution. Should return None
# to fall through to the default.
_type_resolver: Callable[[descriptor_mod.Descriptor], type[message.Message] | None] | None = None


def set_type_resolver(
    resolver: Callable[[descriptor_mod.Descriptor], type[message.Message] | None] | None,
) -> None:
    """Install a custom type resolver for mapping protobuf Descriptors to Python classes.

    The resolver is called with a ``google.protobuf.descriptor.Descriptor`` and should
    return the corresponding Python message class, or ``None`` to fall through to the
    default import-based resolution.

    Pass ``None`` to remove a previously installed resolver.
    """
    global _type_resolver
    _type_resolver = resolver


def _descriptor_to_type(desc: descriptor_mod.Descriptor) -> type[message.Message]:
    """Resolve a protobuf Descriptor to its generated Python class."""
    if _type_resolver is not None:
        result = _type_resolver(desc)
        if result is not None:
            return result

    # Walk up to the top-level message (handling nested types).
    top_class_name = desc.name
    class_nesting: list[str] = []
    wrapper = desc.containing_type
    while wrapper is not None:
        class_nesting.insert(0, top_class_name)
        top_class_name = wrapper.name
        wrapper = wrapper.containing_type

    # Build module path from the .proto file name.
    module_name = desc.file.name
    module_name = module_name.split("/")[-1] if "/" in module_name else module_name
    module_name = module_name.replace(".proto", "_pb2")

    # Build package path from the full proto name.
    temp = desc
    while temp.containing_type is not None:
        temp = temp.containing_type
    package_name = temp.full_name.rsplit(".", 1)[0] if "." in temp.full_name else ""

    try:
        qualified = f"{package_name}.{module_name}" if package_name else module_name
        mod = importlib.import_module(qualified)
        clazz = getattr(mod, top_class_name)
        for nesting in class_nesting:
            clazz = getattr(clazz, nesting)
        return clazz
    except Exception as e:
        raise RuntimeError(f"Couldn't resolve type for {desc.full_name}") from e


# ---------------------------------------------------------------------------
# Field inspection helpers
# ---------------------------------------------------------------------------


def _is_any_field(field: FieldDescriptor) -> bool:
    return field.message_type == any_pb2.DESCRIPTOR.message_types_by_name["Any"]


def _is_map_field(field: FieldDescriptor) -> bool:
    mt = field.message_type
    return (
        field.is_repeated
        and field.type == FieldDescriptor.TYPE_MESSAGE
        and mt is not None
        and mt.has_options
        and mt.GetOptions().map_entry
    )


def _is_src_field_auto_convertible(
    src_field: FieldDescriptor,
    dest_fields_by_name: Mapping[str, FieldDescriptor],
) -> bool:
    """Check whether a source field can be copied to the destination without custom logic."""
    if src_field.name not in dest_fields_by_name:
        return False

    dest_field = dest_fields_by_name[src_field.name]

    if dest_field.is_repeated != src_field.is_repeated or src_field.type != dest_field.type:
        return False

    if _is_map_field(src_field):
        assert src_field.message_type is not None
        assert dest_field.message_type is not None
        src_map = src_field.message_type.fields_by_name
        dest_map = dest_field.message_type.fields_by_name
        return _is_src_field_auto_convertible(
            src_map["key"], dest_map
        ) and _is_src_field_auto_convertible(src_map["value"], dest_map)

    if src_field.type == FieldDescriptor.TYPE_ENUM:
        src_enum = src_field.enum_type
        dest_enum = dest_field.enum_type
        if src_enum == dest_enum:
            return True
        # Different enum types: auto-convertible if every source value number exists in dest.
        if src_enum is not None and dest_enum is not None:
            dest_numbers = {v.number for v in dest_enum.values}
            return all(v.number in dest_numbers for v in src_enum.values)
        return False

    if src_field.type == FieldDescriptor.TYPE_MESSAGE:
        if _is_any_field(src_field) and _is_any_field(dest_field):
            return True
        # Any -> Proto can't be validated statically.
        if _is_any_field(src_field):
            return False
        # Proto -> Any is always valid.
        if _is_any_field(dest_field):
            return True
        if src_field.message_type != dest_field.message_type:
            return False

    return True


def _validate_oneof_multi_mapping(
    src_pb: type[message.Message],
    dest_pb: type[message.Message],
    ignored_fields: list[str],
) -> None:
    """Raise if a oneof in src maps to multiple distinct oneofs/fields in dest."""
    ignored_set = set(ignored_fields)
    dest_field_to_oneof: dict[str, str] = {}
    for oneof in dest_pb.DESCRIPTOR.oneofs_by_name.values():
        for field in oneof.fields:
            dest_field_to_oneof[field.name] = oneof.name
    dest_field_names = set(dest_pb.DESCRIPTOR.fields_by_name.keys())

    for src_oneof_name, src_oneof in src_pb.DESCRIPTOR.oneofs_by_name.items():
        mapped: set[str] = set()
        for field in src_oneof.fields:
            if field.name in ignored_set:
                continue
            if field.name in dest_field_to_oneof:
                mapped.add(dest_field_to_oneof[field.name])
            elif field.name in dest_field_names:
                mapped.add(field.name)
        if len(mapped) > 1:
            raise NotImplementedError(
                f"Oneof field {src_oneof_name} in proto {src_pb.DESCRIPTOR.name} maps to "
                f"more than one field; all fields in the oneof must be explicitly handled "
                f"or ignored."
            )


# ---------------------------------------------------------------------------
# @convert_field decorator
# ---------------------------------------------------------------------------


def convert_field(field_names: list[str] | None = None) -> Callable[[Callable], Callable]:
    """Decorator marking a method as a custom field converter.

    Usage::

        class MyConverter(ProtoConverter[SrcProto, DestProto]):
            @convert_field(["my_field", "other_field"])
            def convert_my_fields(self, src, dest):
                dest.other_field = transform(src.my_field)
    """
    if field_names is None:
        field_names = []

    def decorator(fn: Callable) -> Callable:
        fn.convert_field_names = field_names  # type: ignore[attr-defined]

        @functools.wraps(fn)
        def wrapper(self: Any, src_proto: Any, dest_proto: Any) -> None:
            fn(self, src_proto, dest_proto)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# ProtoConverter
# ---------------------------------------------------------------------------


class ProtoConverter(Generic[F, T]):
    """Converts between two protobuf message types.

    Subclass and parameterize with ``ProtoConverter[FromType, ToType]`` to register
    a converter that will be discovered automatically by :func:`convert` and
    :func:`get_converter`.

    Fields with the same name and compatible type are copied automatically.
    Fields with the same name but different message types are auto-converted
    recursively when a converter for those types exists or can be created.

    Use :data:`IGNORED_FIELDS` to skip fields, and :func:`convert_field` to
    provide custom conversion logic.
    """

    IGNORED_FIELDS: list[str] | None = None

    def __init__(
        self,
        pb_class_from: type[F],
        pb_class_to: type[T],
        field_names_to_ignore: list[str] | None = None,
    ) -> None:
        self._pb_class_from = pb_class_from
        self._pb_class_to = pb_class_to
        self._field_names_to_ignore = list(field_names_to_ignore or [])
        self._function_convert_field_names: list[str] = []
        self._convert_functions: list[Callable[..., None]] = []
        self._unconverted_fields: list[str] = []

        self._validate_fields()

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Extract generic type args from the class definition.
        from typing import get_args
        from typing import get_origin

        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            if get_origin(base) is ProtoConverter:
                pb_class_from, pb_class_to = get_args(base)
                break
        else:
            # Abstract intermediate subclasses are fine; only concrete ones with
            # type parameters get registered.
            return

        if (pb_class_from, pb_class_to) in _registry:
            raise RuntimeError(f"Already have a converter from {pb_class_from} to {pb_class_to}")
        logger.debug(
            "Registering %s as converter from %s to %s",
            cls.__name__,
            pb_class_from,
            pb_class_to,
        )
        _registry[(pb_class_from, pb_class_to)] = cls(
            pb_class_from, pb_class_to, cls.IGNORED_FIELDS
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_fields(self) -> None:
        """Ensure every source field is either auto-convertible, explicitly handled, or ignored."""
        for entry in dir(self.__class__):
            obj = getattr(self.__class__, entry)
            if callable(obj) and hasattr(obj, "convert_field_names"):
                self._convert_functions.append(obj)  # pyright: ignore[reportArgumentType]
                self._function_convert_field_names.extend(
                    obj.convert_field_names  # pyright: ignore[reportFunctionMemberAccess]
                )

        # Protobuf stubs union two FieldDescriptor implementations; cast to the public one.
        src_fields: Sequence[FieldDescriptor] = self._pb_class_from.DESCRIPTOR.fields  # type: ignore[assignment]
        dest_by_name: Mapping[str, FieldDescriptor] = self._pb_class_to.DESCRIPTOR.fields_by_name  # type: ignore[assignment]

        self._unconverted_fields = self._register_recursive_converters(
            src_fields,
            dest_by_name,
            self._field_names_to_ignore,
        )

        if self._pb_class_from.DESCRIPTOR.oneofs:
            _validate_oneof_multi_mapping(
                self._pb_class_from, self._pb_class_to, self._field_names_to_ignore
            )
        if self._pb_class_to.DESCRIPTOR.oneofs:
            _validate_oneof_multi_mapping(
                self._pb_class_to, self._pb_class_from, self._field_names_to_ignore
            )

        unconverted = set(self._unconverted_fields) - set(self._function_convert_field_names)
        if unconverted:
            src_names = {f.name for f in src_fields}
            dest_names = set(dest_by_name.keys())
            raise NotImplementedError(
                "Fields can't be automatically converted; must either be explicitly handled "
                f"or ignored. Converting from {self._pb_class_from} to {self._pb_class_to}. "
                f"Unhandled fields: {unconverted}.\n\n"
                f"Source has fields: {src_names}.\nDestination has fields: {dest_names}."
            )

    def _register_recursive_converters(
        self,
        src_fields: Sequence[FieldDescriptor],
        dest_fields_by_name: Mapping[str, FieldDescriptor],
        ignored: list[str],
    ) -> list[str]:
        """Register converters for nested message fields and return unhandled field names.

        For each source field that can't be auto-converted, tries to find or create a
        converter for the nested message types. Successfully resolved fields are added
        to ``self._convert_functions`` and ``self._field_names_to_ignore``; the rest are
        returned as unhandled.
        """
        unhandled: list[str] = []
        for field in src_fields:
            if field.name in ignored:
                continue

            if _is_src_field_auto_convertible(field, dest_fields_by_name):
                continue

            if field.name not in dest_fields_by_name:
                unhandled.append(field.name)
                continue

            dest_field = dest_fields_by_name[field.name]

            # Map<K, SrcMsg> -> Map<K, DestMsg> via recursive converter.
            if _is_map_field(field) and _is_map_field(dest_field):
                assert field.message_type is not None
                assert dest_field.message_type is not None
                src_map = field.message_type.fields_by_name
                dest_map = dest_field.message_type.fields_by_name
                if _is_src_field_auto_convertible(src_map["key"], dest_map):
                    if _is_src_field_auto_convertible(src_map["value"], dest_map):
                        continue
                    assert src_map["value"].message_type is not None
                    assert dest_map["value"].message_type is not None
                    value_conv = get_converter(
                        _descriptor_to_type(src_map["value"].message_type),
                        _descriptor_to_type(dest_map["value"].message_type),
                    )
                    if value_conv:
                        self._function_convert_field_names.append(field.name)
                        self._field_names_to_ignore.append(field.name)
                        self._convert_functions.append(_make_map_converter(field, value_conv))
                        continue

            # SrcMsg -> DestMsg (singular or repeated) via recursive converter.
            elif (
                field.type == FieldDescriptor.TYPE_MESSAGE
                and dest_field.type == FieldDescriptor.TYPE_MESSAGE
            ):
                assert field.message_type is not None
                assert dest_field.message_type is not None
                field_conv = get_converter(
                    _descriptor_to_type(field.message_type),
                    _descriptor_to_type(dest_field.message_type),
                )
                if field_conv:
                    self._function_convert_field_names.append(field.name)
                    self._field_names_to_ignore.append(field.name)
                    self._convert_functions.append(_make_field_converter(field, field_conv))
                    continue

            unhandled.append(field.name)

        return unhandled

    # ------------------------------------------------------------------
    # Conversion
    # ------------------------------------------------------------------

    def convert(self, src: F) -> T:
        """Convert *src* to an instance of the destination type."""
        src_type = src.DESCRIPTOR.full_name
        expected = self._pb_class_from.DESCRIPTOR.full_name
        if src_type != expected:
            raise TypeError(f"Provided src type [{src_type}] doesn't match expected [{expected}].")

        dest = self._pb_class_to()
        self._auto_convert(src, dest)
        for fn in self._convert_functions:
            fn(self, src, dest)
        return dest

    def _auto_convert(self, src: message.Message, dest: message.Message) -> None:
        """Copy all auto-convertible fields from *src* to *dest*.

        Uses ``ListFields()`` which only yields fields with non-default values.
        This means proto3 fields explicitly set to their default (e.g. ``number = 0``)
        won't be copied — standard proto3 semantics, but worth noting for anyone
        migrating from a hand-written converter that does ``dest.x = src.x``.
        """
        for _src_fd, src_value in src.ListFields():
            # Protobuf stubs union two FieldDescriptor implementations; cast to the public one.
            src_fd: FieldDescriptor = _src_fd  # type: ignore[assignment]
            if (
                src_fd.name in self._field_names_to_ignore
                or src_fd.name in self._unconverted_fields
            ):
                continue

            dest_fd: FieldDescriptor = dest.DESCRIPTOR.fields_by_name[src_fd.name]  # type: ignore[assignment]
            dest_value = getattr(dest, src_fd.name)

            # Map fields
            if _is_map_field(src_fd):
                assert src_fd.message_type is not None
                assert dest_fd.message_type is not None
                src_map_value_fd = src_fd.message_type.fields_by_name["value"]
                dest_map_value_fd = dest_fd.message_type.fields_by_name["value"]
                if _is_any_field(dest_map_value_fd) and not _is_any_field(src_map_value_fd):
                    for key, value in src_value.items():
                        dest_value[key].Pack(value)
                else:
                    dest_value.MergeFrom(src_value)

            # Repeated fields
            elif src_fd.is_repeated:
                if _is_any_field(src_fd):
                    factory = symbol_database.Default()
                    for item in src_value:
                        type_name = item.TypeName()
                        proto_desc = factory.pool.FindMessageTypeByName(type_name)  # pyright: ignore[reportAttributeAccessIssue]
                        proto_class = factory.GetPrototype(proto_desc)  # pyright: ignore[reportAttributeAccessIssue]
                        proto_obj = proto_class()
                        item.Unpack(proto_obj)
                        dest_value.add().Pack(proto_obj)
                elif _is_any_field(dest_fd):
                    for item in src_value:
                        any_proto = any_pb2.Any()
                        any_proto.Pack(item)
                        dest_value.append(any_proto)
                else:
                    dest_value.MergeFrom(src_value)

            # Singular message fields
            elif src_fd.type == FieldDescriptor.TYPE_MESSAGE:
                if _is_any_field(dest_fd) and not _is_any_field(src_fd):
                    dest_value.Pack(src_value)
                else:
                    dest_value.CopyFrom(src_value)

            # Scalars
            else:
                setattr(dest, src_fd.name, src_value)


# ---------------------------------------------------------------------------
# Synthetic converter helpers for recursive nested conversion
# ---------------------------------------------------------------------------


def _make_map_converter(
    field: FieldDescriptor,
    value_converter: ProtoConverter[Any, Any],
) -> Callable[..., None]:
    def convert_map(
        _self: ProtoConverter[Any, Any], src: message.Message, dest: message.Message
    ) -> None:
        for k, v in getattr(src, field.name).items():
            getattr(dest, field.name)[k].CopyFrom(value_converter.convert(v))

    return convert_map


def _make_field_converter(
    field: FieldDescriptor,
    field_converter: ProtoConverter[Any, Any],
) -> Callable[..., None]:
    def convert_field(
        _self: ProtoConverter[Any, Any], src: message.Message, dest: message.Message
    ) -> None:
        if field.is_repeated:
            src_value = getattr(src, field.name)
            dest_value = getattr(dest, field.name)
            dest_value.extend(field_converter.convert(v) for v in src_value)
        elif src.HasField(field.name):
            getattr(dest, field.name).CopyFrom(field_converter.convert(getattr(src, field.name)))

    return convert_field


# ---------------------------------------------------------------------------
# Public module-level API
# ---------------------------------------------------------------------------


def get_converter(pb_class_from: type[F], pb_class_to: type[T]) -> ProtoConverter[F, T]:
    """Look up or create a converter between two proto types."""
    if (pb_class_from, pb_class_to) not in _registry:
        _registry[(pb_class_from, pb_class_to)] = ProtoConverter(
            pb_class_from=pb_class_from, pb_class_to=pb_class_to
        )
    return _registry[(pb_class_from, pb_class_to)]


def convert(src: message.Message, to_type: type[T]) -> T:
    """Convert a protobuf message to a different type.

    This is the primary API. It looks up (or auto-creates) the appropriate
    converter and invokes it::

        api_msg = proto_converter.convert(internal_msg, api_pb2.MyMessage)
    """
    return get_converter(type(src), to_type).convert(src)
