"""Automatic conversion between compatible protocol buffer types.

Converts fields with matching names and compatible types automatically. Fields that
can't be auto-converted must be handled explicitly via @convert_field or ignored via
IGNORED_FIELDS, otherwise construction raises NotImplementedError.

Nested message fields with different types are auto-converted if a converter exists
(or can be trivially created) for those types.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any, Generic, TypeVar, get_args, get_origin

from google.protobuf import any_pb2
from google.protobuf import descriptor as descriptor_mod
from google.protobuf import message

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=message.Message)
T = TypeVar("T", bound=message.Message)

FieldDescriptor = descriptor_mod.FieldDescriptor

_registry: dict[tuple[type[message.Message], type[message.Message]], ProtoConverter[Any, Any]] = {}

# User-installable hooks for type resolution.
_type_resolver: Callable[[descriptor_mod.Descriptor], type[message.Message] | None] | None = None
_module_resolver: Callable[[str], str | None] | None = None


class _BuildingSentinel:
    """Singleton inserted into _registry during construction to detect re-entrancy."""


_BUILDING = _BuildingSentinel()


def get_converter(pb_class_from: type[F], pb_class_to: type[T]) -> ProtoConverter[F, T]:
    """Look up or create a converter between two proto types."""
    key = (pb_class_from, pb_class_to)
    existing = _registry.get(key)
    if existing is not None:
        if isinstance(existing, _BuildingSentinel):
            # We're in the middle of building this converter (circular proto reference).
            # Return a deferred proxy that will resolve at convert-time.
            return _DeferredConverter(pb_class_from, pb_class_to)  # type: ignore[return-value]
        return existing
    # Insert sentinel before construction to break circular references.
    _registry[key] = _BUILDING  # type: ignore[assignment]
    try:
        converter = ProtoConverter(pb_class_from=pb_class_from, pb_class_to=pb_class_to)
    except Exception:
        _registry.pop(key, None)
        raise
    _registry[key] = converter
    return converter


def convert(src: message.Message, to_type: type[T]) -> T:
    """Convert a protobuf message to a different type.

    This is the primary API. It looks up (or auto-creates) the appropriate
    converter and invokes it::

        api_msg = proto_converter.convert(internal_msg, api_pb2.MyMessage)
    """
    return get_converter(type(src), to_type).convert(src)


def set_type_resolver(
    resolver: Callable[[descriptor_mod.Descriptor], type[message.Message] | None] | None,
) -> None:
    """Install a custom type resolver for mapping protobuf Descriptors to Python classes.

    The resolver is called with a ``google.protobuf.descriptor.Descriptor`` and should
    return the corresponding Python message class, or ``None`` to fall through to the
    default import-based resolution.

    This is the general escape hatch. For the common case of remapping package prefixes,
    see :func:`set_module_resolver`.

    Pass ``None`` to remove a previously installed resolver.
    """
    global _type_resolver
    _type_resolver = resolver


def set_module_resolver(
    resolver: Callable[[str], str | None] | None,
) -> None:
    """Install a hook that remaps the Python module path before import.

    The resolver receives the fully-qualified module path (e.g.
    ``"mycompany.v1.messages_pb2"``) and should return a replacement path, or ``None``
    to use the original. This is the easy way to handle projects where the proto package
    doesn't match the Python package::

        def resolver(module_path: str) -> str | None:
            if module_path.startswith("ultravox."):
                return f"ultravox_proto.{module_path}"
            return None

        proto_converter.set_module_resolver(resolver)

    Pass ``None`` to remove a previously installed resolver.
    """
    global _module_resolver
    _module_resolver = resolver


# ---------------------------------------------------------------------------
# @convert_field decorator
# ---------------------------------------------------------------------------


def convert_field(field_names: list[str] | None = None) -> Callable[[Callable], Callable]:
    """Decorator marking a method as a custom field converter.

    The handler runs after auto-conversion, so for fields that are already
    auto-convertible (same name and type in both protos), the auto-converted value
    will be overwritten by the handler.

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
        return fn

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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self._pb_class_from.__name__} -> {self._pb_class_to.__name__})"
        )

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Extract generic type args from the class definition.
        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            if get_origin(base) is ProtoConverter:
                pb_class_from, pb_class_to = get_args(base)
                break
        else:
            # Abstract intermediate subclasses are fine; only concrete ones with
            # type parameters get registered.
            return

        key = (pb_class_from, pb_class_to)
        if key in _registry:
            raise RuntimeError(f"Already have a converter from {pb_class_from} to {pb_class_to}")
        logger.debug(
            "Registering %s as converter from %s to %s",
            cls.__name__,
            pb_class_from,
            pb_class_to,
        )
        # Insert sentinel before construction so recursive get_converter calls
        # (from circular proto references) return a _DeferredConverter instead
        # of creating a plain ProtoConverter that lacks this subclass's
        # IGNORED_FIELDS / @convert_field methods.
        _registry[key] = _BUILDING  # type: ignore[assignment]
        try:
            _registry[key] = cls(pb_class_from, pb_class_to, cls.IGNORED_FIELDS)
        except Exception:
            _registry.pop(key, None)
            raise

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

        # Validate that IGNORED_FIELDS and @convert_field names reference real source fields.
        # Check this before the unconverted-fields error so a typo in IGNORED_FIELDS
        # surfaces as "bogus field name" rather than the confusing "field X is unhandled."
        src_field_names = {f.name for f in src_fields}
        bogus_ignored = set(self._field_names_to_ignore) - src_field_names
        if bogus_ignored:
            raise ValueError(
                f"IGNORED_FIELDS contains names not present in source "
                f"{self._pb_class_from}: {bogus_ignored}"
            )
        bogus_handled = set(self._function_convert_field_names) - src_field_names
        if bogus_handled:
            raise ValueError(
                f"@convert_field references names not present in source "
                f"{self._pb_class_from}: {bogus_handled}"
            )

        unconverted = set(self._unconverted_fields) - set(self._function_convert_field_names)
        if unconverted:
            dest_names = set(dest_by_name.keys())
            raise NotImplementedError(
                "Fields can't be automatically converted; must either be explicitly handled "
                f"or ignored. Converting from {self._pb_class_from} to {self._pb_class_to}. "
                f"Unhandled fields: {unconverted}.\n\n"
                f"Source has fields: {src_field_names}.\nDestination has fields: {dest_names}."
            )

        # Freeze to sets now that mutation is done — used for O(1) lookups in _auto_convert.
        self._skip_fields = set(self._field_names_to_ignore) | set(self._unconverted_fields)

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
                if (
                    _is_src_field_auto_convertible(src_map["key"], dest_map)
                    and src_map["value"].type == FieldDescriptor.TYPE_MESSAGE
                    and src_map["value"].message_type is not None
                    and dest_map["value"].message_type is not None
                ):
                    try:
                        value_conv = get_converter(
                            _descriptor_to_type(src_map["value"].message_type),
                            _descriptor_to_type(dest_map["value"].message_type),
                        )
                    except NotImplementedError:
                        pass
                    else:
                        self._function_convert_field_names.append(field.name)
                        self._field_names_to_ignore.append(field.name)
                        self._convert_functions.append(_make_map_converter(field, value_conv))
                        continue

            # SrcMsg -> DestMsg (singular or repeated) via recursive converter.
            elif (
                field.type == FieldDescriptor.TYPE_MESSAGE
                and dest_field.type == FieldDescriptor.TYPE_MESSAGE
                and field.message_type is not None
                and dest_field.message_type is not None
            ):
                try:
                    field_conv = get_converter(
                        _descriptor_to_type(field.message_type),
                        _descriptor_to_type(dest_field.message_type),
                    )
                except NotImplementedError:
                    pass
                else:
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
            if src_fd.name in self._skip_fields:
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
                if _is_any_field(dest_fd) and not _is_any_field(src_fd):
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
        if _module_resolver is not None:
            qualified = _module_resolver(qualified) or qualified
        mod = importlib.import_module(qualified)
        clazz = getattr(mod, top_class_name)
        for nesting in class_nesting:
            clazz = getattr(clazz, nesting)
        return clazz
    except Exception as e:
        raise RuntimeError(f"Couldn't resolve type for {desc.full_name}") from e


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
        # Comparison is by number (matching proto wire format), not by name.
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


class _DeferredConverter(Generic[F, T]):
    """Lazy proxy returned when a converter is requested during its own construction.

    This breaks circular references (e.g. ``TreeNode`` containing ``repeated TreeNode``).
    The real converter will be in the registry by the time ``convert()`` is called.
    """

    def __init__(self, pb_class_from: type[F], pb_class_to: type[T]) -> None:
        self._key = (pb_class_from, pb_class_to)
        self._resolved: ProtoConverter[F, T] | None = None

    def convert(self, src: F) -> T:
        if self._resolved is None:
            real = _registry.get(self._key)
            if not isinstance(real, ProtoConverter):
                raise RuntimeError(
                    f"Circular converter for {self._key} was never fully constructed"
                )
            self._resolved = real
        return self._resolved.convert(src)
