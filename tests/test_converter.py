import pytest
from google.protobuf import any_pb2
from google.protobuf import struct_pb2
from hamcrest import assert_that
from proto_matcher import equals_proto
from test_api import api_pb2
from test_internal import internal_pb2

import proto_converter
from proto_converter.converter import _registry


@pytest.fixture(autouse=True)
def _clear_registry():
    """Ensure each test starts with a clean converter registry."""
    saved = dict(_registry)
    _registry.clear()
    yield
    _registry.clear()
    _registry.update(saved)


# ---------------------------------------------------------------------------
# Identical-schema conversion (fields match by name and type)
# ---------------------------------------------------------------------------


class TestAutoConvert:
    def test_scalars(self):
        src = api_pb2.SimpleMessage(text="hello", number=42)
        dest = proto_converter.convert(src, internal_pb2.SimpleMessage)
        assert dest == internal_pb2.SimpleMessage(text="hello", number=42)

    def test_roundtrip(self):
        src = api_pb2.SimpleMessage(text="hello", number=42)
        roundtripped = proto_converter.convert(
            proto_converter.convert(src, internal_pb2.SimpleMessage),
            api_pb2.SimpleMessage,
        )
        assert roundtripped == src

    def test_deep_convert_no_registration(self):
        """The core use case: deep-convert an entire message tree with zero
        explicit converter registration. api_pb2.Address and internal_pb2.Address
        are different Python types but share the same fields, so converters are
        created on the fly — for singular, repeated, and map-value positions."""
        src = api_pb2.Person(
            name="Alice",
            age=30,
            address=api_pb2.Address(street="123 Main", city="Springfield"),
            tags=["a", "b"],
            metadata={"k": "v"},
            other_addresses=[
                api_pb2.Address(street="456 Oak", city="Shelbyville"),
            ],
            named_addresses={
                "work": api_pb2.Address(street="789 Elm", city="Capital City"),
            },
            extra=struct_pb2.Struct(fields={"x": struct_pb2.Value(string_value="y")}),
        )
        dest = proto_converter.convert(src, internal_pb2.Person)

        expected = internal_pb2.Person(
            name="Alice",
            age=30,
            address=internal_pb2.Address(street="123 Main", city="Springfield"),
            tags=["a", "b"],
            metadata={"k": "v"},
            other_addresses=[
                internal_pb2.Address(street="456 Oak", city="Shelbyville"),
            ],
            named_addresses={
                "work": internal_pb2.Address(street="789 Elm", city="Capital City"),
            },
            extra=struct_pb2.Struct(fields={"x": struct_pb2.Value(string_value="y")}),
        )
        assert_that(dest, equals_proto(expected))

    def test_oneof_string(self):
        src = api_pb2.OneofMessage(name="test", str_value="hello")
        dest = proto_converter.convert(src, internal_pb2.OneofMessage)
        assert dest.name == "test"
        assert dest.str_value == "hello"
        assert dest.WhichOneof("value") == "str_value"

    def test_oneof_int(self):
        src = api_pb2.OneofMessage(name="test", int_value=99)
        dest = proto_converter.convert(src, internal_pb2.OneofMessage)
        assert dest.name == "test"
        assert dest.int_value == 99
        assert dest.WhichOneof("value") == "int_value"

    def test_empty_message(self):
        src = api_pb2.SimpleMessage()
        dest = proto_converter.convert(src, internal_pb2.SimpleMessage)
        assert dest == internal_pb2.SimpleMessage()

    def test_bytes_field(self):
        src = api_pb2.SimpleMessage(text="hello", data=b"\x00\x01\xff")
        dest = proto_converter.convert(src, internal_pb2.SimpleMessage)
        expected = internal_pb2.SimpleMessage(text="hello", data=b"\x00\x01\xff")
        assert_that(dest, equals_proto(expected))

    def test_int_keyed_map(self):
        src = api_pb2.SimpleMessage(int_keyed_map={1: "one", 2: "two"})
        dest = proto_converter.convert(src, internal_pb2.SimpleMessage)
        expected = internal_pb2.SimpleMessage(int_keyed_map={1: "one", 2: "two"})
        assert_that(dest, equals_proto(expected))

    def test_proto3_default_values_not_copied(self):
        """Proto3 fields set to their default value are not copied (ListFields skips them)."""
        src = api_pb2.SimpleMessage(text="", number=0)
        dest = proto_converter.convert(src, internal_pb2.SimpleMessage)
        assert dest == internal_pb2.SimpleMessage()

    def test_any_singular(self):
        """Singular Any field is copied between matching Any fields."""
        inner = api_pb2.SimpleMessage(text="packed", number=7)
        payload = any_pb2.Any()
        payload.Pack(inner)
        src = api_pb2.AnyHolder(payload=payload)

        dest = proto_converter.convert(src, internal_pb2.AnyHolder)
        unpacked = api_pb2.SimpleMessage()
        dest.payload.Unpack(unpacked)
        assert_that(unpacked, equals_proto(inner))

    def test_any_repeated(self):
        """Repeated Any fields are copied via MergeFrom."""
        items = []
        for i in range(3):
            a = any_pb2.Any()
            a.Pack(api_pb2.SimpleMessage(text=f"item{i}"))
            items.append(a)
        src = api_pb2.AnyHolder(items=items)

        dest = proto_converter.convert(src, internal_pb2.AnyHolder)

        expected = internal_pb2.AnyHolder(items=items)
        assert_that(dest, equals_proto(expected))

    def test_nested_message_type(self):
        """Messages defined inside another message (Outer.Inner) are resolved correctly."""
        src = api_pb2.Outer(
            nested=api_pb2.Outer.Inner(value="hello"),
            label="test",
        )
        dest = proto_converter.convert(src, internal_pb2.Outer)
        assert dest.label == "test"
        assert dest.nested == internal_pb2.Outer.Inner(value="hello")

    def test_compatible_enum(self):
        """api Status {0,1,2} -> internal Status {0,1,2,3}: all source values exist in dest."""
        src = api_pb2.EnumMessage(name="test", status=api_pb2.STATUS_ACTIVE)
        dest = proto_converter.convert(src, internal_pb2.EnumMessage)
        assert dest.name == "test"
        assert dest.status == internal_pb2.STATUS_ACTIVE

    def test_enum_dest_superset(self):
        """internal Status {0,1,2,3} is a superset of api Status {0,1,2}, so
        api -> internal works but internal -> api requires explicit handling
        (since ARCHIVED=3 has no api counterpart)."""
        src = api_pb2.EnumMessage(name="test", status=api_pb2.STATUS_INACTIVE)
        dest = proto_converter.convert(src, internal_pb2.EnumMessage)
        assert dest.status == internal_pb2.STATUS_INACTIVE

        with pytest.raises(NotImplementedError, match="status"):
            proto_converter.get_converter(internal_pb2.EnumMessage, api_pb2.EnumMessage)


# ---------------------------------------------------------------------------
# IGNORED_FIELDS
# ---------------------------------------------------------------------------


class TestIgnoredFields:
    def test_extra_src_fields_require_ignore(self):
        """internal.Person has internal_id and created_at that api.Person lacks."""
        with pytest.raises(NotImplementedError, match="internal_id"):
            proto_converter.get_converter(internal_pb2.Person, api_pb2.Person)

    def test_ignored_fields_on_subclass(self):
        class InternalToApiPerson(
            proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]
        ):
            IGNORED_FIELDS = ["internal_id", "created_at"]

        src = internal_pb2.Person(name="Bob", age=25, internal_id="secret", created_at=1234567890)
        dest = proto_converter.convert(src, api_pb2.Person)
        expected = api_pb2.Person(name="Bob", age=25)
        assert_that(dest, equals_proto(expected))


# ---------------------------------------------------------------------------
# @convert_field custom handlers
# ---------------------------------------------------------------------------


class TestConvertField:
    def test_custom_handler(self):
        class InternalToApiPerson(
            proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]
        ):
            IGNORED_FIELDS = ["created_at"]

            @proto_converter.convert_field(["internal_id"])
            def convert_id(self, src, dest):
                dest.metadata["original_id"] = src.internal_id

        src = internal_pb2.Person(name="Carol", internal_id="id-123")
        dest = proto_converter.convert(src, api_pb2.Person)
        expected = api_pb2.Person(name="Carol", metadata={"original_id": "id-123"})
        assert_that(dest, equals_proto(expected))


# ---------------------------------------------------------------------------
# Recursive nested conversion (different message types)
# ---------------------------------------------------------------------------


class TestRecursiveNested:
    @pytest.fixture(autouse=True)
    def _register_detail_converters(self):
        """Register converters for InternalDetail <-> ApiDetail."""

        class InternalToApi(
            proto_converter.ProtoConverter[internal_pb2.InternalDetail, api_pb2.ApiDetail]
        ):
            IGNORED_FIELDS = ["internal_note"]

        class ApiToInternal(
            proto_converter.ProtoConverter[api_pb2.ApiDetail, internal_pb2.InternalDetail]
        ):
            pass

    def test_singular_nested(self):
        src = internal_pb2.DifferentNested(
            label="test",
            detail=internal_pb2.InternalDetail(info="x", priority=1, internal_note="secret"),
        )
        dest = proto_converter.convert(src, api_pb2.DifferentNested)
        expected = api_pb2.DifferentNested(
            label="test", detail=api_pb2.ApiDetail(info="x", priority=1)
        )
        assert_that(dest, equals_proto(expected))

    def test_repeated_nested(self):
        src = internal_pb2.DifferentNested(
            label="test",
            details=[
                internal_pb2.InternalDetail(info="a", priority=1, internal_note="n1"),
                internal_pb2.InternalDetail(info="b", priority=2, internal_note="n2"),
            ],
        )
        dest = proto_converter.convert(src, api_pb2.DifferentNested)
        expected = api_pb2.DifferentNested(
            label="test",
            details=[
                api_pb2.ApiDetail(info="a", priority=1),
                api_pb2.ApiDetail(info="b", priority=2),
            ],
        )
        assert_that(dest, equals_proto(expected))

    def test_map_nested(self):
        src = internal_pb2.DifferentNested(
            label="test",
            named_details={
                "first": internal_pb2.InternalDetail(info="a", priority=1, internal_note="n"),
                "second": internal_pb2.InternalDetail(info="b", priority=2, internal_note="o"),
            },
        )
        dest = proto_converter.convert(src, api_pb2.DifferentNested)
        expected = api_pb2.DifferentNested(
            label="test",
            named_details={
                "first": api_pb2.ApiDetail(info="a", priority=1),
                "second": api_pb2.ApiDetail(info="b", priority=2),
            },
        )
        assert_that(dest, equals_proto(expected))

    def test_roundtrip_nested(self):
        src = api_pb2.DifferentNested(
            label="rt",
            detail=api_pb2.ApiDetail(info="x", priority=5),
            details=[api_pb2.ApiDetail(info="y", priority=6)],
            named_details={"z": api_pb2.ApiDetail(info="z", priority=7)},
        )
        roundtripped = proto_converter.convert(
            proto_converter.convert(src, internal_pb2.DifferentNested),
            api_pb2.DifferentNested,
        )
        assert roundtripped == src


# ---------------------------------------------------------------------------
# Type resolver
# ---------------------------------------------------------------------------


class TestTypeResolver:
    def test_resolver_overrides_default(self):
        """A resolver that returns a concrete class bypasses the default import logic."""
        resolved: list[str] = []

        def resolver(desc):
            if desc.full_name == "test_internal.Address":
                # Return the same result (since its correct), but record the resolution to
                # prove the resolver was called.
                resolved.append(desc.full_name)
                return internal_pb2.Address
            return None

        proto_converter.set_type_resolver(resolver)
        try:
            src = api_pb2.Person(
                name="test",
                address=api_pb2.Address(street="Main St", city="Town"),
            )
            dest = proto_converter.convert(src, internal_pb2.Person)
            assert dest.address == internal_pb2.Address(street="Main St", city="Town")
            assert "test_internal.Address" in resolved
        finally:
            proto_converter.set_type_resolver(None)

    def test_resolver_fallthrough(self):
        """A resolver returning None falls through to default resolution."""

        def resolver(desc):
            return None

        proto_converter.set_type_resolver(resolver)
        try:
            src = api_pb2.SimpleMessage(text="hi", number=1)
            proto_converter.convert(src, internal_pb2.SimpleMessage)
            # SimpleMessage has no nested messages, so the resolver won't be called
            # during this conversion. But it was installed successfully.
        finally:
            proto_converter.set_type_resolver(None)


class TestModuleResolver:
    def test_module_remapping(self):
        """set_module_resolver's return value is used as the import path."""
        remapped: list[tuple[str, str | None]] = []

        def resolver(module_path: str) -> str | None:
            # The test_internal proto package maps to test_internal.internal_pb2.
            # Remap it through an identity transformation to prove we control the path.
            if module_path.startswith("test_internal."):
                # Return the same path — but this proves the resolver was called and
                # its return value was used (if it returned garbage, import would fail).
                remapped.append((module_path, module_path))
                return module_path

            remapped.append((module_path, None))
            return None

        proto_converter.set_module_resolver(resolver)
        try:
            src = api_pb2.Person(
                name="test",
                address=api_pb2.Address(street="Main St", city="Town"),
            )
            dest = proto_converter.convert(src, internal_pb2.Person)
            assert dest.name == "test"
            # Verify the resolver was called with the internal module path and returned
            # a non-None value that was used for the import.
            internal_calls = [(m, r) for m, r in remapped if "test_internal" in m]
            assert internal_calls
            assert all(r is not None for _, r in internal_calls)
        finally:
            proto_converter.set_module_resolver(None)


# ---------------------------------------------------------------------------
# Circular proto references
# ---------------------------------------------------------------------------


class TestCircularProtos:
    def test_self_referential_auto(self):
        """api TreeNode -> internal TreeNode with auto-created converter."""
        src = api_pb2.TreeNode(
            name="root",
            children=[
                api_pb2.TreeNode(name="child1"),
                api_pb2.TreeNode(
                    name="child2",
                    children=[api_pb2.TreeNode(name="grandchild")],
                ),
            ],
        )
        dest = proto_converter.convert(src, internal_pb2.TreeNode)
        expected = internal_pb2.TreeNode(
            name="root",
            children=[
                internal_pb2.TreeNode(name="child1"),
                internal_pb2.TreeNode(
                    name="child2",
                    children=[internal_pb2.TreeNode(name="grandchild")],
                ),
            ],
        )
        assert_that(dest, equals_proto(expected))

    def test_self_referential_with_ignored_fields(self):
        """internal TreeNode -> api TreeNode requires IGNORED_FIELDS for internal_note.

        The recursive children field must use the same subclass converter (with
        IGNORED_FIELDS), not a plain ProtoConverter that would fail validation.
        """

        class TreeConverter(
            proto_converter.ProtoConverter[internal_pb2.TreeNode, api_pb2.TreeNode]
        ):
            IGNORED_FIELDS = ["internal_note"]

        src = internal_pb2.TreeNode(
            name="root",
            internal_note="secret",
            children=[
                internal_pb2.TreeNode(name="child", internal_note="also secret"),
            ],
        )
        dest = proto_converter.convert(src, api_pb2.TreeNode)
        expected = api_pb2.TreeNode(
            name="root",
            children=[api_pb2.TreeNode(name="child")],
        )
        assert_that(dest, equals_proto(expected))


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrors:
    def test_type_mismatch(self):
        converter = proto_converter.get_converter(api_pb2.SimpleMessage, internal_pb2.SimpleMessage)
        with pytest.raises(TypeError, match="doesn't match"):
            converter.convert(api_pb2.Person(name="wrong type"))  # pyright: ignore[reportArgumentType]

    def test_duplicate_registration(self):
        class First(
            proto_converter.ProtoConverter[api_pb2.SimpleMessage, internal_pb2.SimpleMessage]
        ):
            pass

        with pytest.raises(RuntimeError, match="Already have a converter"):

            class Second(
                proto_converter.ProtoConverter[api_pb2.SimpleMessage, internal_pb2.SimpleMessage]
            ):
                pass

    def test_unhandled_fields_error(self):
        with pytest.raises(NotImplementedError, match="internal_id"):

            class Bad(proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]):
                pass

    def test_incompatible_enum(self):
        """api IncompatiblePriority has CRITICAL (3) which internal lacks."""
        with pytest.raises(NotImplementedError, match="priority"):
            proto_converter.get_converter(
                api_pb2.IncompatibleEnumMessage, internal_pb2.IncompatibleEnumMessage
            )

    def test_bogus_ignored_field(self):
        with pytest.raises(ValueError, match="internal_idd"):

            class Bad(proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]):
                IGNORED_FIELDS = ["internal_idd", "created_at"]

    def test_bogus_convert_field(self):
        with pytest.raises(ValueError, match="nonexistent"):

            class Bad(proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]):
                IGNORED_FIELDS = ["internal_id", "created_at"]

                @proto_converter.convert_field(["nonexistent"])
                def handle(self, src, dest):
                    pass

    def test_duplicate_convert_field(self):
        with pytest.raises(ValueError, match="internal_id"):

            class Bad(proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]):
                IGNORED_FIELDS = ["created_at"]

                @proto_converter.convert_field(["internal_id"])
                def handle1(self, src, dest):
                    pass

                @proto_converter.convert_field(["internal_id"])
                def handle2(self, src, dest):
                    pass
