import pytest
from google.protobuf import struct_pb2
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

        assert dest.name == "Alice"
        assert dest.age == 30
        assert dest.address == internal_pb2.Address(street="123 Main", city="Springfield")
        assert list(dest.tags) == ["a", "b"]
        assert dict(dest.metadata) == {"k": "v"}
        assert list(dest.other_addresses) == [
            internal_pb2.Address(street="456 Oak", city="Shelbyville"),
        ]
        assert dict(dest.named_addresses) == {
            "work": internal_pb2.Address(street="789 Elm", city="Capital City"),
        }
        assert dest.extra == struct_pb2.Struct(fields={"x": struct_pb2.Value(string_value="y")})

    def test_oneof_string(self):
        src = api_pb2.OneofMessage(name="test", str_value="hello")
        dest = proto_converter.convert(src, internal_pb2.OneofMessage)
        assert dest.name == "test"
        assert dest.str_value == "hello"
        assert dest.WhichOneof("value") == "str_value"

    def test_oneof_int(self):
        src = api_pb2.OneofMessage(name="test", int_value=99)
        dest = proto_converter.convert(src, internal_pb2.OneofMessage)
        assert dest.int_value == 99
        assert dest.WhichOneof("value") == "int_value"

    def test_empty_message(self):
        src = api_pb2.SimpleMessage()
        dest = proto_converter.convert(src, internal_pb2.SimpleMessage)
        assert dest == internal_pb2.SimpleMessage()


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
        assert dest.name == "Bob"
        assert dest.age == 25


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
        assert dest.name == "Carol"
        assert dest.metadata["original_id"] == "id-123"


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
        assert dest.label == "test"
        assert dest.detail == api_pb2.ApiDetail(info="x", priority=1)

    def test_repeated_nested(self):
        src = internal_pb2.DifferentNested(
            label="test",
            details=[
                internal_pb2.InternalDetail(info="a", priority=1, internal_note="n1"),
                internal_pb2.InternalDetail(info="b", priority=2, internal_note="n2"),
            ],
        )
        dest = proto_converter.convert(src, api_pb2.DifferentNested)
        assert list(dest.details) == [
            api_pb2.ApiDetail(info="a", priority=1),
            api_pb2.ApiDetail(info="b", priority=2),
        ]

    def test_map_nested(self):
        src = internal_pb2.DifferentNested(
            label="test",
            named_details={
                "first": internal_pb2.InternalDetail(info="a", priority=1, internal_note="n"),
            },
        )
        dest = proto_converter.convert(src, api_pb2.DifferentNested)
        assert dict(dest.named_details) == {
            "first": api_pb2.ApiDetail(info="a", priority=1),
        }

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
    def test_custom_resolver(self):
        calls = []

        def resolver(desc):
            calls.append(desc.full_name)
            return None  # fall through to default

        proto_converter.set_type_resolver(resolver)
        try:
            src = api_pb2.SimpleMessage(text="hi", number=1)
            proto_converter.convert(src, internal_pb2.SimpleMessage)
        finally:
            proto_converter.set_type_resolver(None)


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
