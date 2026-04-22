import pytest
from google.protobuf import any_pb2
from google.protobuf import struct_pb2
from hamcrest import assert_that
from proto_matcher import equals_proto
from remapped_api import api_pb2 as remapped_api_pb2
from remapped_internal import internal_pb2 as remapped_internal_pb2
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

    def test_same_type_conversion(self):
        """Converting a message to its own type produces an equal copy."""
        src = api_pb2.SimpleMessage(text="hello", number=42)
        dest = proto_converter.convert(src, api_pb2.SimpleMessage)
        assert dest == src
        assert dest is not src

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

    def test_typed_to_any_singular(self):
        """Proto -> Any auto-conversion: typed field is packed into Any."""
        src = api_pb2.TypedPayload(
            payload=api_pb2.SimpleMessage(text="typed", number=42),
        )
        dest = proto_converter.convert(src, internal_pb2.TypedPayload)
        unpacked = api_pb2.SimpleMessage()
        dest.payload.Unpack(unpacked)
        assert_that(unpacked, equals_proto(api_pb2.SimpleMessage(text="typed", number=42)))

    def test_typed_to_any_repeated(self):
        """Repeated Proto -> Any auto-conversion: each element is packed."""
        src = api_pb2.TypedPayload(
            payloads=[
                api_pb2.SimpleMessage(text="a"),
                api_pb2.SimpleMessage(text="b"),
            ],
        )
        dest = proto_converter.convert(src, internal_pb2.TypedPayload)
        assert len(dest.payloads) == 2
        for i, text in enumerate(["a", "b"]):
            unpacked = api_pb2.SimpleMessage()
            dest.payloads[i].Unpack(unpacked)
            assert unpacked.text == text

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

    def test_keyword_form(self):
        """The keyword form @convert_field(field_names=[...])."""

        class InternalToApiPerson(
            proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]
        ):
            IGNORED_FIELDS = ["created_at"]

            @proto_converter.convert_field(field_names=["internal_id"])
            def convert_id(self, src, dest):
                dest.metadata["id"] = src.internal_id

        src = internal_pb2.Person(name="Dave", internal_id="x")
        dest = proto_converter.convert(src, api_pb2.Person)
        expected = api_pb2.Person(name="Dave", metadata={"id": "x"})
        assert_that(dest, equals_proto(expected))

    def test_handler_not_overwritten_by_recursive_converter(self):
        """A @convert_field handler for a field with different-but-convertible message
        types must not be silently overwritten by a recursive auto-converter."""

        class _InternalToApi(
            proto_converter.ProtoConverter[internal_pb2.InternalDetail, api_pb2.ApiDetail]
        ):
            IGNORED_FIELDS = ["internal_note"]

        class _NestedConverter(
            proto_converter.ProtoConverter[internal_pb2.DifferentNested, api_pb2.DifferentNested]
        ):
            @proto_converter.convert_field(["detail"])
            def convert_detail(self, src, dest):
                dest.detail.info = src.detail.info.upper()
                dest.detail.priority = 999

        src = internal_pb2.DifferentNested(
            label="test",
            detail=internal_pb2.InternalDetail(info="hello", priority=1, internal_note="n"),
        )
        dest = proto_converter.convert(src, api_pb2.DifferentNested)
        expected = api_pb2.DifferentNested(
            label="test",
            detail=api_pb2.ApiDetail(info="HELLO", priority=999),
        )
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
        """set_module_resolver's return value is used as the import path. (deprecated)"""
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

        with pytest.warns(DeprecationWarning, match="set_module_resolver is deprecated"):
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
            with pytest.warns(DeprecationWarning):
                proto_converter.set_module_resolver(None)


class TestModuleResolverRules:
    """Tests for add_module_resolver_rule using protos whose proto packages
    (``ext_api.remote.v1`` and ``ext_internal.remote.v1``) don't match their
    Python module paths (``remapped_api.api_pb2`` and ``remapped_internal.internal_pb2``).
    Conversion between them only works when appropriate rules are registered."""

    @pytest.fixture(autouse=True)
    def _clear_rules(self):
        """Ensure each test starts with no rules registered."""
        from proto_converter.converter import _module_resolver_rules

        saved = list(_module_resolver_rules)
        _module_resolver_rules.clear()
        yield
        _module_resolver_rules.clear()
        _module_resolver_rules.extend(saved)

    def _src(self) -> remapped_api_pb2.Wrapper:
        return remapped_api_pb2.Wrapper(inner=remapped_api_pb2.Inner(name="hello"))

    def test_no_rule_fails(self):
        """Without any resolver rule, conversion fails because the default path
        (derived from the proto package) doesn't match the Python module.

        The "Couldn't resolve type" RuntimeError from ``_descriptor_to_type`` is
        caught during recursive-converter registration, so the user-visible error
        is "Unhandled fields" at validation time."""
        with pytest.raises(NotImplementedError, match="Unhandled fields"):
            proto_converter.convert(self._src(), remapped_internal_pb2.Wrapper)

    def test_rule_with_named_group(self):
        """A rule with a named capture group rewrites the module path via str.format."""
        proto_converter.add_module_resolver_rule(
            r"ext_api\.remote\.v1\.(?P<mod>\w+)", "remapped_api.{mod}"
        )
        proto_converter.add_module_resolver_rule(
            r"ext_internal\.remote\.v1\.(?P<mod>\w+)", "remapped_internal.{mod}"
        )

        dest = proto_converter.convert(self._src(), remapped_internal_pb2.Wrapper)
        assert dest.inner.name == "hello"

    def test_rule_with_positional_group(self):
        """Positional groups are 0-indexed in the replacement (str.format convention),
        NOT 1-indexed like regex substitution."""
        proto_converter.add_module_resolver_rule(r"ext_api\.remote\.v1\.(\w+)", "remapped_api.{0}")
        proto_converter.add_module_resolver_rule(
            r"ext_internal\.remote\.v1\.(\w+)", "remapped_internal.{0}"
        )

        dest = proto_converter.convert(self._src(), remapped_internal_pb2.Wrapper)
        assert dest.inner.name == "hello"

    def test_near_miss_is_called_out_in_error(self):
        """When a rule's pattern matches the path as a prefix but doesn't fullmatch,
        the resolver surfaces a ValueError that mentions the near-miss, so users
        aren't left wondering why their rule didn't fire.

        (ValueError — not RuntimeError — so it propagates through the recursive
        converter's ``except (NotImplementedError, RuntimeError)`` handler instead
        of being silently swallowed into a generic "Unhandled fields" error.)"""
        proto_converter.add_module_resolver_rule(r"ext_api\.remote\.v1", "does_not_matter")
        with pytest.raises(ValueError, match="matched a prefix but not the full path"):
            proto_converter.convert(self._src(), remapped_internal_pb2.Wrapper)

    def test_duplicate_rule_same_replacement_is_noop(self):
        """Adding the same pattern/replacement twice is silently deduped."""
        proto_converter.add_module_resolver_rule(r"foo\..+", "bar.{0}")
        proto_converter.add_module_resolver_rule(r"foo\..+", "bar.{0}")  # no-op
        from proto_converter.converter import _module_resolver_rules

        assert len(_module_resolver_rules) == 1

    def test_duplicate_rule_different_replacement_raises(self):
        proto_converter.add_module_resolver_rule(r"foo\..+", "bar.{0}")
        with pytest.raises(ValueError, match="already registered"):
            proto_converter.add_module_resolver_rule(r"foo\..+", "baz.{0}")

    def test_ambiguous_match_raises(self):
        """Two rules matching the same path raise at construction time."""
        # Two different patterns that both match the api path.
        proto_converter.add_module_resolver_rule(r"ext_api\.remote\.v1\.(\w+)", "remapped_api.{0}")
        proto_converter.add_module_resolver_rule(
            r"ext_api\.remote\.v1\.(?P<mod>\w+)", "remapped_api.{mod}"
        )
        with pytest.raises(ValueError, match="Multiple module resolver rules matched"):
            proto_converter.convert(self._src(), remapped_internal_pb2.Wrapper)

    def test_remove_rule(self):
        proto_converter.add_module_resolver_rule(r"foo\..+", "bar.{0}")
        proto_converter.remove_module_resolver_rule(r"foo\..+")
        from proto_converter.converter import _module_resolver_rules

        assert len(_module_resolver_rules) == 0

    def test_remove_nonexistent_rule_is_noop(self):
        # Idempotent: removing a rule that was never added doesn't raise.
        proto_converter.remove_module_resolver_rule(r"nothing\..+")


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

    def test_convert_field_string_not_list(self):
        with pytest.raises(TypeError, match="list of strings"):

            class Bad(proto_converter.ProtoConverter[internal_pb2.Person, api_pb2.Person]):
                IGNORED_FIELDS = ["created_at"]

                @proto_converter.convert_field("internal_id")  # type: ignore[arg-type]
                def handle(self, src, dest):
                    pass

    def test_auto_created_before_subclass(self):
        """Calling convert() auto-creates converters for the tree; a later subclass fails."""
        proto_converter.convert(api_pb2.SimpleMessage(text="hi"), internal_pb2.SimpleMessage)
        with pytest.raises(RuntimeError, match="Already have a converter"):

            class Late(
                proto_converter.ProtoConverter[api_pb2.SimpleMessage, internal_pb2.SimpleMessage]
            ):
                pass
