# proto-converter

Automatic deep conversion between compatible protocol buffer types. Inspired by
[python-proto-converter](https://github.com/google/python-proto-converter) but
designed to require far less boilerplate.

## The problem

You have parallel proto hierarchies — say an internal schema and a public API
schema — with messages that are structurally compatible (same field names and
types) but generated as different Python classes. Converting between them by hand
is tedious and breaks every time a field is added.

The existing [python-proto-converter](https://github.com/google/python-proto-converter)
helps with this, but still requires an explicit mapping for every relevant proto.
Submessages are particularly cumbersome because they require their own converter
*and* special handling in the containing converter. This `proto-converter` library
does away with that requirement. As long as the submessages are also automatically
convertible, no converters need to be defined at all.

## What this library does

Call `proto_converter.convert(msg, TargetType)` and it figures out the rest:

1. **Scalars, enums, `Struct`, `Any`** — copied when the name and type match.
   Enums with different types are compatible if every source value number exists
   in the destination (matching proto wire-format semantics).
2. **Nested messages** (singular, repeated, and map values) — if the field names
   match but the message types differ, a converter for the nested types is created
   automatically and applied recursively. This works to arbitrary depth.
3. **No registration needed** when every source field has a compatible counterpart
   in the destination. Just call `convert()`.

When the types *aren't* fully compatible (extra fields, renamed fields, fields
that need transformation), you register a converter subclass — but only for the
specific type pair that differs, not the whole tree.

Note the asymmetry: extra *source* fields with no destination counterpart raise
`NotImplementedError` (potential data loss). Extra *destination* fields are left
at their proto3 defaults (harmless).

## Installation

```bash
pip install proto-converter
```

## Quick start

```python
import proto_converter

# Deep-convert an entire message tree with zero configuration — works as long
# as field names and types are compatible at every level.
api_msg = proto_converter.convert(internal_msg, api_pb2.MyMessage)
```

When the source has fields the destination doesn't (or vice versa), register a
converter to tell the library what to do with them:

```python
from proto_converter import ProtoConverter, convert_field

class PersonConverter(ProtoConverter[internal_pb2.Person, api_pb2.Person]):
    # Fields that exist only in the source and can be dropped.
    IGNORED_FIELDS = ["internal_id", "created_at"]

    # Fields that need custom logic.
    @convert_field(["secret_name"])
    def convert_name(self, src, dest):
        dest.display_name = src.secret_name.upper()
```

Just defining the class is enough — `ProtoConverter.__init_subclass__` registers
it in a global registry. After that, `proto_converter.convert()` finds and uses
it automatically, including when it appears as a nested message inside a larger
conversion.

Any field that can't be auto-converted and isn't handled by `IGNORED_FIELDS` or
`@convert_field` raises `NotImplementedError` at converter construction time, not
during conversion — so missing fields are caught early.

**Important:** `convert()` auto-creates and caches converters for the entire message
tree on first call. If you define a `ProtoConverter` subclass for a type pair that
was already auto-created, registration will fail. Define all custom converter
subclasses before calling `convert()`.

## Custom type resolution

The recursive converter needs to map protobuf `Descriptor` objects back to Python
classes. By default it uses `importlib`, assuming the proto package maps directly
to a Python package. If your generated code lives under a different prefix, use
`set_module_resolver` to remap the import path:

```python
import proto_converter

def resolver(module_path: str) -> str | None:
    if module_path.startswith("ultravox."):
        return f"ultravox_proto.{module_path}"
    return None  # use the original path

proto_converter.set_module_resolver(resolver)
```

For full control over type resolution (e.g. when you need to intercept at the
`Descriptor` level), use `set_type_resolver` instead — it receives a protobuf
`Descriptor` and returns a Python class directly.

## Test recommendations

For most projects, no conversion-specific tests are likely to be helpful as
higher-level tests ought to invoke the code path calling `convert` anyway. That
invocation is sufficient to ensure all fields can be converted.

If specific customized conversion code is difficult to test at a higher level,
that may warrant conversion tests.

## Thread safety

Converters are cached in a global registry. Once a converter for a given type pair
has been created (typically at import time or on first use), `convert()` is a plain
dict lookup followed by a stateless conversion. It is thread-safe as long as any
custom field conversions on the path are themselves thread-safe.

However, converter *construction* (the first `convert()` call for a new type pair,
or defining a `ProtoConverter` subclass) is not thread-safe. The same applies to
`set_module_resolver()` and `set_type_resolver()`. If this is a concern, do all of
these during single-threaded startup rather than lazily from worker threads.

## Proto2 notes

This library is designed for proto3 but works with proto2 in most cases. Known
differences:

- **Default values**: auto-conversion uses `ListFields()`, which skips fields set
  to their default value. In proto3 this is standard (defaults are always
  zero-values). In proto2, fields with explicit non-zero defaults that happen to be
  set to that default will be skipped. Use `@convert_field` for any proto2 fields
  where preserving explicit defaults matters.
- **Required fields**: not validated — a required source field at its default won't
  be copied, potentially producing an invalid destination message.
- **Groups**: not supported (groups are extremely rare in practice).

## Development

```bash
just install                     # install deps + generate test protos
just                             # format, check, and test (the default)
just test                        # just tests
just check                       # lint + type check
just format                      # auto-format
just build-protos                # regenerate test protos after changing .proto files
```

## Releasing

1. Update the version in `pyproject.toml`.
2. Merge to `main`.
3. Tag and push: `git tag v<version> && git push origin v<version>`
4. Build and publish: `uv build && uv publish`
