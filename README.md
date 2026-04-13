# proto-converter

Automatic deep conversion between compatible protocol buffer types, with no
registration required for the common case. Inspired by
[python-proto-converter](https://github.com/google/python-proto-converter).

## The problem

You have parallel proto hierarchies — say an internal schema and a public API
schema — with messages that are structurally compatible (same field names and
types) but generated as different Python classes. Converting between them by hand
is tedious and breaks every time a field is added.

## What this library does

Call `proto_converter.convert(msg, TargetType)` and it figures out the rest:

1. **Scalars, enums, `Struct`, `Any`** — copied when the name and type match.
2. **Nested messages** (singular, repeated, and map values) — if the field names
   match but the message types differ, a converter for the nested types is created
   automatically and applied recursively. This works to arbitrary depth.
3. **No registration needed** when every source field has a compatible counterpart
   in the destination. Just call `convert()`.

When the types *aren't* fully compatible (extra fields, renamed fields, fields
that need transformation), you register a converter subclass — but only for the
specific type pair that differs, not for the whole tree.

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

class InternalToApi(ProtoConverter[internal_pb2.Person, api_pb2.Person]):
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
at runtime — so missing fields are caught early.

## Custom type resolution

The recursive converter needs to map protobuf `Descriptor` objects back to Python
classes. By default it uses `importlib`, assuming the proto package maps directly
to a Python package. If your generated code lives under a different prefix,
install a resolver:

```python
import proto_converter

def my_resolver(descriptor):
    if descriptor.full_name.startswith("mycompany."):
        # Remap to the actual Python package
        ...
    return None  # fall through to default resolution

proto_converter.set_type_resolver(my_resolver)
```

## Development

```bash
just setup                       # install deps + generate test protos
just                             # format, check, and test (the default)
just test                        # just tests
just check                       # lint + type check
just format                      # auto-format
just build-protos                # regenerate test protos after changing .proto files
```
