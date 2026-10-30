# Compound Keys

Status: Implemented

Authors:

* [Roman Grebennikov](https://github.com/shuttie)

## Summary

A table key can now be made of several columns, and key columns can be `utf8`
or any integer type. Fetch requests become columnar: one array per key column,
sent as JSON or as an Arrow IPC stream.

## Motivation

Until now a table had exactly one `utf8` key column. Feature tables are often
keyed by a pair like `(user_id, item_id)`, so users had to glue the parts into
one string on the client, and convert integer ids to strings on the way.

The fetch request was also the last row-oriented piece of the API. Writes and
fetch responses are columnar and can travel as Arrow, but keys were a JSON list
of strings. With 1000 keys per request, parsing that list is work we can skip.

## Goals

- Keys over one or more `utf8` and integer columns.
- A fetch request that can be sent as Arrow IPC end to end.
- One internal request shape, so JSON is only an adaptor at the API boundary.

## Non-Goals

- Range or prefix scans. Key bytes have no meaningful order.
- Compatibility with existing data directories and the old fetch body.
  Murr is pre-alpha, both break.
- Type coercion of keys on the server.
- Python and Java bindings, and Flight `do_exchange`. These come later.

## Design

### Schema

The top-level `key` field is gone. Columns are flagged instead:

```json
{
  "columns": {
    "user_id": {"dtype": "utf8",  "key": true, "nullable": false},
    "item_id": {"dtype": "int64", "key": true, "nullable": false},
    "score":   {"dtype": "float32"}
  }
}
```

- At least one column must be a key.
- A key column cannot be nullable. `nullable` defaults to `true`, so key
  columns have to say `"nullable": false`. We reject the schema instead of
  quietly overriding the flag.
- Float and bool columns cannot be keys.
- The order of key components is the order of columns in the schema.
- A body with the old top-level `key` is rejected as an unknown field.

The separate `key` field existed to guarantee a single key column. That
guarantee is no longer wanted, and a list of names next to the column map
would say the same thing twice.

### Key bytes

Each key column is encoded so that it delimits itself, and the key is the
concatenation of its components in schema order:

- integers: LEB128 varint, zigzag for signed types
- `utf8`: varint length, then the bytes

There is no special case for single-column keys or for the last component.
The length prefix is what keeps `("ab", "c")` and `("a", "bc")` apart.

Varints come from the `integer-encoding` crate. Its `VarInt` trait covers all
eight integer widths, so one generic function serves every integer dtype.

### Encoding in code

Each dtype that can be a key implements one trait:

```rust
pub trait KeyEncoder: Send + Sync {
    fn encode_keys(&self, arr: &dyn Array) -> Result<BinaryArray, MurrError>;
}
```

It encodes a whole column at once, so the Arrow downcast happens once per
column and not once per value. The byte format of a dtype lives only in its
`KeyEncoder`. Adding bool or timestamp keys later means one more impl.

`KeyBatch` collects the encoded columns. It checks that every appended column
has the same number of rows, and `finish()` concatenates them row by row into
the final keys. `KeySchema` ties it together for a table: it finds the key
columns by name, rejects nulls, and runs the encoders. Both `Table::write` and
`Table::read` call it.

### Fetch request

Everything below the API layer sees one type:

```rust
pub struct FetchRequest {
    pub keys: RecordBatch,
    pub columns: Vec<String>,
}
```

`keys` must hold exactly the key columns of the table, matched by name, with
exactly the types from the schema. A missing column, an extra column, a wrong
type or a null is a 400. Row `i` of the response answers row `i` of `keys`,
and a key that is not found gives a row of nulls, as before.

Wire formats convert into `FetchRequest` through `TryFrom`:

JSON, `Content-Type: application/json`:

```json
{"keys": {"user_id": ["u1", "u2"], "item_id": [42, 7]}, "columns": ["score"]}
```

Arrow IPC, `Content-Type: application/vnd.apache.arrow.stream`: the batches of
the stream are the key columns. The columns to return go into the stream
schema metadata, under the key `columns`, as a JSON array of names.

The Flight `do_get` ticket is the JSON body plus a `table` field.

The HTTP handlers now match on the content type, and a missing or unknown one
is a 400. Before, anything that was not Arrow or Parquet was parsed as JSON.

### Strict key types

The server does not cast key columns. An `int32` array sent for an `int64` key
is an error. JSON requests are not affected, because JSON values are parsed
with the dtype from the schema.

This matters for Arrow clients: pyarrow infers `int64` from a list of Python
ints, and polars sends `LargeUtf8` or `Utf8View` for strings. The client
bindings should cast by the table schema before sending.

## Compatibility

- `manifest.json` files written before this change do not load, and old data
  has different key bytes. Data directories have to be recreated.
- Table creation, the fetch body and the Flight ticket all change shape.
- The Arrow schema returned over Flight no longer has the `key` metadata
  entry. Clients read key columns from the HTTP schema endpoint.
- Writes keep their shape. Two side effects: an Arrow IPC write now reads all
  batches of the stream and not only the first one, and it needs a proper
  `Content-Type`.

## Testing

- `Table` tests on `MemoryStore`: an `rstest` table of key shapes (single int,
  `utf8` + int, int + int, two strings that share a boundary, three
  components), and a second one for rejected key batches.
- Schema validation: float key, nullable key, no key.
- HTTP: compound key fetch as JSON and as Arrow IPC, IPC without the `columns`
  metadata, table creation with the old `key` field.
- Flight: `do_get` with columnar keys.

The read benchmarks compile but have not been run against this change yet.

## Alternatives

### Order-preserving key bytes

Big-endian integers with a flipped sign bit, and escaped, terminated strings.
Byte order then follows tuple order, which makes prefix scans possible. We do
not plan scans, and the encoding is harder to get right.

### Hashing the key tuple

Fixed 16-byte keys are attractive for PlainTable. A collision would return a
wrong row with no error, and that is not acceptable for a cache that people
trust to be exact.

### Keys as row tuples

`"keys": [["u1", 42], ["u2", 7]]` reads well, but components are positional.
Swap two components of the same type and you get misses instead of an error.
It also has no Arrow form, so the IPC request would need a different shape.

### Columns in a query parameter

`POST /fetch?columns=a,b` is the obvious place for an Arrow request. We expect
to add server-side expressions later, cosine similarity over embeddings for
example, and those will not fit into a URL. Schema metadata has no size limit,
and query vectors could travel as extra Arrow columns in the same batch.

### Casting keys on the server

Lossless casts (int widths, string variants) would be friendlier to Arrow
clients. We chose strict types to match the write path and to keep one
obvious place for the cast, which is the client binding.

## Open Questions

- Does key encoding show up in the read benchmark? `KeyBatch::finish` copies
  every key once for compound keys.
- Should a bare Flight client be able to learn the key columns without the
  HTTP endpoint?

## Updates

- 2026-10-04: the "Strict key types" section no longer holds. The server now
  widens key columns, `int32` into `int64` for example. See
  [0002-type-coercion.md](0002-type-coercion.md).
