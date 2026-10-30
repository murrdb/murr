# Sparse Reads

Status: Implemented

Authors:

* [Roman Grebennikov](https://github.com/shuttie)

## Summary

A fetch response holds only the keys that were found. Its first column, `_idx`
(`uint32`), is the 0-based position of each row's key in the request. A missing
key has no row, and row order is not part of the contract. Before this, a
response had one row per requested key, with nulls for a miss.

## Motivation

Take a ranking feature like "how often did this customer order from this
vendor". A request asks about one customer and every vendor on screen, and
almost no pair has history. On a replay of a day of such requests, 1.55% of
lookups found a row and the median request found one row out of 150 keys.

A dense Arrow response does not shrink when values are null: a null primitive
keeps its 8-byte slot. The median response was 5.4 KB for one found row, 6x
what Redis sends for the same lookup, and the server spent most of its read
time appending nulls.

## Design

Request `{"keys": {"doc_id": ["doc_1", "nope", "doc_5"]}, "columns": ["score"]}`
returns:

```json
{"columns": {"_idx": [0, 2], "score": [0.95, 0.68]}}
```

Arrow IPC and Flight `do_get` return the same batch: `_idx` first, then the
requested columns in request order.

- `_idx` is present on every response. A shape that depends on the data is a
  client bug magnet, and 4 bytes per row is cheap.
- Row order is unspecified. The `multi_get_sorted` read method no longer
  permutes its results back into request order.
- A key listed twice gets two rows. Nothing found is a zero-row batch with the
  full schema. A null in a found row is a stored null.
- `_idx` is a reserved column name, rejected at table creation before anything
  reaches the manifest. Only that exact name is reserved.
- `uint32`, not `uint16`: two bytes per row is not worth a 65536-key cap.

## Compatibility

- HTTP fetch and Flight `do_get` change shape. A client that assumes "row i
  answers key i" misaligns rows as soon as one key is missing.
- Flight `get_schema` still returns the table schema, as before.
- Python client: needs the scatter step inside `read`. Out of scope here.
- Data directories, manifest, write path, configuration: unchanged.

## Performance

pyarrow sizes, 4 columns of int64 and float64:

| Response                               | Bytes |
|----------------------------------------|------:|
| 152 dense rows (before)                |  5448 |
| 1 found row plus `_idx`                |   720 |
| 1000 all-hit rows, 10 float32 (before) | 41104 |
| 1000 all-hit rows plus `_idx`          | 45208 |

The all-hit benchmark pays about 10% more bytes, the low-hit median shrinks
about 7x. Latency has not been measured.

## Alternatives

- Keep the dense response: no client change, and 6x the bytes on the workload
  that matters.
- A `?sparse=true` flag, default off: two code paths in every store and client
  for a contract that is a few months old.
- Positions in schema metadata: keeps column names free, but metadata is text,
  so positions get encoded and parsed a second time. A column is zero-copy.

## Open Questions

- Whether `multi_get_sorted` should become the default read method now that
  it no longer pays for the un-permute. A benchmark question.
