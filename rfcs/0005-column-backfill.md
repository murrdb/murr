# Column Backfill

Status: Implemented

Authors:

* [Matt Van Horn](https://github.com/mvanhorn)

## Summary

A write no longer has to carry every column of the table. Key columns are
still required. Any omitted value column is filled from the row already
stored for that key. A new key gets nulls for omitted nullable columns,
and is rejected if an omitted column is non-nullable.

## Motivation

Feature tables grow a column at a time. A job that backfills `click_rate_7d`
for every user already in the table should not have to re-send `country`
and `signup_ts`. Today a write with only some columns fails because the
row blob is a full row, last-write-wins: missing fields would wipe the
previous values.

Issue #33 asks for that backfill: send columns A and B, keep C from the
existing row.

## Goals

- A write batch may list any subset of value columns, plus all key columns.
- Omitted columns keep their stored values for keys that already exist.
- New keys may omit only nullable columns; those become null.
- JSON, Arrow IPC, and Parquet writes all follow the same rule.

## Non-Goals

- True sparse storage of null columns. The stored row is still a full blob.
- Concurrent column updates without lost writes. The merge is
  read-modify-write; two backfills of different columns on the same key
  can clobber each other.
- Changing a table schema as part of the write.

## Design

`Table::write` looks at the batch schema:

1. Every key column must be present. Extra columns the table does not
   know are ignored, as before.
2. If every value column is present, the existing full-row write runs.
3. Otherwise it reads the omitted columns for those keys, scatters the
   sparse `_idx` response back to request order with Arrow `take`, and
   writes a complete row.

A found row copies the stored value, including a stored null. A miss on
a nullable omitted column becomes null. A miss on a non-nullable omitted
column is `TableError`.

JSON `WriteRequest` builds a `RecordBatch` from only the columns in the
body, so a JSON patch of `{id, score}` never has to invent `label`.

## Alternatives

- Require the client to fetch, join, and rewrite. That works, and it is
  what people do today, but it doubles the bytes on the wire for a one
  column change.
- Store each column in its own RocksDB key. Then a backfill would be a
  true partial write, at the cost of one Get per column on the read path
  that already has to be fast.
- Copy previous segment bytes at the row-codec layer instead of merging
  Arrow arrays. That avoids `take`, but it needs a raw `Store::get` and
  a `WriteRow::set_null`. The Arrow merge reuses `read` and `write_full`.

## Compatibility

Full writes are unchanged. A client that still sends every column never
hits the merge. Fetch responses, keys, and the row format stay the same.

A write that used to fail with a missing column now succeeds when the
omitted columns are nullable or already stored.
