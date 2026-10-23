# Batch Fetch

Status: Implemented

Authors:

* [Matt Van Horn](https://github.com/mvanhorn)

## Summary

`POST /api/v1/fetch` takes a list of table lookups and returns one sparse
result per lookup, in the same order. The lookups run at the same time.
A single-table `POST /api/v1/table/{name}/fetch` is unchanged.

## Motivation

An inference request often needs features from more than one table: user
rows, item rows, and a pair table, for the same batch of keys. Today that
is N HTTP round trips. The client has to fan them out, handle N errors,
and join the replies. Issue #5 asks for one call that does that fan-out
on the server.

## Goals

- One HTTP request that fetches from several tables.
- Result order matches request order, including two lookups of the same table.
- Lookups run concurrently under the existing store read lock.
- The whole call fails if any lookup fails.

## Non-Goals

- Arrow IPC for this route. Tables in one call can have different schemas,
  and a single IPC stream has one schema.
- Partial success. A mixed 200 with per-item errors is a second response
  shape, and the client still has to retry the failed items.
- Changing Flight `do_get`. Tickets stay one table.

## Design

Request:

```json
{
  "requests": [
    {"table": "user", "keys": {"id": ["u1", "u2"]}, "columns": ["f1"]},
    {"table": "item", "keys": {"id": ["i1"]}, "columns": ["price"]}
  ]
}
```

Each item is the JSON fetch body plus `table`. Response:

```json
{
  "results": [
    {"table": "user", "columns": {"_idx": [0, 1], "f1": [0.1, 0.2]}},
    {"table": "item", "columns": {"_idx": [0], "price": [3.5]}}
  ]
}
```

Each `columns` object is the same sparse shape as a single-table fetch:
`_idx` plus the requested columns, one row per found key.

`MurrService::read_many` maps the list with Rayon onto `read`. Reads already
share the store `RwLock`, so two tables (or two lookups of one table) do
not wait on each other. Schema checks run first, sequentially, so a missing
table fails before any read starts.

Empty `requests` returns `{"results": []}`.

## Alternatives

- One result map keyed by table name. Two lookups of the same table would
  collide, and ranking often fetches the same table twice with different
  columns.
- Keep going after a failed item and return per-item errors. The client then
  has two ways to notice a miss: an item error, or a sparse `_idx` gap.
  One error for the call is enough.
- Multiplex Arrow IPC with a `table` metadata key on each batch. Clients
  that already speak the JSON fetch body can reuse it here; mixed schemas
  in one stream are extra work for little gain.

## Compatibility

Existing single-table fetch, write, and Flight tickets do not change.
Python lives in another repo and can add `get_batch` against this route
later.
