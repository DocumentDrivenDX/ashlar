# Authored core 0.8 typed source example

This small additional-source example preserves original integer/decimal JSON
lexical tokens, booleans and optional string absence/null/value while applying
three complete create/update/delete transactions and exact replay. Its model is
separately authored UMF core 0.8; explicit type/property IDs are development
bindings, not Truss-issued catalog IDs. It does not downgrade a domain pack.

Run from the Ashlar repository with an existing DuckDB CLI and a clean UMF
checkout at `c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3`:

```sh
PYTHONPATH=src:tools python3 -B tools/run_typed_source_example.py \
  --umf-source /path/to/pinned-umf \
  --output /path/to/fresh-output
```

The public UMF `validateCoreRecordValues` API directly checks the original model
and all four original events. It owns integer ranges, decimal precision/scale,
field types, required presence and optional absence/null handling. The adapter
only translates exact source tokens and checks original request/evidence custody.
Keys, relationships and unknown relevant extension semantics require independent
public checks; their incompleteness cannot be replaced by a successful scalar run.

DuckDB screening tables retain canonical property text, all original event bytes
and per-transaction snapshots. Independent expectations cover uint64 maximum,
integers beyond 2^53, decimal trailing zeros/signed zero, booleans, absence,
explicit null and Unicode. Actual native typed queries compare exact final values,
while original raw properties/history remain available. Replaying the original
last group changes no logical state and submits zero native writes; fresh native
readback verifies unchanged current/history.

Retained output includes the original public UMF receipt, request, SQL, DuckDB
file, native queries/readbacks and summary. Output must be a fresh directory.
This is a local DuckDB screening profile, not Unity Catalog Delta deployment,
Weft numeric field qualification, source ACK, immutable publication or production
writer authority. Native Delta and Weft integration remain separate required work.
