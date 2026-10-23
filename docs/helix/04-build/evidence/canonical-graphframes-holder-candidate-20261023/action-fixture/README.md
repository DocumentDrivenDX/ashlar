# Canonical `.4` GraphFrames action fixture — offline preparation

This fixture is a deterministic 3-object/2-edge source for the smallest
canonical `.4` action slice. It has one isolated object and two parallel edges
from `(ashlar-canonical-graphframes, 1, 101)` to
`(ashlar-canonical-graphframes, 1, 202)`, with distinct edge identities 9001
and 9002. The source rows include all generated carrier values except
`lookup_hash`; the writer computes that value in Spark with the pinned native
`xxhash64(CAST(source_system AS STRING), CAST(type_id_or_rel_type_id AS BIGINT),
CAST(id AS BIGINT))` expression.

`source-rows.json` is emitted by
`canonical_fixture_source_bytes()` in
`src/ashlar_host/pinned_delta_graphframes.py`. `independent-oracle.json` is a
separately authored two-occurrence bag for the selected seed. The immutable
carrier resource, UMF revision, and selected runtime tuple are named in the
source document, including exact JAR SHA-256 pins. The action cap is 2 and its
ordered output schema is `src_source, src_type, src_id, edge_source, edge_type,
edge_id, dst_source, dst_type, dst_id`. All integer values fit signed BIGINT;
explicit nulls are `object_current.root_id` and `edge_current.order_key`; every
other field is present with a fixed deterministic value. `published_at` is
`2026-10-23T00:00:00.123456Z`; the writer parses it as timezone-aware UTC and
preserves all six fractional digits.

The generator revision is `canonical-graphframes-action-fixture/0.1`.
At preparation, SHA-256 values were generator module
`f122f3082aefd60e800c0aaed347da20e471aeaa05b195ff552c68308c10238f`, source
rows `5e3c1110a982f04fefe03453d29306654ad4f1a631a170daa0125fd4e50a87ab`, and
independent oracle
`7b167c9e871196ced1f352eb8c3440474fac66e18b09da895f70ed4c52c8b832`.

The writer and cleanup helper use the same stable `.ashlar-graphframes-action.lock`
inode as `PinnedCanonicalGraphFramesActionFixture`. The writer requires a fresh
private mode-0700 root containing only that lock and leaves a durable receipt
after successful table creation. Cleanup requires the receipt and verifies its
root inode, exact table paths/inodes, generator/source digests and Delta UUIDs
before removing those tables. It refuses preexisting table directories that
have no matching complete receipt. The action wrapper retains the same lock for
its complete interval.

Actual persisted schema, nullability, protocol, timestamp encoding and Spark
`xxhash64` output have not been verified against native Delta files. The writer
test uses inert Spark-shaped fakes only. Fixture writing, cleanup, Spark
creation, and GraphFrames execution were not run during this offline
preparation.

The action remains blocked until a separately reviewed closed manifest and
external anchor digest bind the exact generator/source/oracle bytes, formula
proofs, 3/2 counts, seed, cap 2, and nine-field output schema, and until the
Truss execution slot and independent review are available. No source progress,
ACK, publication, Unity Catalog, or production authority is claimed.
