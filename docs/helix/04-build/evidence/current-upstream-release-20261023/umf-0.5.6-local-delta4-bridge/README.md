# Public UMF 0.5.6 local Delta 4 bridge rebind

Ashlar's embedded `spark-delta4/0.1` proposal bridge is rebuilt from public
UMF tag `umf-v0.5.6`, commit
`259bf1ed91466ff7e9782d7517c14f2112282026` (tree
`204cfba81eadbe9a6a0458bc9a7de2dc1e50e58b`). The prior bridge was built from
the pre-merge candidate `9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08`. Seven of
the 33 UMF source inputs in Ashlar's bridge build inventory changed between
those revisions; 188 of 195 total build inputs are byte-identical. The
[source-input comparison](source-input-delta.json) records the exact changed
paths and hashes.

The bridge was rebuilt with Bun 1.4.3 from the tag's `src/adapters/delta/ddl.ts`
and `src/model/document.ts`, using the retained [entry](../../../../../../src/ashlar_host/resources/local-current-delta4/bridge-entry.ts),
[build script](build-bridge.ts), and [metafile](metafile.json). The six
Ashlar role source bytes were passed through the rebuilt bridge and compared
with the selected role semantics in the [source check](merged-tag-six-role-source-check.json).
This is source/bridge admission evidence, not native Delta qualification.

| Item | SHA-256 |
| --- | --- |
| Bridge IIFE (508,540 bytes) | `34deb5c79b5dcb1ace8093e66a4b1213a98cbcc1fb50bfd24fdc4a21721c76b5` |
| Build-input inventory (195 inputs) | `b314d067d88ede4117f37c0b25604f53fac5753b5c5c2d05500762530950a26a` |
| Build script | `2c03ed1d4ef593bdad98e235bd18f5c5d8be078deded42fde2ef0ff2cce71124` |
| Build metafile | `19d4a797180a4ac55d0afa46cb2174884193cab1494f0add09e7666e437c4b08` |
| Bridge entry | `d05088842f4c00d21151b1707e74e8477bda54f860940f7996540705140b11c8` |
| Realization manifest | `da7d4b2a50ca949632520e64a616674646ecc3f829c3b9574dadced42ee84089` |
| Role semantics | `148a8f471b572e5dcb2cd19c8f170404c26c37f6ebb2c670ddbe3c74a224f33d` |

Focused offline verification passed: the six role source/schema outputs retain
their admitted semantics, and `tests.test_local_current_delta4_umf` plus
`tests.test_local_current_xxhash64_profile` passed 10 tests. No Spark or Delta
process ran. The separate historical [one-table receipt](../umf-0.5.6-delta4-one-table-acceptance-20261023/README.md)
was made against the earlier candidate and is not requalified by this rebuild.
Neither receipt qualifies the full Truss lifecycle, Unity Catalog, publication,
ACK, or production performance.
