---
ddx:
  id: ashlar.evidence.r59-native-query-export-20261102
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: FEAT-003
      kind: informed_by
---

# R59 native Weft query and `.4` graph release evidence — 2026-11-02

This packet records a focused local read qualification against the retained R59
publication `truss-graph-678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea`.
The Delta root and Truss source were read-only. No source, ACK, cloud, Unity
Catalog, or engine execution was involved.

## Native Weft query

The exact pinned four-role vector is recorded in
[`r59-vector-observation.json`](r59-vector-observation.json), with its bytes
hashed as `6a11806b3c45ed859343fa94e4ad193ff8b9487dcede8c8ba98f1d12cd5fd441`.
Opening and closing versions matched: object current 3, edge current 2,
tombstone 1, and whole-source history 3. The versioned R28 model SHA-256 is
`b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2`.
Source row expectations came from the exact retained R59 source page event
bytes (page digests are in the result receipt), not a prior G03 oracle.

[`r59-native-query-result.json`](r59-native-query-result.json) records request,
compiled SQL, and response hashes for four requests. On Spark 4.2.0, Delta
4.4.1, Weft 0.4.56, local[1], all returned expected values: singleton Dana;
grouped two-hop results Ada with two paths and one target; a two-row page Ada
and Ben with Cy as lookahead; and continuation Cy and Dana. The one Spark
application closed after the checks. The receipt preserves the first setup
refusal (`UNBOUND_SQL_PARAMETER p1`) as a runtime setup issue: the initial
attempt omitted
`spark.sql.legacy.parameterSubstitution.constantsOnly=true`; the successful
run included it. The packet also records an opening custody guard refusal caused
by schema-label rendering (`long` versus `bigint`), corrected to Spark SQL
`simpleString()` before the successful run. No native result was produced by
either refused attempt. The successful receipt now includes matching hashes for
full opening/closing table UUID, schema, version, manifest UUID/version and
selected manifest row state. Each compiled request binding digest and its four
role vector, model pin, layout and lookup hash profile are checked before SQL
table-name substitution.

## `.4` graph release

[`r59-graph-release.json`](r59-graph-release.json) is the direct generic
`read_graph_release` output for the same publication and pin vector. It is
canonical `ashlar-graph-release/0.1` content, 16,831 bytes, six unique nodes,
three unique edges, and SHA-256
`41c39394878978f6f0c161c8cc7adcbf39e8e89e82cd8e0133aae93f0b217237`.
Every edge endpoint is present; three nodes are isolated; a repeated endpoint
pair retains distinct edge identities and multiplicity. The export marks
`engineExecution=false`. [`r59-graph-export-close-receipt.json`](r59-graph-export-close-receipt.json)
records that the local Spark session closed successfully before the output file
was written.

This is a direct generic graph release readback. It does not establish the
end-to-end `read_original_truss_graph_export` owner path, which requires a
restored Truss source session; PostgreSQL was stopped for this qualification.
It also does not establish GraphFrames execution or managed Unity Catalog.

The focused fixes are in `src/ashlar_host/truss_graph_export.py` and
`src/ashlar/graph_release.py`: select `.4-xxhash64` only when the restored
installation and lookup-hash profiles match, and accept the native projection's
actual `.4` ordering of native hash before `published_at`. The adapter unit
tests cover `.3` compatibility, exact `.4` selection, and profile mismatch;
the graph release test covers the `.4` native projection. Focused command:

```text
PYTHONPATH=src:tests python3.12 -m unittest \
  test_graph_release.GraphReleaseTests.test_xxhash64_release_uses_native_typed_key_and_declares_exact_profile \
  test_truss_graph_export -v
Ran 3 tests: OK
```

## Release qualification remaining

The source-owner wrapper readback remains unqualified, as do managed runtime,
GraphFrames, and production authority. The distribution build was not repeated
in this packet; the isolated build attempt could not obtain the required
setuptools `>=77` in the restricted network environment. Package metadata and
stable release version/notes remain subject to the integrated release review.
No tag, publish, or push was performed.
