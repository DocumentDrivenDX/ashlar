# Installed grouped graph and distinct-terminal counts

Three installed `run-configured-local` query cases passed on the existing
[typed graph Delta publication](../configured-typed-graph-installed-20261017/README.md),
without reingest. The original authored UMF 0.8 model, explicit bindings and 13
source events remained unchanged. Public UMF admission used exact revision
`07357eadbda9cf8299b9d72d004cbb77f8091aee`; indexed Weft public 0.4.1 produced
byte-identical response and recompiled text in every successful invocation.
Every original SQL statement and emitted guard ran unchanged.

| Case | Selected ordinal | Exact result | Guards passing | Command seconds |
| --- | ---: | --- | ---: | ---: |
| Grouped knows→knows | 1 | person-1, 3 paths, 2 terminal keys | 15 | 43.550 |
| Grouped knows→located | 1 | person-1, 2 paths, 1 terminal key | 25 | 39.838 |
| Global knows→located after deletion | 3 | 0 paths, 0 terminal keys | 24 | 31.593 |

Expected bags derive from the selected original source prefix, independently of
SQL and native result cells. The same-type case includes the self-loop followed
by each of three independent outgoing edges. The cross-type case follows two
parallel knows edges through Person 2's located edge to Place 1. Distinct counts
compare complete ordered authored terminal-key tuples in the fixed terminal
Record context; path counts retain every edge pair. After deletion, no cross-type
path remains. The source oracle retains independent edge-pair witnesses. Small
inert controls additionally cover empty grouped results, distinct-only global
counts and changed group/pathTarget descriptors; those are not native passes.

All three successful stdout/public/runtime results agree. Each retains latest
checkpoint `5724`, three completed source batches and `protected_ack: false`,
while historical queries select ordinal 1's complete native vector. Separately
archived before/after fingerprints match for each query across installation
registry, all Delta JSON logs and complete SQLite operation/plan/progress rows.
Existing temporary physical aliases and all held source/publication/native
closing checks completed before result release. Final process inspection found
no remaining operator or Spark worker.

## Receipt-capacity correction

The first same-type case used exact Ashlar
`742eeb0553c0d1e7373d45eedbfb4ef677135905`, offline wheel SHA256
`35dd77d3cc1cab6d2221150836f4e478cc11c43b60e18f35e57b7c2c9acb2658`.
Its subsequent cross-type command refused without a successful report. The
initial runtime imposed 262,144 bytes despite a public CLI ceiling of 1 MiB.
Retained public probe protocol plus original guard metadata alone requires at
least 280,975 bytes for grouped cross-type and 263,220 for global cross-type,
before native schemas, cells or outer report fields. `initial/capacity-diagnosis.json`
records these conservative minima; the original probe files are retained.

The reviewed fix aligns the runtime ceiling with 1 MiB; the existing worker and
parent still enforce the selected CLI receipt bound. The two remaining cases
used an explicit 524,288-byte receipt bound and exact Ashlar
`3699fe51bfc0990b3209ef5da6dced62c78f9439`, offline wheel SHA256
`39712c70b42f706273f907f16d570d6c05386daeb97dd5f38ccf22288a1f50a2`.
The successful cross-type report is 315,584 bytes. The first successful case was
not repeated. The refused command and matching canonical-state fingerprints
remain under `initial/`; no guard completion or native pass is inferred from
that refusal. `capacity-fixed/` retains the two actual successful calls.

Python 3.11.17, Spark 4.0.1 and Delta 4.0.0 ran sequentially with a 512 MiB driver.
Durations include fresh process/Spark setup and verification; these tiny checks
are not latency or throughput benchmarks. No cloud compute ran. Local file
progress grants no protected producer ACK, Unity Catalog or remote writer
fencing. Inverse traversal, optional/numeric keys, arbitrary filters/joins and
direct edge-row projection remain outside this query subset. Use the
[operator guide](../../../../../examples/end-to-end/typed-graph-README.md).

`manifest.json` records compressed/original hashes and lengths for 47 files,
including both preparations, exact operator scripts, successful protocol/results,
paired fingerprints, initial refusal and public probe metadata. Gzip timestamps
are zero. The native Delta installation is retained separately.
