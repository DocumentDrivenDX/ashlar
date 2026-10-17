# Installed typed graph Weft queries

Four installed `run-configured-local` query invocations passed on the retained
[typed graph Delta installation](../configured-typed-graph-installed-20261017/README.md).
The offline wheel was built from Ashlar
`c48c8b2f442d85741251e54962a1c5148e6fb223`, SHA256
`8aab957743de8d6fee85c866e716c41ac637d44680802aef18b744e72542984c`.
The original authored UMF 0.8 model and 13 source events remained unchanged.
Fresh public UMF admission used exact revision
`07357eadbda9cf8299b9d72d004cbb77f8091aee`. The indexed Weft public 0.4.1
count-star-having/paths-keys compiler produced identical original response and
recompiled text in every invocation. All original SQL and guards ran unchanged.

| Case | Selected ordinal | Expected result | Guards passing | Seconds |
| --- | ---: | --- | ---: | ---: |
| Historical one-hop, bound 2 | 1 | Person 1: person-1/person-2, truncated; other two roots: empty | 13 | 69.053 |
| Current one-hop, bound 2 | 3 | Person 1: person-1, complete; isolate: empty | 13 | 45.895 |
| Historical two-hop root projection | 1 | person-1 three times | 13 | 50.363 |
| Historical two-hop path count | 1 | 3 | 13 | 49.823 |

Each expected result comes from the selected original source prefix, independently
of generated SQL and native cells. The original topology includes two parallel
Person 1→Person 2 edges and one Person 1 self-loop. Its three two-hop paths use
the self-loop as first edge and each of those three edges as second edge.
Independent witnesses retain both edge IDs per path. The one-hop bound exposes
full-bag truncation, including the third occurrence beyond the returned prefix.
The historical selection still uses ordinal 1's complete vector after ordinal 3
has deleted Person 2 and related edges. The final topology retains the self-loop
and isolate. This does not qualify distinct-target counts, grouped counts,
property joins, inverse traversal or direct independent edge-row projection.

All four stdout results equal their public and runtime report files. Each
returned the unchanged latest source checkpoint `5724`, three completed batches
and `protected_ack: false`. Separately retained before/after fingerprints match
for every query: installation registry, all Delta JSON logs and complete SQLite
operation/plan/progress rows. The existing held source, publication resolver,
physical aliases, native schema/protocol/retention, compiler obligations and
required closing checks supplied execution custody. Final process inspection
found no remaining operator or Spark worker. No reingest or cloud compute ran.

Python 3.11.17, Spark 4.0.1 and Delta 4.0.0 used sequential local workers with a
512 MiB driver. These are tiny functionality checks; durations include fresh
process/Spark setup and verification and are not query latency benchmarks.
Local immutable-file progress supplies no protected producer ACK, Unity Catalog
or remote writer authority. Query examples are in the
[operator guide](../../../../../examples/end-to-end/typed-graph-README.md).

`manifest.json` retains compressed and original hashes and lengths for 32 files:
original invocations, stdout/results, paired state fingerprints, wheel preparation,
runtime metadata and exact operator scripts. Gzip timestamps are zero. Full
original compiler protocol, native guards, decoded tagged key carriers and
independent source witnesses are retained in each successful stdout result.
The retained Delta installation is separate from this compact evidence archive.
