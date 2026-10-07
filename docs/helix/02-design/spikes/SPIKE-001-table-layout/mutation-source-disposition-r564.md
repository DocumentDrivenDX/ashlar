# Complete source-only probe — R562–564

A bounded native read-only probe evaluates the exact sixth normalized mutation
relation independently of the current target scan. All20 before/after fields are
hashed with the established exact token algorithm. Complete multisets match the
independent R529 oracle:90k update preimages,90k update postimages and10k deletion
preimages; typed identities are unique within both disjoint operation groups.
The input pins are unchanged and no current/manifest data is written.

The first probe took5485.970875ms caller and4761ms engine, reading233106753 bytes
entirely from reported data cache,22 files and190k input rows, with0 writes/spill.
Compilation was378ms and metadata_time_ms0. Aggregation, sorting and hashing add
validation work; this is not a naked source-materialization timing or a controlled
cold/warm cohort. No repeat executed. The initial relational plan shows a
SortMergeJoin LeftOuter and a non-Photon section; this is not the executed
adaptive plan inside the historical MERGE.

The harness stopped on its first result because Boolean values were rendered
False/True by the SQL driver, while its expected strings used lowercase. All
counts and full carrier digests match exactly. R563 qualifies only that Boolean
representation difference, retrieves final history for the same four completed
statement IDs and audits exact SQL/result-plan hashes/finality/costs. It does not
replay the query, silently alter the stopped runner, or claim the planned repeat
ran. The old assertion/stop remain in the terminal summary. The overall original
setup/observation clock is unavailable; caller timing and recovery clock are
reported separately.

This makes additional source materialization lower priority than target-file
work: the scoped cached source plus validation takes under6 seconds versus the
historical64.68-second MERGE caller clock. These different workloads cannot be
subtracted to produce a causal target-cost estimate. The completed MERGE's large
remote read and low query-wide pruning remain evidence for a bounded matched
physical-layout/target-access experiment. Prior typed42 materialization and
hash-range subdivision failed to produce a whole-clock win; do not repeat them
without a distinct intervention. Full predecessor guards and source integrity
remain mandatory in any comparator.

UC Delta remains fixed. Warm singleton caller/engine, controlled cold-data,
publication freshness, sustained10k/s and100k/s burst, and1B/5B admission remain
open. No performance gate is passed by this source-only result; no consumer or
UMF support claim changes.
