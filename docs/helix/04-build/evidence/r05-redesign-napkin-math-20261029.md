---
ddx:
  id: ashlar.evidence.r05-redesign-napkin-math-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-008
      kind: governed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.r05-materialization-physical-limits-20261024
      kind: informed_by
---

# R05 redesign napkin math and low-cost discriminator — 2026-10-29

## Decision

Keep the 1B-node/5B-edge attempt held. The current 28.206-minute projection is
for setup, concurrent writes and one combined persisted row/value plus endpoint
formula pass. It is already longer than the 15-minute review trigger and the
provisional five-minute design target before the second distinct scan, point
lookups, graph action, startup and cleanup. The measured sample disproves five
minutes as a supported projection for the current design; it does not establish
an approved revised SLO or prove that every future design must miss it. No
current codec or layout evidence supports an optimization likely to close that
gap while preserving the selected `.4-xxhash64` Delta carrier and complete test
semantics.

The PRD treats scale as an operational qualification target, not an architecture
gate, and requires an explicit budget for scale trials. The owner chose “Hold
and redesign first” in the current R05 packet. This note does not authorize a
scale attempt or change that decision.

## Napkin math

The paired sample contains 45M nodes and 225M edges, so target row volume is
22.2222 times the sample. The concurrent write took 30.620 seconds and its
setup/write/combined-scan interval took 60.924 seconds. At the packet's 80%
planning rate, those project to 14.176 minutes for writes and 28.206 minutes for
the measured interval. The latter alone requires at least 1.88x speedup to fit
under 15 minutes, or 5.64x to fit a provisional 300-second total. The five-minute
case would require that measured interval to fall from 60.924 seconds to at most
10.8 seconds on the existing sample, before assigning any time to omitted work.

The 30.304-second difference between that interval and the concurrent write
contains DDL/setup, metadata work and the combined witness. Those phases were
not timed separately, so treating all 30.304 seconds as variable scan time is a
planning case, not a measured scaling law. The write-only projection of 14.176
minutes is the minimum allocated time in this 80%-throughput planning case, not
a physical lower bound: throughput could change at scale. The concurrent measured
bundle projects to 28.206 minutes plus unmeasured work; the sequential bundle
projects to 30.684 minutes plus unmeasured work at the same planning rate. The
combined witness already checks row/value and endpoint formulas. The additional
work is the requested second distinct scan, point reads, GraphFrames action,
startup and cleanup. One paired sample, with the concurrent mode second in the
same JVM, cannot establish a reliable best-case full-run duration.

The paired sample wrote 3,041,334,560 active Parquet bytes. Linear scaling gives
67,585,212,444 bytes (67.585 decimal GB) at target. At the 80%-throughput
planning rate, writes alone take 14.176 minutes, about 79.5 MB/s aggregate.
Writing that output inside 300 seconds requires at least 225.3 MB/s, before
setup, scans or GraphFrames work: 2.84 times the planning rate. This arithmetic
does not identify storage bandwidth as the bottleneck; the sample has no
phase-resolved write profile or target-scale read/graph measurement.

A separate [edge-only CPU sample](r05-storage-throughput-reassessment-20261023/README.md)
measured 310.684 executor CPU-seconds for 225M edges. Linear scaling gives
about 6,904 executor CPU-seconds for 5B edges. At 18 cores continuously busy,
that is 6.4 minutes; at 80% utilization (14.4 effective cores), it is about
8.0 minutes. This cross-check covers edge writing alone, excludes nodes and
reads, and is not additive to the concurrent wall-time projection. It further
shows that the five-minute goal is not supported by the current measured work
profile, even if the local host has more than 96 GiB of physical RAM. Memory
adequacy at target remains unknown; active Delta bytes are disk use, not a
requirement to hold the whole dataset in RAM.

## Candidate changes assessed

| Candidate | Evidence and semantic effect | Disposition |
| --- | --- | --- |
| Increase shuffle partitions / file count above the current 18 | The current sample uses `local[18]` and 18 partitions. More tasks may lower per-task work and reduce output-file size at target, but they do not add cores or reduce bytes, and no measured task-size, spill or skew bottleneck exists. | Not a credible standalone route to the required 1.88x–5.64x gain. Do not tune until a profile identifies a task-granularity problem. |
| Change ZSTD to SNAPPY | A prior one-pair candidate exceeded its bounded output threshold before completing the INSERT, so there is no valid timing or full-table semantic comparison. The selected profile remains ZSTD. | Closed under that experiment's resource bound; do not retry or broaden the codec matrix. |
| Change row order/layout | Existing hash-range and identity-range sorting samples were slower and larger than the baseline; probe-file results did not establish useful pruning ([physical limits](r05-materialization-physical-limits-20261024.md)). | Rejected by current comparison. |
| Add more writer concurrency | The paired concurrent node/edge mode improved write-only time from 35.227 s to 30.620 s (about 1.15x) and the combined setup/write/validation interval from 66.278 s to 60.924 s (about 1.09x) in one same-JVM sample ([paired writer result](r05-paired-writer-second-20261024/README.md)). | Already included in the current baseline; the measured gains are insufficient and must not be counted again. |
| Remove a second persisted graph copy | TD-008's direct pinned-frame mapping avoids duplicate materialization while retaining graph-engine execution. | Already the selected path; it does not speed the measured Delta writes, and graph action cost is still unknown. |
| Disable regular Parquet statistics only for `source_delivery_id` | JFR stacks include `BinaryStatistics.updateStats`, but do not identify the active field. A prior single pair's byte effect was only 238,615 bytes (0.0093%) for the edge output; see the [offline write-path review](parquet-field-statistics-offline-review-20261023/README.md). | Weak falsification hypothesis only, not a recommendation or scale gate. Test first on a bounded 5M-edge baseline/candidate micro-pair; reject if the option is ignored, selected Delta statistics change, or relevant writer CPU/time does not improve. A small pair tests the mechanism, not target performance. |

These findings do not prove that the target cannot be met. They show that the
currently evidenced changes do not support a plausible timing claim. A larger
compute profile or a different physical design would need an explicit resource
and cost envelope and a semantics-preserving comparison before adoption.

## Next low-cost discriminator

Before any 45M/225M phase pilot, use a bounded 5M-edge baseline/candidate
micro-pair to test the weak `source_delivery_id` statistics hypothesis. Keep
the admitted runtime, schema, Delta protocol, ZSTD writer profile and SQL
identical; change only the per-column setting. Separately time setup/DDL, edge
INSERT, and footer/Delta-log validation; record task CPU/GC and output bytes.
Prove the option's footer effect, unchanged selected Delta statistics, and
exact row/value equality. If it passes those checks and shows a meaningful
writer CPU/time improvement, return for review before a larger phase-resolved
pilot. The micro-pair is only a mechanism falsification test, not a throughput
or scale gate. Do not run it as part of this note or infer that it closes the
five-minute gap.

If no smaller test identifies a credible bottleneck, do not repeat the
45M/225M sample solely for phase timing. A later phase-resolved pilot at that
shape would need a specific reviewable question, the same admitted schema,
Delta protocol, carrier, ZSTD writer profile and 18-core runtime, and separate
timers for DDL/setup, each role write, both point reads, each persisted scan,
GraphFrames construction/action, final checks and cleanup. It would record
active bytes, file geometry, plans, task CPU/GC, spill, scan input bytes and
Spark job/stage attribution without double-counting nested work. Keep any
attempt bounded by existing R05 runtime and temporary-storage controls, with
no retries, tuning matrix or full-scale data.

Any future phase pilot's decision is diagnostic: identify which measured
phases dominate and whether a linearly scaled estimate can approach an
owner-reviewed budget. If CPU, spill, file geometry or graph-action evidence
identifies a specific bottleneck, prepare exactly one changed-variable
follow-up with separate review. If the measured path cannot fit the design
target by arithmetic even before scale uncertainty, return the target, hardware
envelope and full-run decision to the owner before proposing a scale attempt.
Any complete projection above 15 minutes requires owner review, consistent
with the R05 packet.

## Limits

The pilot remains a synthetic local profile. It does not establish accepted
source entropy, managed Unity Catalog behavior, production authority, target
scale, or a full-run completion time. Preserve all Delta values, identity,
presence, multiplicity, protocol and validation obligations when considering a
future optimization. Predictive optimization remains enabled.
