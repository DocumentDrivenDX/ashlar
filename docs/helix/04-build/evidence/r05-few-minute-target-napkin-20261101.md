---
ddx:
  id: ashlar.evidence.r05-few-minute-target-napkin-20261101
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
      kind: informed_by
    - id: ashlar.evidence.r05-release-scale-review-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-72partition-run-reconciliation-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-production-hardware-napkin-20261031
      kind: informed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
---

# R05 few-minute target: measured gap and one screen — 2026-11-01

## Decision

Keep the 1B-node/assumed-5B-edge attempt held. “A few minutes” remains the
existing provisional five-minute design reference, not a newly approved
service-level objective. The 72-partition candidate improved one local write
sample, but its 80%-rate projection for setup, writes and one combined
row/value/endpoint witness is 22.437 minutes. That already exceeds the
15-minute owner-review trigger before separate scans, point reads, graph action,
startup and cleanup. The measurement does not qualify local or production
target-scale performance.

The analysis uses the one completed 45M/225M candidate and retained paired
writer evidence only. It did not run Spark or scale work, rerun the baseline,
or select cloud resources.

## What the sample supports

| Local sequential sample | Write wall | Setup/write/compact-witness interval | Combined table bytes |
| --- | ---: | ---: | ---: |
| Retained 18-partition baseline | 35.227 s | 66.278 s | 3,041,334,560 B |
| One 72-partition candidate | 28.306671 s | 48.464554 s | 3,116,356,172 B |

The 72-partition run reduced write wall by 19.64% and increased measured
throughput by 24.45% relative to the retained sequential sample. Its row/value
and endpoint witness passed for 45M nodes and 225M edges; the two write stages
each had 72 tasks and zero spill. Its table bytes were 2.47% higher. This is
one local sample, not a stable tuning result or a causal diagnosis. See the
[run reconciliation](r05-72partition-discriminator-20261101/r05-72partition-run-reconciliation-20261101.md)
for row, stage, runtime and supervisor receipts. The supervisor outcome remains
held because its global event-attribution gate falsely rejected those tagged
write stages; owned cleanup is pending Sol review.

Scaling the candidate's measured bytes by `1B / 45M = 22.2222` gives an
approximate 69.25 GB of active Parquet at 1B/5B: 10.54 GB for nodes and
58.71 GB for edges. This synthetic row-linear estimate is 2.47% above the
retained baseline estimate and excludes logs, staging, retained versions,
shuffle, spill and graph-engine working data.

At the candidate's measured rate, writer-only arithmetic is 13.105 minutes at
80% of sample throughput. The measured setup/write/compact-witness interval
projects to 22.437 minutes at that same planning rate. The writer-only number
does not include the required graph and validation path; the measured interval
includes one combined persisted witness but not the full release workload.
Neither is a target-scale completion estimate. The 22.437-minute bundle needs
4.49x speedup to fit five minutes, even before the remaining phases.

Under ideal inverse-linear core scaling, that measured bundle would require
about 81 cores to fit five minutes by itself (`18 × 22.437 / 5`). A 256-core
candidate would reduce it arithmetically to 1.578 minutes and leave 3.422
minutes for omitted work. This is a sizing sensitivity only: the sample used
18 local cores, and neither 81-core nor 256-core efficiency is measured. The
local result therefore does not establish either a few-minute local run or a
few-minute production run.

## Bytes and CPU required by the target

The candidate's active-byte estimate requires about 231 MB/s of sustained
useful output to write once in 300 seconds. The local candidate wrote about
110 MB/s, so the target needs 2.10x its observed writer rate (2.62x its
80%-discounted planning rate) before setup or validation.

The full-path bytes depend on how much each scan and the graph action actually
read. A conservative sensitivity using the measured table sizes is:

| Pass assumption | Target useful bytes | Required average in 300 s |
| --- | ---: | ---: |
| Write + row/value scan of both tables + endpoint scan of edges + one full graph input pass | 266.47 GB | 888 MB/s |
| Write + one combined row/value/endpoint pass + one full graph input pass | 207.76 GB | 693 MB/s |

The second row assumes the endpoint checks share a persisted scan with the
row/value checks. The 72-partition candidate already demonstrated that
combined witness on the 45M/225M synthetic tables. It is a design sensitivity,
not permission to weaken independent validation: the R05 full-path contract
still calls for two separate persisted scans. The GraphFrames action's actual
input bytes are unmeasured, so these are planning bounds rather than measured
requirements. If only 80% of admitted capacity is useful to this workload, the
first sensitivity implies about 1.11 GB/s of admitted sustained useful
read/write capacity.

The 72-task node and edge write stages consumed 88.154 and 326.413 executor
CPU-seconds for the sample. Linear row scaling implies about 9,213 executor
CPU-seconds for writes alone at 1B/5B: at least 30.7 continuously busy cores
for five minutes, or about 38.4 allocated cores at 80% utilization. The
combined persisted witness used about 139.816 executor CPU-seconds locally;
row scaling adds roughly 3,107 CPU-seconds for one such pass. Writes plus one
combined pass therefore imply about 51 cores at 80% utilization over five
minutes, before the separate required scan or GraphFrames work. These CPU
figures are linear sensitivities from one synthetic sample. They do not
establish CPU saturation, useful scale-out, or graph-action cost.

The measured mechanism remains O(N+E): compute row identities, 64-bit hashes
and endpoint formulas, encode the `.4-xxhash64` carrier with ZSTD, then read
persisted data for validation and graph processing. The 72-partition result
shows that larger task granularity improved this local writer sample, but
stage CPU, task times and zero spill do not identify CPU, compression, storage
or partition overhead as the unique limit. Repeated full-data passes and
row-wise work dominate the arithmetic; target-scale parallel efficiency and
actual scan bytes remain open.

## One reversible next experiment

After the owner selects and admits a dedicated compute shape, run one
1M-node/5M-edge full-path screen on the proposed 256-executor-core candidate,
using 256 node partitions and 1,024 edge partitions. Keep the schema, carrier,
values, ZSTD, point predicates, both independent persisted scans, pinned-frame
GraphFrames action and all semantic witnesses unchanged. Capture per-phase
wall time and bytes, stage task counts and duration spread, executor CPU/GC,
spill, peak memory, output files/bytes and verified owned cleanup. Use one
attempt, no retry, a 900-second wall cap and 32 GiB attempt-root cap; clean the
private output only after the existing ownership and process-quiescence checks.
This changes only runtime task geometry and is reversible by discarding the
private run.

The screen is 1/1,000 of the target row volume. A linear five-minute budget
allows at most 0.300 seconds per 1M/5M workload unit before fixed overhead; at
the 80%-rate planning factor, the measured variable work would need to fit in
0.240 seconds per unit. Treat exceeding that arithmetic as an early rejection
of this compute candidate. Passing only makes it eligible for a larger
reviewed pilot: it cannot prove 1B/5B scaling, 40–55 MiB target partition
geometry, memory sufficiency, Unity Catalog behavior or GraphFrames throughput.
The 256-core shape, storage path and cost limit have not been admitted, so this
screen is a recommendation, not an authorization to provision or run compute.
