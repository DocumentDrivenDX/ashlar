# R05 storage and throughput reassessment (2026-10-23)

The latest measured local `.4` synthetic sample is the 18-core calibration at
45M nodes and 225M edges. It wrote 270M rows totaling exactly 3,062,253,112
active Parquet bytes. Row-linear arithmetic gives about 68.05 decimal GB for the
1B-node/5B-edge target. This is an arithmetic extrapolation for this exact
synthetic carrier, not a full-size run, production capacity estimate, or total
disk requirement.

At 80% of the measured per-role write throughput, the same packet projects
1,082.8 seconds (about 18.05 minutes) for writes only. The estimate remains
above the 15-minute review threshold, so the full run stays held. Retain the
historical 1M/5M measurement and its evidence as history; this later measurement
supersedes its throughput projection for current planning.

The owner's 2026-10-23 release decision is to retain full Delta semantics and
use the measured approximately 68.05 GB active-Parquet envelope for this
synthetic target. No compact representation investigation is required for this
release. This is not a production capacity claim and does not authorize the
held full-size run.

The measured fixture stores approximately 8.003 compressed bytes per row for
`lookup_hash`, which row-linear arithmetic scales to about 48.02 GB at six
billion rows. The fixture also set `source_position = id`, measuring about 1.02
compressed bytes per row and scaling to about 6.12 GB. This is a synthetic
fixture property, not a production defect or universal lower bound. The actual
Truss-to-carrier mapping leaves `source_position` null when the source does not
supply a position; it preserves `(xid, seq)` in source cursor/record/progress
custody and does not copy entity ID into that field. The potential 6.12 GB
difference is therefore conditional on a future admitted source actually
providing positions and must not be claimed as a production saving. The
remaining roughly 13.9 GB in the linear estimate comes from other fixture
columns and compression behavior.

The estimate excludes realistic opaque delivery IDs and source payloads,
history, tombstones, Delta logs, staging, retained versions, temporary space,
validation, publication, retries and graph-engine exports. Bytes per row can
change with values, file geometry, runtime and layout. No full 1B/5B run,
publication, ACK, Unity Catalog operation or graph-engine execution is qualified
by this packet.

The separate [paired integer-DIV experiment](../edge-integer-div-paired-20261023/README.md)
does not support promotion: the candidate edge write was 3.12% slower, and the
sample harness did not capture the INSERT physical plan. Its narrow tiny
equivalence probe is not a scale-performance qualification. No carrier contract
or production representation is changed by this reassessment.

## Bounded edge-write attribution follow-up

A separate local edge-only repeat materialized 225M `.4` edges to 18 ZSTD
files in 26.354 seconds and wrote 2,569,031,961 active bytes, matching the
prior edge-table bytes. Event attribution ties the materialization to an
18-task stage spanning 25.485 seconds (96.7% of the INSERT wall interval).
Those tasks summed 424.703 task-seconds, 310.684 executor CPU-seconds and
11.277 task-seconds of JVM GC; task durations ranged from 21.763 to 25.466
seconds. The stage used 310.684 CPU-seconds to write 225M rows. These figures
are local synthetic observations, not a production rate or proof that CPU,
compression, expression evaluation or IO is the limiting resource. The 0.869
seconds outside the main stage span is residual INSERT overhead, not isolated
Delta commit time. See the [attempt-03 receipt](../edge-write-attribution-20261023/attempt-03/experiment.json)
and [retained event attribution](../edge-write-attribution-20261023/attempt-03/spark-attribution-reparsed.json).

The selected synthetic target remains approximately 68.050 GB of active
Parquet. At the current 80%-throughput planning curve, the combined 1B-node /
assumed 5B-edge write remains about 1,082.8 seconds (18.05 minutes), above the
15-minute review gate and held. A five-minute total is only an illustrative
sensitivity: it requires about 3.61x the weighted throughput of that curve. If
node time stayed at the projected 268.877 seconds, only 31.123 seconds remain
for edges, requiring about 26.15x the projected edge throughput. Separately,
linear extrapolation of the observed edge-stage CPU work gives about 6,904 CPU
seconds for 5B edges, or 383.6 seconds at ideal use of 18 cores. This is
arithmetic on the unchanged work profile, not a lower bound after an algorithm
or hardware change and not a measured full-size result. The production target
and full-run acceptance requirement are unchanged; no 1B/5B trial passed or
was authorized by this diagnostic.

The next diagnostic is the [prepared attempt-04 JFR window](../edge-write-attribution-20261023/jfr-attempt-04-plan.md).
It is unexecuted and requires independent review plus a free local resource
slot; it profiles the same INSERT interval with sampled CPU/allocation events.
Its output can narrow hotspot hypotheses but cannot establish performance
without a full qualified workload, and profiling overhead precludes treating
its elapsed time as an unprofiled speed result.
