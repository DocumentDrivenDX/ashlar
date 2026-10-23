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
