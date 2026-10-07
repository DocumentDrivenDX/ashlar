# Native commit-bound closing — R556–558

The R553 wrapper now executes natively against the ten accepted sixth-batch
base/input tables, using four independent persistent SQL clients. Expected
mutable heads bind the actual R537 statement IDs and exact single-commit
intervals. The selected node version remains6 while physical head8 is checked.
UUID, full schema, protocol, properties, partitions, clustering and table features
remain exact. Neither base data nor a manifest is written in this iteration.

Two consecutive complete metadata cohorts took4.519597417 and3.016591250 seconds.
Aggregate engine execution was4077/3874ms; aggregate caller time was16098.552/
9927.511ms. Full setup/collection/final telemetry took18.313919208 seconds.
All68 native statements are exact-text matched, successful and FINISHED with
is_final metrics. Reported data read/write/spill bytes are allzero, within10MB/
0/0 and180-second bounds. This does not establish zero catalog I/O or zero cost.
Runtime and query metrics are retained in shared-history.json; compute was the
existing authorized warehouse, with no configuration change.

The before_descriptor/after_readback phase names test durable publisher call-site
labels only: no new descriptor or new full publisher ran. The independent audit
reconstructs expectations from completed R537 receipts rather than trusting the
new runner's expected vector, then matches all native responses and cost totals.

Integration readiness is now stronger: both offline binding/refusal controls and
native wrapper execution pass. A future full publisher can replace its serial
closing-profile loop with close_after while retaining separate closed commit
interval checks, atomic full-field predecessor guards, independent content
checks, publication ordering/readback and final budget telemetry. Historical
R537 is immutable and must never be replayed.

Performance admission remains open. Do not subtract these component times from
the221.83-second historical publication and report an observed new clock. The
64.68-second current MERGE dominates and already exceeds the60-second target.
The next material experiment should target that mutation's metadata/file-access
cost with bounded read-only plan/metrics evidence before another full batch;
another unchanged publication would add cost without qualifying a new policy.
Warm singleton caller/engine budgets, actual cold-data repeatability, sustained
10k/s,100k/s burst and1B/5B remain unproved. Canonical UC Delta and complete Truss
semantics remain selected; consumer limits and UMF deferral are unchanged.
