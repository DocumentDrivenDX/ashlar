# R02 local prepublication result

The independently reviewed one-shot attempt completed the configured-local `ashlar-delta/0.3` initialize, three sequential publication batches, and byte-identical batch-3 replay. The supervisor exited successfully and durably closed as `complete-held-after-publication`. The [machine-readable receipt](result.json) binds the attempt, manifest, Sol review, state, five operation receipts, output reports, and final four-role publication vector.

The replay retained checkpoint 5724, publication ID `local-configured-jsonl-b08fb1265de623cf0afed1e3052f9234cdecf06c2a50b5f3702a9e7c8c5975c3`, and table versions edge 5, object 3, tombstone 3, history 4, while reporting `replayed: true`. The final read-only Delta log inventory matched those versions. This supports the narrow no-new-visible-version replay conclusion; it is not a claim that a before/after log snapshot was taken during replay.

The supervisor ceilings were 300 seconds, 512 MiB private logical bytes, 16 MiB receipts, and 1 MiB per command output. The configured Spark runtime enforced `local[1]` and a `512m` driver (the packet template had stated `local[2]`/`2g`; that template/runtime mismatch is recorded here and was not the actual setting). Recorded logical attempt size was 300,017,708 bytes and receipts totaled 32,747 bytes. The filesystem timestamp envelope implies about 149.72 seconds; the runner did not retain a monotonic elapsed-time value or peak RSS, so neither is reported as a direct resource measurement.

This result is `.3` prepublication evidence only. No native reads, Weft compilation, SQL queries, protected ACK, cloud, or Unity Catalog operations occurred. The R05 Spark cohort carried Weft 0.4.48, while the selected Weft 0.4.53 continuation remains held pending a runner that captures fresh artifacts against live query aliases and an independent review gate before SQL.

## Traceability

- [R02 packet and frozen inputs](../r02-native-execution-packet-20261024/README.md)
- [R02 release requirement](../../../toolkit-delivery-plan.md)
- [Product requirements](../../../../01-frame/prd.md)
- [Publication boundary](../../../../02-design/contracts/CONTRACT-001-publication-boundary.md)
- [Consumer read boundary](../../../../02-design/contracts/CONTRACT-002-consumer-read-boundary.md)
- [Delta graph tables](../../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md)
- [Publication resolver](../../../../02-design/contracts/CONTRACT-004-publication-resolver.md)
