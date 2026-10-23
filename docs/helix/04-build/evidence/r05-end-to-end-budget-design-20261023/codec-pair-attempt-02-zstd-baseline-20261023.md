---
ddx:
  id: ashlar.evidence.r05-codec-pair-attempt-02-zstd-baseline-20261023
  type: evidence
  activity: build
  status: recorded
  authoring:
    home: repo
  links:
  - id: ashlar.evidence.r05-codec-snappy-edge-pair-plan-20261023
    kind: informed_by
  - id: TD-007
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
---

# R05 codec-pair attempt 02: retained ZSTD baseline

**Outcome:** The ZSTD Spark/Delta case completed its 225,000,000-row INSERT and semantic readbacks. The child returned exit 0 and the Spark stop stage was recorded. The supervisor rejected the case because `killpg(pgid, 0)` returned `PermissionError` after leader exit and it could not establish process-group absence. This is a process-cleanup evidence gap, not a codec or Delta failure. No SNAPPY case ran.

The retained attempt root is `/private/tmp/ashlar-r05-codec-pair-20261023-02/zstd`. Its progress, runner log, supervisor receipt, and Delta-log hashes remain preserved. The candidate-only continuation removed only this root's owned warehouse after hashing its Delta-log files against progress; see the separate retention receipt in the [attempt-03 root](codec-pair-attempt-03-snappy-output-cap-20261023.md). The successful runner receipts record Spark 4.2.0, Delta 4.4.1, Python 3.12.14, JDK 21.0.13.0.2, `local[18]`, and a 24g driver. INSERT time was 25.962255 seconds. Footer verification found 225,000,000 rows, 18 ZSTD files, and 2,569,031,961 active Parquet bytes. The canonical schema hash was `bf6657de5fd7839e327c10e94b155dc2faa9e302c741cc351fd1bfa97ae59ce1`; the Delta metadata schema hash was `eb17bbf49525de56c8faac2f9005541f711119949db30441ccb087c715eede00`. Protocol readback was reader 1/writer 7 with `domainMetadata`, `clustering`, `invariants`, and `appendOnly`; table configuration and five deterministic persisted-row witnesses are bound by the progress-file hash below. These observations do not make the supervisor outcome a valid pair result.

The supervisor receipt records exit 0, no resource limit, 2,592,150,683 output bytes and 3,017,163 diagnostic bytes. It records leader reaped, SIGTERM sent, `PermissionError: [Errno 1] Operation not permitted` during process-group probing, and `remaining_process_group_uncertain=true`. The historical receipt did not preserve the child PGID, so this packet does not infer one. The parent task subsequently reported a successful independent read-only `ps` inventory with no Spark/Java child. That inventory is provenance for the continuation admission only; it does not rewrite the retained supervisor receipt.

| Retained file | SHA-256 |
| --- | --- |
| `out/progress.jsonl` | `1a74ba8ace61f9bc61fd8adf7830dbb175fe4545afa7c4766b9c6455fc847338` |
| `runner.log` | `b18b42323f9609389b7f7ba64b8a5154ae4e8eba21550819c11af6cab433d653` |
| `supervisor-result.json` | `811f42525572f4d4e7f4f6b27b044d533d87d8a6e45d36f47c8c569f244c5bfe` |

The candidate-only continuation was launched once beneath `/private/tmp/ashlar-r05-codec-pair-20261023-03`; it never invoked the ZSTD case. Before SNAPPY launch it verified these receipt hashes and key semantic fields, required a successful independent process inventory with no Spark/experiment process, verified the exact owned attempt-02 table path, hashed the Delta log files against the retained progress receipt, wrote a separate retention receipt, and removed only `/private/tmp/ashlar-r05-codec-pair-20261023-02/zstd/out/warehouse`. The baseline progress, runner log, supervisor receipt, and Delta-log hashes remain. The candidate then stopped at its output threshold before INSERT completion; because the candidate case did not return, the continuation did not perform its planned post-candidate baseline re-hash. The candidate supervisor receipt records a reaped leader and empty process-group inventory. See the [attempt-03 failure record](codec-pair-attempt-03-snappy-output-cap-20261023.md) and updated [codec plan](codec-snappy-edge-pair-plan-20261023.md).
