# Medical WAL-aware journal reconciliation

A read-only SQLite transaction under the actual existing medical installation
lease observes five committed operations and zero publication artifacts. The
journal is in WAL mode. Immutable main-file inspection misses a committed
operation; it is insufficient to decide current logical recovery state.

The original installation handoff is checked by its selected SHA-256 and byte
length. Each of the five stored receipts matches its Delta log commit's exact
userMetadata, native operation, operation digest, installation ID, table UUID and
version. Stored receipt snapshot row digests are internally valid. Receipt rows
are 51 objects, 62 edges, 113 history entries and two successive attempt snapshots.
These are recorded receipt counts, not fresh native snapshots. The corrected
observation completed in 0.083 seconds, with SQLite closed before lease renewal
and release. Row-envelope hashes match the earlier observation.

This renews local journal/commit correspondence only. It opens no source or Spark,
performs no recovery, publication or ACK, and does not prove current source
privileges, Parquet readability, full effect parity or completion. The private
full result retains original receipt payloads; its digest/path/size are recorded
without copying medical sample content into this evidence.

The existing recovery preflight separately opens and closes four historical prior
bags and six current snapshots, then checks committed ordinals and runtime custody.
That repeated work is visible in code, but the prior 300-second timeout has no
stage timing to establish its cause. The next action is a bounded preflight-only
diagnostic with fixed stage timings, preserving source/installation authority and
all opening/closing checks. Publication and protected ACK remain pending.
