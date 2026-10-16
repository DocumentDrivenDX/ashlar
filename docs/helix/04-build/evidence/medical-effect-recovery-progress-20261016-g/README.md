# Medical recovery advances to durable committing custody

The installed recovery command advanced the exact retained medical publication
from `applying` through `applied` to `committing`, then reached its native
deadline. The supervised process exited 2 after 305.001 seconds. Native time
remained bounded to 300 seconds, whole-worker time to 360 seconds and the
process-group supervisor to 390 seconds. No final publication or durable ACK
success is claimed.

All 77 stage records are contiguous. Opening and closing preflight checks
completed, followed by publisher session acquisition (0.338 seconds), attempt
observation (0.485 seconds), and completed effect recovery (112.862 seconds).
The enclosing publication application did not complete. These measurements do
not identify the exact operation active at termination.

The journal advanced from five to seven committed operations. Two new attempt
operations have native receipts at versions 4 and 5. Independent read-only
DuckDB decoding of the exact active Parquet files selected by Delta log version
5 found the four native phase rows: `prepared`, `applying`, `applied` and
`committing`. Original request correspondence remains retained; the last two
phases include their result carrier. The journal now retains one 48,992-byte
publication artifact. That proposed artifact is not a committed manifest or an
ACK receipt. The manifest table remains at version 1, with no native publication
commit. Object, edge and whole-source-history targets remain at version 2;
tombstone remains at version 1. Existing commit-log hashes are unchanged.

The consistent read-only post-run SQLite backup passed integrity checking under
SQLite 3.53.1 and matched the current logical journal dump. It retains the actual
artifact and complete operation/source/effect custody. Backups and observations
are evidence, not replacement runtime grants or a reconstruction procedure.
Ordinary source and protected ACK observations closed successfully: one original
batch at source head 1, ACK position 0, no ACK receipt.

The fresh installed wheel came from main
`08d0aaee3eb4685a7c06702c668c87af829f9d03`; all 410 installed Ashlar file hashes
and their exact wheel correspondence were independently checked before launch.
This selects the reviewed duplicate-producer-renewal removal. Selected runtime
pins, original model/graph/baseline, installation, journal, request bytes and six
native target identities remain unchanged from
[the preceding attempt](../medical-effect-recovery-deadline-20261016-f/README.md),
except for the explicitly new installed runtime and private output selection.
Python 3.12.14, Spark 4.0.1/Delta 4.0.0, JDK 21 and Bun 1.4.3 run locally with
local[1] and a 512 MiB driver. SDK hashes were reused with current
namespace/metadata checks; no fresh whole-SDK or OS closure is asserted.
The existing original PostgreSQL source/database was reused. No fixture,
batch, authority identity, cloud capacity or source ACK was initialized.

Any continuation must preserve the actual retained `committing` custody. No
manifest operation intent exists in the journal. The current manifest-recovery
port requires such an original intent, so this phase advance alone does not
establish that another unchanged invocation can proceed. Resolve that boundary
before another native run, while preserving complete current admission and
original custody. Actual manifest publication and protected durable ACK remain
unfinished.

manifest.json covers 117 exact compressed records and their raw lengths/hashes:
installed/runtime selection, original configuration, supervised terminal output,
all stages, before/after source and journal observations, both consistent journal
backups, proposed-artifact fingerprints, active attempt carriers and native
attempt Delta commits. No bearer tokens or passwords are included. This is one
bounded local recovery observation, not a throughput benchmark or a cloud result.
