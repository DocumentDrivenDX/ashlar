# B offline runtime attempt 01 interruption record

Status: incomplete, unreceipted offline preparation. This record is not a
runtime receipt, execution approval, source approval, or Truss/native result.
Preserve the attempt-01 venv as quarantined evidence; do not reuse or delete it.

## Exact source custody copies

The following copies were written before runtime creation and read back byte for
byte. Both files were current-user-owned regular files with mode `0600`; their
parent directory was created private. This records custody only and grants no
source authority.

| Input | Private copy | Bytes | SHA-256 | Mode |
| --- | --- | ---: | --- | --- |
| Retained UMF source | `/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/source.umf.json` | 4,100 | `498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b` | `0600` |
| B origin candidate | `/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/b-origin.json` | 141 | `2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247` | `0600` |

The origin copy remains unapproved. The source copy was not altered or
reserialized. No acceptance, catalog write, consumer registration, feed read,
destination, or ACK occurred.

## Interrupted runtime attempt

The selected first path
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv` was created and
the offline 27-wheel pip install was started with `--no-index --no-deps` and a
600-second subprocess timeout. The session was interrupted when Arras announced
a new SDK generator slot. The orchestration returned a `KeyboardInterrupt`
from `subprocess.run(...).communicate()` before it wrote an install log or
receipt; `pip check` and installed RECORD verification were not reached. The
venv path exists and is incomplete or otherwise unverified. It is quarantined
and must not be reused as a runtime.

An attempt to inventory processes with `ps` returned `operation not permitted`
from the sandbox. The child pip return code and process disappearance were not
independently observed, so this note does not claim a confirmed child kill or
reap. No further process or package inspection was performed during Arras's
new slot.

The fresh candidate path is
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv-attempt-02`, with
receipt candidate
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/runtime-receipt-attempt-02.json`.
It is not created. Resume only after Arras reports terminal cleanup, using the
exact pinned R4 lock. No B output root or PGDATA exists. The config retains
`execution.approval: null`, `origin.approved: false`, and unresolved slot,
source authority, and retention-owner decisions.
