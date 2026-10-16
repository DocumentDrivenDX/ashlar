# Bounded medical recovery preflight and identity-query deduplication

Two redundant native identity queries were removed without caching authority.
Bounded snapshot capture retains its opening identity query and the public
wrapper's fresh all-exit closing query after local sorting/serialization. Committed
receipt inspection retains both independent histories, both full snapshots,
current admissions/profile checks and final journal comparison; it relies on the
first snapshot's actual closing identity query instead of repeating it after pure
local proof comparison. Actual bounded, unbounded and memo snapshot paths all
retain that closing check. No source, ACK or observation boundary was removed.

Forty-five focused verification/committed-inspection tests passed, including
exact boundary calls, changed closing identity, cancellation with independent
closing failure, two fresh histories/snapshots/admissions and unchanged journal.
Module boundaries pass. Independent review approved the exact removals and
candidate correspondence before one native read-only diagnostic.

A fresh installed candidate wheel based on fb02885c used the retained medical16b
installation, original source/effect plans, original request and native tables
from [the recovery attempt](../medical-writeahead-recovery-20261016-b/README.md).
No new fixture, effect submission, manifest publication or ACK was requested.
The selected runtime remains Spark4.0.1/Delta4.0.0, Python3.12.14, local[1]/512MiB;
SDK hash reuse with current metadata is explicitly qualified, not a fresh complete
payload rehash. Wheel and candidate-source hashes are retained in wheel.json.

The installed preflight command returned terminal exit2/native-deadline in
68.151 seconds with a60s native/150s whole/180s outer limit. Thirty-three fixed,
payload-free stage records were retained. Source admission completed in3.453s,
source-plan validation in5.492s (including nested prior captures), and ordinal
state in5.238s. All six opening native-target observations completed in1.449–2.195s
each. Committed-ordinal0 and1 completed in8.288s and8.241s; the last record is the
begin for ordinal2. Nested durations overlap and must not be summed as disjoint
work. This prefix identifies reached/completed stages, not an exact root cause or
proof that closing ran. No preflight success or performance percentile is claimed.

Read-only WAL-aware logical SQLite comparison against the retained preflight
backup is exactly equal, including all original intents/plans/receipts. The
selected SQLite3.53.1 runtime read the complete backup as immutable (it is already
a finished standalone backup); the live journal used ordinary read-only WAL
semantics. Ordinary reader/ACK opening/closing checks still report source head1,
one batch, protected ACK0 and no receipt. Their scope/signature/privilege checks
completed without writes. No further native attempt followed this diagnostic.

manifest.json retains exact raw/compressed SHA256 for configuration, runtime
selection, native status, original request, all stage records and read-only
observations. Historical operator paths are not an installed deployment interface.
The original journal/tables/source remain available. Publication and source ACK
completion are still required; this diagnostic does not replace them.
