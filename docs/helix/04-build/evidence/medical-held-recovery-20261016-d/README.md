# Original medical recovery: complete preflight, publication still pending

One actual local recovery used the exact original medical16b installation,
source/ACK scope, retained source/effect plans and request from
[the original attempt](../medical-writeahead-native-20261016-b/README.md).
It reused the installed runtime recorded in
[the preceding diagnostic](../medical-capture-preflight-20261016-c/README.md),
including the reviewed redundant identity-read removal now on main at30c22d42.
No fixture, source batch, installation, cloud compute or grants were created.
The dedicated source container was reused. Spark4.0.1/Delta4.0.0 ran local[1]
with512MiB driver memory. Bounds were native300s/whole360s/outer390s,
within the existing configuration limits; the previous recovery used240s native.

The supervisor returned terminal exit2 after309.420seconds with native-deadline.
This is not a successful recovery. Its71 retained stage records show the complete
opening and closing recovery preflight passing, including all three committed
ordinals, all six native targets, original source plan/state, source closing,
ACK scope and binding. The last record begins publication-apply; it has no end
record. The prefix locates the unfinished outer stage, not the precise internal
cause. Stage durations overlap and must not be summed as disjoint work.

Post-terminal observations found the same five committed journal operations
(two attempt records and three graph effects), one source plan and one effect
plan, and zero publication artifacts. A complete logical SQLite comparison with
the preceding recovery backup is identical, with integrity_check passing for
both backups. Current native log versions remain object_current2, edge_current2,
whole_source_history2, tombstone1, attempts3 and manifest1. These commit-log and
journal observations are not an independent full logical Delta read.

Ordinary source-reader and protected ACK opening/closing scope/signature/privilege
checks observed source head1, one batch and ACK0 with no receipt. No ACK was
submitted by the observer. Complete source/ACK publication correspondence remains
an unmet P04/P08 requirement. No subsequent native retry was made in this slice.
The next implementation investigation belongs inside publication application;
a standalone preflight repeat cannot advance this source.

The manifest hashes exact compressed and uncompressed operator/configuration,
terminal output, stage prefix, observers, WAL-aware SQLite backup and current
Delta JSON/Parquet snapshots. Runtime selection and its SDK-hash reuse limits
remain in the linked diagnostic. Evidence snapshots confer no runtime authority
and are not an automatic restore procedure. Independent review checked the
bounded invocation, exact original pins, observations and archived hashes.
