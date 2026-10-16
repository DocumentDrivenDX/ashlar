# Medical recovery: completed preflight, effect-recovery deadline

The retained original medical publication recovery reached its native deadline
inside effect recovery. The installed command exited 2 after 307.672 seconds;
its native allowance remained 300 seconds, whole-worker allowance 360 seconds,
and outer process-group supervisor 390 seconds. The native worker was terminated
and reaped. No additional native attempt followed this deadline.

All recorded opening and closing preflight checks completed. The newer inner
diagnostics then measured publisher session acquisition at 0.399 seconds and
attempt observation at 0.630 seconds, followed by an unfinished `effect-recovery`
interval. The 76 stage records are contiguous. This narrows the next investigation
to native effect recovery; it does not identify which inner operation consumed
that interval or prove a successful publication.

Read-only inspection under the actual installation lease found the same five
committed operations, one ordered effect plan, one source plan, no publication
artifact, and identical Delta commit-log hashes/versions. The complete SQLite
logical dump equals its consistent before-image; SQLite 3.53.1 reports integrity
`ok`. Ordinary source and protected ACK observations also match: one original
batch at source head 1, ACK position 0, no ACK receipt. These observations do not
substitute for complete final Delta row admission or publication authority.

The source is the exact retained PostgreSQL container
`f862d55b64c078ffc4c2369ec64022b784b7a620385c9d74d75e2f99d4b73d91`,
restarted with its existing database, one CPU, 512 MiB and loopback port 25446.
Original installation, journal, source scope, model/graph/baseline, request bytes
and six target identities remain unchanged from
[the preceding recovery](../medical-held-recovery-20261016-d/README.md).
No fixture, batch, authority record or ACK was fabricated or initialized.

The fresh installed wheel is from main
`10f89dff3cecdff5e7505737580e4b565fa1ed3d`; all 410 installed Ashlar files are
hash-pinned. Selected Python 3.12.14, Spark 4.0.1/Delta 4.0.0, JDK 21 and the
exact Delta JARs use local[1] with a 512 MiB driver. SDK hashes were reused with
current namespace/metadata checks, not freshly qualified as a complete SDK or OS
closure. Bun 1.4.3 is explicitly pinned after the machine's older binary was
removed. The runtime accepts its 68,491,536-byte executable by streaming exact
hash checks within the unchanged 128 MiB aggregate allowance.

Startup failures remain separate from the actual native attempt. An earlier
runtime selection failed the obsolete 64 MiB selected-file bound before Spark.
The corrected installation then refused a working directory with mode `0755`,
before native phase entry and before creating output. Correcting that directory
to `0700` satisfied the existing private-output rule. The successful startup
attempt used distinct supervisor receipt and stream names; neither startup
failure is counted as a native or publication pass.

A separate bounded pure profile used the same three original input hashes,
fresh actual public dataset revalidation and one plan renewal. It completed in
6.987 seconds; the profiled renewal took 0.523 seconds. Eight Git subprocesses
inside producer pin checks accounted for 0.447 seconds; graph SQL planning took
0.014 seconds. Its outbox checkpoint was constructed only for pure planning and
confers no source or ACK authority. This identifies duplicated producer checks
as an optimization candidate; it does not attribute the entire native timeout
or demonstrate an end-to-end speedup. No Spark or cloud compute was used for
that profile.

After the duplicate renewal was removed, a second bounded pure profile used
checkout commit `8892a9225ead3dc1a4b0ee8699179e5fba81cea3`, with recorded source
file hashes and the same original input hashes. One renewal took 0.276 seconds,
with two producer renewals and four Git subprocesses; the workflow including
fresh public revalidation took 4.288 seconds. Both plan admission metadata gates
remain active. These are individual local observations with uncontrolled cache
state. The earlier profile did not retain a plan digest, so exact before/after
plan equality is not proved by these profiles. Focused semantic and drift tests
cover the code change separately. The optimized code has not rerun native recovery.

manifest.json pins 136 exact compressed records, uncompressed sizes and hashes,
including configuration/runtime selection, operator scripts, terminal receipts,
before/after observations, all stage files, original request, public revalidation
and the pure profile. Independent review verified the installed files, original
pins, terminal failure, stage sequence and unchanged retained state. No Databricks
compute or paid graph capacity was used. Medical publication and durable ACK
remain unfinished.
