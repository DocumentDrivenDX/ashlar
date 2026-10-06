# Incremental validation and publication design candidate

Governed by [CONTRACT-003](../../contracts/CONTRACT-003-delta-graph-tables.md)
and [ADR-001](../../adr/ADR-001-delta-canonical-and-serving-layout.md).
This is a design candidate, not an admitted producer or publisher profile.
Unity Catalog Delta remains the selected storage architecture; UMF binding is
deferred. Latency and throughput measurements guide tuning.

## Validated baseline plus complete changes

A billion-node publisher cannot assume a fresh full-graph scan for every batch.
The proposed validation path starts from an admitted immutable predecessor
publication, a qualified complete input boundary and exclusive writer authority.
The predecessor's validation coverage is an input to the proof, not an inferred
property of whichever tables happen to exist. Bootstrap and periodic exhaustive
audits establish or renew that coverage. An unqualified predecessor cannot become
qualified merely because the next batch's changed rows pass.

Retain complete source envelopes and immutable stage membership before applying
the batch. Check delivery uniqueness, exact payload bytes/digests, native cursor
components and schema revision under the selected source profile. Property events
must preserve old/new presence separately from null, native entity/event versions
and stable event ordinal. Delta uniqueness is a publisher invariant, not an
automatic consequence of the DDL. Synthetic full-carrier input does not establish
native Truss transaction completeness or event interpretation.

Validate every changed canonical tuple against its retained intended output,
including exact JSON text, native identity, hash derivation and source reference.
The operation must have explicit expected create/update/delete membership and
preconditions; a stale version or unmatched expected update blocks publication.
Validate origin references and journal membership in the affected input set.
For structural changes, validate affected endpoint closure in the final graph,
including surviving incident edges for node deletion. Rebuild or update all
covered derived projections affected by those changes. Property-only changes
reuse structural versions only after proving their structural fields unchanged;
typed property projections still require invalidation when included.

Record actual committed versions, complete coverage, previous publication and
input/output binding in the receipt profile. Install the immutable manifest only
after those checks. Consumers pin its versions; intermediate latest-table writes
are not a publication. No acknowledgement or cursor advancement precedes durable
publication. Serialized statement-boundary recovery fixtures do not establish
concurrent fencing, ambiguous write recovery or real producer acknowledgement.

## Separate operational validation from experiment audits

The r89 experiment deliberately performs an exhaustive baseline/output comparison
over all 20M edges before publishing its synthetic slice. This detects changed
identity, unexpected row membership and unintended changes to every field. It is
experiment evidence, not a mandate to do that scan on each production batch.
The operational path must independently prove complete changes, authoritative
writer isolation and a validated predecessor before substituting incremental
checks. Measure that path separately; do not subtract experiment audit time and
declare freshness satisfied.

The large r86 baseline has no retained raw source envelopes. Therefore r89 can
qualify its changed-row origins and storage costs only. Its manifest explicitly
records unqualified baseline origins and does not promote itself to a complete
graph publication profile. A production bootstrap still needs retained source
coverage, complete structural extraction and source authority evidence.

## Next performance measurements

Measure full-carrier reads at the committed updated version before maintenance.
Scattered updates can affect many baseline deletion vectors and introduce broad
new-file statistics even when no unchanged rows are copied. Record exact files,
bytes, remote reads and engine/caller times; compare with the immutable baseline.
Only then evaluate targeted clustering maintenance and its storage/time cost.
Do not assume full OPTIMIZE is required or beneficial for every batch.

Sustained and burst admission needs scheduled arrivals, repeated publications,
queue growth, reader load and arrival-to-validated-manifest distributions. A
single 200k batch is neither p95 nor evidence of 10k/s sustained or 100k/s burst
capacity. Include raw, journal, stage, retained versions and maintenance in the
capacity model. Bound further scale from measured bytes and throughput rather
than compressed low-entropy carriers or arithmetic alone.

## Measured maintenance and validator findings

[r91](out/native/ashlar_entropy_maintenance_20261006_r91/audited-summary.json)
rewrites only the 333 MB update layer in about 13 s, preserving every canonical
field over 20M rows. [r92](out/native/ashlar_entropy_maintained_reads_20261006_r92/audited-summary.json)
reduces point-read files from 17 to 2. Candidate maintenance triggers should measure
unclustered update overlap, read amplification, queued bytes and retained-storage
cost. These findings favor evaluating routine incremental clustering before a
full-table cleanup, but do not establish a universal threshold or per-batch policy.

Maintenance creates a new Delta version. Held publication descriptors remain
immutable and keep their old versions. To serve the maintained layout, install
a new validated descriptor with the new canonical version, preserved source
progress and explicit logical-parity evidence. Reuse unchanged journal/raw/
structural projection versions only within their existing admitted coverage.
Retention still protects old descriptors. r92 tests version 2 directly; it does
not install that new descriptor or advance producer acknowledgement.

[r93](out/native/ashlar_entropy_changed_validation_20261006_r93/audited-summary.json)
proves exact affected-row parity and lexical property preservation with a narrow
native-key broadcast before the wide baseline join. It measures 9 s and 29 s for
those checks on the retained 200k-row batch, respectively. Explicit null differs
from absence; malformed JSON and a changed lexical token are detected. This
validates the query pattern within the fixture, not a complete operational proof.
A new batch must include actual staging, raw/history append, apply, validation
and manifest latency. Test large old/new journal tokens as well as the existing
common boolean addition; keep source authority and incomplete baseline origins
explicit.
