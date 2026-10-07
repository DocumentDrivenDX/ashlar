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

## Actual large-token batch and independent serialization oracle

[r94](out/native/ashlar_entropy_wide_journal_20261006_r94/audited-summary.json)
publishes 100k present-to-present large-token changes with affected-row checks
in 78 s on the existing compute. It adds 639 MB journal data versus 2.54 MB for the
earlier 200k boolean events. Membership is an existing hot set, which touches
four old update files; this does not qualify graph-wide scattered batches or
sustained/burst rates. Keep input width, affected file count and retention explicit
when choosing batch size, maintenance and resource bounds.

Comparing captured payload with the same serialization expression is insufficient.
The independent r95 oracle detects millisecond truncation of derived published_at
in r89/r94 wire while native Delta values remain exact. A source encoding profile
MUST name its timestamp representation and independently verify roundtrip instants
and token text, including nonzero sub-millisecond cases. The corrected synthetic
wire uses explicit UTC microseconds; old raw input is immutable and remains
qualified as lossy in that metadata field. Never overwrite a delivered payload
under its original origin ID to make a reconstruction test pass.

The large-token patcher is qualified for known compact, unescaped ASCII-hex
values only. Escaped/pretty lexical forms are rejected, not silently decoded
into a narrower source claim. A general native property-event adapter still
requires the actual producer token semantics, full input completeness and
writer/acknowledgement authority.

## Maintenance and publication version binding

Native background maintenance advanced current version 3 to 6, and raw/journal
version 2 to 4, before the r96 schedule. Recorded commits are OPTIMIZE operations;
the service-account identity alone does not establish the maintenance scheduler.
The current-table maintenance includes compaction and deletion-vector cleanup.
An assumed next version number would bind an incorrect publication vector.

Capture the actual predecessor and post-write versions, verify intervening
lineage, and bind immutable versions in the manifest. The synthetic serialized
harness allows only recorded OPTIMIZE operations alongside its one expected
MERGE and checks exact affected outputs. That is a test guard, not an exclusive
writer fence or proof that real concurrent business writers are safe. Raw and
journal origins remain exact and independently checked at their captured versions.

## Raw validation query tuning

[r97 persistent uncached comparison](out/native/ashlar_raw_validation_20261007_r97/persistent-uncached/audited-summary.json)
tests the same 100k-member immutable r96-b3 input/raw version7. Combining payload
parity and membership into one full-outer aggregate is not a measured latency
win: separate caller totals are 5.67/6.24 s versus combined 5.78/6.16 s; combined
engine time is higher in both pairs. Membership reads only about 7.54 MB of narrow
columns, so eliminating that scan saves little beside the wide payload check.
The full-outer candidate detects extra/missing origins; cardinality detects a
duplicate even when payload mismatch count is zero. Valid, missing, duplicate,
extra-origin and independently timestamp-corrupted controls behave as expected.
The fixture gate uses globally distinct native IDs; a general multi-source gate
must count complete semantic tuples rather than assume globally distinct IDs.
This experiment does not cover every outer-envelope metadata column or establish
a production source adapter. An earlier REST repeat returned cached results;
those timings are explicitly excluded and retained as a transport qualification.

[r98 batch predicate comparison](out/native/ashlar_raw_batch_filter_20261007_r98/audited-summary.json)
isolates a raw apply_batch_id restriction with identical immutable snapshots and
the same payload/cursor/digest/independent timestamp checks. All exact final query
metrics are uncached; all data reads are from the warm data cache. Caller times
are unfiltered/filtered 15.14/5.18 s and 6.49/4.81 s in alternating pairs. Read
bytes, including input, are 3.64/4.03 GB unfiltered versus 1.33 GB filtered.
Files/rows scanned vary across unfiltered executions; this is a bounded query
measurement, not a universal speedup or a cold-read benchmark.

Use the batch boundary to scope expensive raw payload validation after proving
membership and preserving global origin uniqueness under qualified writer
authority. A batch filter alone cannot discover a conflicting origin stored
under another batch, and this synthetic history does not qualify that authority.
Keep exact membership checks, predecessor coverage and bootstrap/periodic global
audits. Apply the predicate in the next real synthetic publication timing;
do not subtract this isolated saving from r96 and claim a new freshness result.

[r99 actual publication](out/native/ashlar_filtered_publication_20261007_r99/audited-summary.json)
applies the batch predicate with a persistent uncached session and extra envelope
metadata checks. All three publications pass; one terminal metadata-column read
failure is recovered before canonical apply without repeating writes. The first
batch includes recovery and cannot substantiate clean latency; the remaining
full processing samples are 61.62/66.47 s. No freshness p95 or sustained-rate
admission follows. Raw capture and journal append remain serial 12–13 s phases.

The native journal's explicit data-skipping statistics omit apply_batch_id.
Its batch-filtered equality checks read 68/86/100 files with no file pruning as
retained history grows. Test adding the batch column to statistics and recomputing
existing file statistics as a physical tuning candidate, preserving logical
columns, source-feed/cursor semantics and publication versions. Track the
maintenance commit and actual before/after pruning, bytes and parity timings;
do not assume metadata configuration alone retroactively indexes existing files
or that mixed-batch compaction guarantees perfect pruning. Only a new full-path
run can qualify any resulting publication latency.
