---
ddx:
  id: ashlar.evidence.r05-identity-range-layout-proposal-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-007
      kind: informed_by
    - id: TD-008
      kind: informed_by
    - id: ashlar.evidence.r05-redesign-napkin-math-20261029
      kind: informed_by
    - id: ashlar.evidence.r05-paired-writer-second-20261024
      kind: informed_by
    - id: ashlar.evidence.r05-storage-throughput-reassessment-20261023
      kind: informed_by
---

# R05 identity-range layout redesign candidate — 2026-10-31

**Sol review disposition: do not implement for this release.** Even the
optimistic case that reduces write time in proportion to active bytes leaves
the measured bundle at about 18.20 minutes before the second scan, point
reads, graph export, startup and cleanup. It cannot support the few-minute
target. A run would still exceed 15 minutes and require review, and the change
would replace the selected 64-bit lookup-hash profile with a new, unqualified
compatibility surface.
Retain this calculation as a rejected release optimization; do not run its
45M/225M discriminator without a new owner decision on profile and target.

## Proposal

Keep the 1B-node/5B-edge run held. The largest quantified per-row cost in the
current `.4-xxhash64` fixture is its stored hash accelerator: the 18-core
calibration measured about 8.0026 compressed bytes per row for `lookup_hash`,
or approximately 48.02 decimal GB at six billion rows. This is 70.6% of the
roughly 68.05 GB active-Parquet projection. A useful redesign candidate is a
separately versioned identity-range carrier with no persisted `lookup_hash`:
retain and filter by the full `(source_system, type_id/rel_type_id, id)` key,
store file statistics for those exact key fields, and write identity-ordered
files so Delta file min/max statistics can prune exact point reads.

This would be a new profile, provisionally `ashlar-delta/0.5-identity-range`;
it is not a change to `.4`, its UMF resources, or any existing publication.
UMF must author and generate the complete new carrier/DDL. The current reader,
writer and Weft query artifacts cannot be reused as-is: the `.4` native lookup
path computes and predicates on `lookup_hash`, while this proposal relies on
identity statistics. Any implementation must select the new profile
explicitly and retain the exact full-key predicate in SQL. `lookup_hash` is
not graph identity, so its removal from this new physical profile does not
change entity identity or edge multiplicity. It does change the declared
carrier and requires new schema, publication-vector, native singleton and
compiled-query evidence before support.

For the R05 deterministic `range` generator, IDs are already increasing and
Spark range partitions are contiguous. The candidate should preserve that
order inside each output file without adding a shuffle. For arbitrary
production sources, ordered identity files are not free: a bounded range
partition and local sort may require a full shuffle. If the producer cannot
prove ordered input, or the selected writer cannot preserve it, this layout
must not claim useful file pruning without measuring the sort/shuffle cost.
Delta clustering declarations alone do not prove files are physically
organized. A point-read witness must report the actual AddFile key ranges,
candidate files and bytes read against the same pinned table version.

## Quantified opportunity and limits

The current 80%-throughput write-only projection is 14.176 minutes. Scaling
the measured 48.02 GB hash column by the same row count gives a remaining
active-data size near 20.03 GB if all other column chunks stay unchanged.
If write time were purely proportional to active bytes at the measured
aggregate rate, the write-only sensitivity would be about 4.17 minutes. This
is an optimistic arithmetic sensitivity, not a forecast: Parquet compression,
per-row hash calculation, writer CPU, file geometry and local I/O do not have
one demonstrated bytes-per-second scaling law. The current profile's
28.206-minute setup/write/combined-witness projection would fall to about
18.20 minutes if only its write segment scaled by that byte ratio. The combined
witness may also benefit from a narrower row and no hash recomputation, but its
phase time is not separately measured. These figures therefore show a material
lever, not achievement of the owner's “few minutes” target; the second scan,
point reads, graph action, startup and cleanup remain unmeasured.

The measurement basis is synthetic and uses `source_position=id`; the selected
fixture's `source_delivery_id` is decimal text rather than an accepted opaque
source token. The hash byte estimate is fixture-specific, but `lookup_hash`
is a fixed-width BIGINT in the selected profile, so its row-linear storage
cost is less sensitive to payload entropy than the other fields. No target
scale bytes, runtime or latency were measured for the proposed profile.

## Focused discriminator if selected

Do not run the 1B/5B workload or a tuning matrix. After the owner accepts the
profile change and UMF produces the selected carrier, the smallest useful
comparison is one same-runtime sequential baseline/candidate pair over the
existing 45M-node/225M-edge generator, each on fresh tables. Preserve all
carrier values except the explicit absence of `lookup_hash`; candidate DDL
must put `source_system`, `type_id` or `rel_type_id`, and `id` in the Delta
data-skipping inventory. Keep the same Spark/Delta/JDK versions, cores,
partition count, ZSTD codec, row formulas, independent oracle and resource
limits. Capture the executed write plans, per-role time and output bytes;
check complete typed row bags after projecting away only the profile-specific
hash field; verify Delta schema/protocol and that point lookups retain exact
identity and actually prune files using key min/max statistics. Include the
same row/value and independent endpoint witnesses. A result only tests this
sample and mechanism; it cannot establish target-scale throughput.

Reject the design if either exact-key point read cannot prune on the candidate
layout, the writer needs an unaccounted large shuffle, the pair does not
meaningfully reduce both bytes and end-to-end sample time, complete row and
endpoint witnesses differ, or the generated UMF schema/protocol is not an
explicit new profile. Do not convert a small-pair win into a 1B/5B timing
claim. Revisit any full-scale plan only after the new profile, query artifact,
scan costs and GraphFrames action have independent evidence and an owner-reviewed
resource budget.

## Decision boundary

This is a design candidate for review, not a schema approval, implementation,
benchmark authorization or revised SLO. The existing owner decision remains
“Hold and redesign first.” The earlier R05 storage note said a compact
representation investigation was not required for that release; this proposal
does not override that decision. It identifies a new, quantitatively supported
physical-profile option for the owner to accept or reject before any code or
native attempt. Preserve `.4` semantics and receipts intact either way.
