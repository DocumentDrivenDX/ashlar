# Full intermediate mixed-publication audit candidate

Governing artifacts: [CONTRACT-003](../../contracts/CONTRACT-003-delta-graph-tables.md),
[ADR-001](../../adr/ADR-001-delta-canonical-and-serving-layout.md).
This is a synthetic spike execution design, not a production producer profile.
UMF binding remains deferred. Native execution of this plan is pending.

The target is the complete 8M-node/40M-edge baseline, followed by 100k distinct
changes: 90k updates and 10k deletions. Input is independently generated locally
and transported byte-exactly to the owned Unity Catalog volume. All four input
roles must pass their complete original-input and typed-field digests, raw payload
SHA, unique identities/event ordinals, version2, exact delivery/cursor links and
exhaustive disjoint update/deletion partition before any publisher mutation.
Pin each input by table UUID and creation statement's actual Delta version.

Before applying changes, compare all20 predecessor fields of all100k selected
edges against the qualified baseline snapshot. Reject missing or different
predecessors. Use private shallow clones of qualified baseline role versions;
recheck their UUIDs and maintenance settings. These clones are experimental
publisher isolation, not a concurrent-writer fence or retained-storage inventory.
Keep the nodes at their qualified version. Name every append target column.
Tombstones include entity_version; manifest uses canonical recorded_at.

Do not regenerate the entire40M local graph to qualify one100k batch. The
already verified baseline supplies the unchanged-row reference. Audit the final
current and adjacency snapshots with both directions of EXCEPT ALL over every
known field for rows outside the selected identities. Compare changed surviving
current/adjacency rows against independent local complete-field digests; prove
all10k deleted identities absent and no unexpected selected rows. Counts must be
39,990,000 for each role; final typed endpoint closure and identity uniqueness
must cover the whole graph. Exact string fields preserve decimal lexemes,
explicit nulls, retained content and unknown extension content.

For raw and journal, prove every baseline row remains with its multiplicity and
compare the complete added-row multisets against independent input digests.
Use the synthetic unique delivery and batch scope only after proving the scopes
are disjoint from the baseline. Expected raw total48,100,000 and journal
192,216,667. Tombstones total10,000 with every known field verified. Digest
comparisons assume SHA256 collision resistance; native unchanged-row comparisons
must retain multiplicity rather than collapse duplicates with ordinary EXCEPT.

Bind each mutation's actual statement ID to its Delta history version; retain
all partial receipts. Publish no descriptor until every role, input origin,
revision map and final audit passes. Unknown write outcomes are inspected through
the same statement handle and history; never replay blindly. Real producer
fencing, source ACK, durable recovery and resurrection authority remain open.

Measure separate clocks for source preparation/transfer, input staging, clone
setup, and processing through final validation and manifest readback. The357s
observed transfer is a real preparation cost, not hidden from end-to-end arrival
age. Report the ready-input processing clock separately and do not describe it
as sustained10k/s or100k/s burst evidence. Admit native execution only after
source-bound read/write/spill/retained-storage and wall bounds are computed from
completed40M details. Existing compute only; no overlap with the growth workload.
