# Full-vector authority reference r369–r370

Governed by CONTRACT-001–003 and proposed ADR-001. This private reference does not activate a production authority service or mutate graph roles.

The reference preserves the exact original r332 publication descriptor strings, including progress, schema revisions, validation report and recorded_at, plus six native UUID/version bindings: object6/edge2/raw2/journal2/forward3/tombstone2. Original manifest UUID and every role UUID are checked live; each pinned snapshot remains readable. Preservation/integrity qualification is inherited from the separately audited source publication, not inferred from LIMIT1 availability. This includes the recorded range32 edge role; it does not substitute LC5 into that graph or select range32 as the canonical layout.

Private control table `client_dev.ashlar_entropy_20261006_r86.full_vector_authority_r369` UUID`38b002b5-beaa-4eb2-a250-808f83b3912d` stores an authority row and immutable descriptor references. One MERGE evaluates expected owner/token and changes authority plus appends a descriptor in the same Delta transaction. WriterA starts at0 and publishes with successor1. A transfer changes owner toB/token2. A staleA/token1 source also proposes a novel descriptor; native ASHLAR_AUTHORITY_STALE aborts the entire MERGE. Exact before/after rows and complete commit history remain unchanged. WriterB/token2 then publishes and advances to3. Complete owned versions0–4 map to CREATE, seed, acceptedA, transfer and acceptedB; the stale attempt has no commit. All27successful and1failed statements finalize with exact SQL custody.

Workload23.086s reports2,456,820read bytes/38,885write bytes/0spill, including the failed attempt and six snapshot availability checks. Descriptor references leave all original graph/history tables and real source progress unchanged. No source ACK, production pointer, provision/resize, OPTIMIZE, retention reduction or cleanup occurs.

## Scope of authority

The owner/token comparison is atomic only with the descriptor rows in this control table. It cannot stop an old worker writing to a different Delta table, does not prove source/catalog ownership and is not a lease service. Issued successor tokens are0→1→2→3 in this trusted worker; the SQL condition does not itself enforce monotonic generation issuance, so a general public API must enforce successor=current+1 inside the guard and validate row kinds/descriptor-token binding. Concurrent takeover, process crashes and lost submission outcomes remain unqualified. Exact duplicate publication must have a separate retained-descriptor readback path before deciding whether a fresh authority transition is appropriate; blindly reusing an old token is not accepted as replay.

For integrated logical publication, stale role writes must either be prevented by proven external ownership or confined to immutable attempt outputs that cannot advance the authoritative descriptor. Do not claim cross-table atomicity from this control-table MERGE. Adopt neither cleanup nor expiration until active manifest/cursor/recovery/serving pins can be enumerated and respected.

Next harden generation issuance and qualify concurrent takeover on this same small authority reference, then select the integrated writer-isolation protocol without loosening full carrier/history checks. This reference does not meet singleton/freshness/sustained/burst or1B/5B targets; all remain unchanged and open. Canonical hash LC remains the physical candidate, graph-engine limits remain separate, UMF binding deferred.

Evidence: [audited receipt](out/native/ashlar_full_vector_authority_r369/audited-summary.json), executable worker and independent offline audit.
