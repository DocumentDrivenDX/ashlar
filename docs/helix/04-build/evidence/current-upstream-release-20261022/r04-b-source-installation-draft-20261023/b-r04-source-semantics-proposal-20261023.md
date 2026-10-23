# B/R04 source semantics proposal for owner review

Status: effect-free semantic proposal only. No retained source bytes were
changed, reserialized, prepared, validated, or accepted. No provider IDs are
asserted. This proposal does not approve origin, source authority, execution,
or the R04 schedule.

## Evidence pins and limits

The B candidate is the retained UMF 0.10.0 source at
`/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/source.umf.json`,
4,100 bytes, SHA-256
`498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b`. Its
document ID is `ashlar-current-045-lifecycle-20261021`. The candidate revision
`ashlar-current-045-lifecycle-r1` comes from A's acceptance record; the source
JSON has no revision field. Only B's eventual accepted-document readback can
establish the B revision.

The source defines `Person` (`person_key` required string identity,
`person_name` required string, `quantity` absent-allowed integer), `Place`
(`place_key` required string identity, `place_name` required string), and
directed `knows` and `located` relationships. Both relationships currently
declare `targetLifecycle: independent`. This is a concrete structural match
for the draft R1/R2 Person and Place intent, but is not accepted R04 source
evidence and does not by itself define source-endpoint deletion or an Ashlar
R4 meaning.

## Proposed R3 incident-edge closure

For the draft lineage, R1 creates one new Person, one Place, and one directed
`located` edge from that Person to that Place. R2 updates that same Person and
leaves the exact edge identity and endpoints unchanged. Proposed R3 deletes
that exact edge and the same Person in one source transaction, retaining
history/tombstones for both; the Place survives. If the accepted R1/R2 readback
shows any other edge incident to the Person, enumerate and delete each in the
same transaction or stop. Do not leave a dangling edge or infer an edge ID.

This proposal follows CONTRACT-001's endpoint rule: node deletion with
surviving incident edges is `ENDPOINT_INVALID`; deleting those edges in the
same batch is permitted. The edge and Person identities remain symbolic until
the R1/R2 committed public results and readbacks supply them. Under the
approved slot-by-slot freeze rule, freeze the exact R3 request only after
verifying those predecessor results and before the R3 source effect. Whether
Truss 0.0.49's accepted transaction semantics can express and read back this
same-transaction closure must be verified against the actual accepted B API
results before adopting this proposal. This is a candidate policy, not a claim
that `targetLifecycle: independent` already means cascade deletion.

## Proposed versioned R4 additive property

Proposed minimal source delta from the retained original:

1. Add the vocabulary declaration `"ashlar.r04": {"version": "0.1.0"}` to
   the UMF document's `vocabularies` map.
2. Append module `m` / element `place_annotation` to the `Place` record's
   `members` list, leaving existing members, keys, relationships, values, and
   extension bytes unchanged.
3. Append a Field element `place_annotation` with `scalarType: "string",
   nullability: "absent-allowed"`, and `cardinality: "one"`. Give that field
   the versioned extension
   `"ashlar.r04": {"profile": "opaque-annotation/0.1", "meaning":
   "An optional source-authored annotation on a Place; preserve the exact
   string and its field presence. This is not identity, relationship,
   endpoint, ordering, or control data. The field's absent-allowed declaration
   does not specify explicit-null behavior."}`

The proposed R4 data transition is absent `place_annotation` before R4 to a
present non-null string after R4 on the R1-created Place; all prior fields and
identities remain exact. Keep the R1-created Person deleted at R3; R4 does not
recreate it. The literal annotation value remains an owner-authored schedule
choice and must not be filled from a fixture or inferred from the extension
name. This proposal treats R4 as an additive property value admitted by the
accepted, versioned model before schedule freeze. If the intended R4 operation
instead changes the accepted schema at slot R4, that is a different schedule
and must be specified and accepted explicitly before freeze.

The exact proposed field shape is a review target, not an accepted source or
semantically qualified UMF artifact. Under CONTRACT-003, UMF remains the schema authority;
Ashlar's meaning belongs in the explicitly versioned extension. The retained
source and any unknown extensions must remain byte-preserved. A B model carrying
this proposal would have a new exact digest and revision and could not be
described as byte-identical reuse of the retained source. The A source must
independently admit the same extension semantics, or the owner must select and
reconcile an explicit cross-source semantic mapping before freezing the common
R04 schedule.

An effect-free B draft was generated at
[`b-r04-model-draft-20261023.json`](b-r04-model-draft-20261023.json) by
applying only the vocabulary declaration, Place member, and Field element
listed above to the retained source structure. Its SHA-256 is
`8e8cab81f584495f25f8a5f4741dc5efd43f65ff1809a0622c88d6d9a62a9623`; the
retained original remains unchanged. One complete local pure preparation
against the installed Truss 0.0.49 / UMF core 0.10.0 bundle returned
`valid: true`, `complete: false`. It reported `UNKNOWN_EXTENSION` for
`/vocabularies/ashlar.r04` (exact extension version unavailable; content
retained) and `EXPERIMENTAL_CORE_SEMANTIC_TYPES`. The draft therefore passes
the selected structural validator with warnings, but the Ashlar extension
meaning is not bound by the installed UMF validator and must not be treated as
accepted or interpreted semantics. The Python `umf_access` 0.1.0.dev7 wheel is
present in the selected runtime; it does not expose a standalone raw UMF
document validator, so this validation result is from Truss's packaged UMF
core 0.10.0 producer, not an independent dev7 validation. Exact results and
effect limits are in the [validation receipt](b-r04-model-validation-receipt-20261023.json).

## Decisions still required

- Approve or replace the exact B origin payload and actor. The current candidate
  is pinned at SHA-256
  `2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247` and has
  no approval record.
- Separately authorize the one B source-acceptance attempt, its named registrar
  session, exact request key and expected catalog CAS, and the released
  exclusive LocalPostgres slot. Any approval must bind the exact source,
  origin, runtime, config, worker and supervisor hashes; the candidate actor
  string and prepared executor do not grant this authority.
- Accept or replace the R3 edge-closure policy after confirming Truss's source
  transaction representation and report/readback obligations.
- Accept, revise, or reject the `ashlar.r04/0.1.0` vocabulary and
  `place_annotation` definition; select the exact R4 value and whether R4 is a
  property update over an already accepted schema or a separately accepted
  schema revision.
- Decide whether the retained source/revision is suitable for the complete B
  transaction inventory. B's accepted report/view and provider-issued source,
  installation, feed, epoch, namespace, and consumer identities remain future
  observations; none are predicted here.
- Name the retention custodian and approve the retention policy. The existing
  B proposal preserves initialized/accepted PGDATA and all uncertainty evidence,
  but no custodian has accepted that duty.

## Governing evidence

- [CONTRACT-001](../../../../02-design/contracts/CONTRACT-001-publication-boundary.md),
  including endpoint invalidation and the approved R04 slot-by-slot freeze rule.
- [CONTRACT-003](../../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md#current-core-two-source-materialization-amendment),
  including UMF ownership, extension preservation, and independent source
  identity.
- [R04 semantic schedule draft](../r04-two-source-semantic-schedule-draft-20261023/README.md)
  and [B source custody record](b-source-custody-retention-decision-20261023.md).
