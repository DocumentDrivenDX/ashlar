# B source custody and retention decision record

Status: selected preparation defaults with unresolved authority decisions. This
record does not approve source acceptance, create custody copies, name a
custodian, or authorize execution. No private path was created or modified.

## Source candidate

Select the exact retained A UMF 0.10.0 bytes as the B source candidate because
the frozen R04 semantic draft uses the same candidate `Person` and `Place`
types, required `person_key`, `person_name`, `place_key`, and `place_name`
fields, absent-allowed integer `quantity`, and independent directed `located`
relationship. The source is 4,100 bytes with SHA-256
`498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b`, document
ID `ashlar-current-045-lifecycle-20261021`, and external revision metadata
`ashlar-current-045-lifecycle-r1`. The revision is not present in the source
JSON bytes. The proposed B path is
`/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/source.umf.json`;
it remains absent until separately authorized exact-byte custody preparation.

This shape supports the draft R1 create and R2 update intents. It does not
resolve whether Person deletion at R3 closes, preserves, or otherwise handles
incident edges, and it does not define the R4 additive candidate's versioned
extension semantics or payload. The semantic schedule itself labels these
inputs illustrative and unfrozen. Thus the source is a structurally grounded
candidate, not yet proven suitable for the complete B transaction inventory.
An owner must confirm that suitability after the complete operation inventory
and accepted source report/view are available.

## Origin, identities, and paths selected for preparation

Use the separate B origin payload already pinned in `b-origin-candidate.json`
(141 bytes; SHA-256
`2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247`) at the
external candidate path
`/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/b-origin.json`.
Its purpose and actor are B-specific. It remains unapproved; neither its
presence nor its hash establishes authority. A's origin is not reused.

For the bounded candidate config, select these routine unique defaults: output
root `/private/tmp/ashlar-r04-b-install-20261023-01/root`, PGDATA
`.../root/postgres-data`, attempt `.../root/attempt-01`, runtime
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv`, owner role
`ashlar_r04_b_owner_20261023`, registrar role
`ashlar_r04_b_reg_20261023`, registrar login
`ashlar_r04_b_reg_login_20261023`, reader login
`ashlar_r04_b_reader_20261023`, consumer
`ashlar-current-core-r04-b-20261023`, and acceptance request key
`ashlar-r04-b-source-accept-20261023-r1`. The config records these as
candidates only. The output root and paths remain absent; preflight must verify
canonical ownership, modes, non-aliasing, empty/fresh state, input separation,
and identity collision absence at execution time.

The candidate catalog CAS is expected catalog revision `0`, with exactly one
`CatalogDocumentExpectation` for the candidate document at source-catalog
revision `0`. This is conditional on a newly initialized empty B catalog; it
does not authorize initializing or overwriting an existing path. The nominal
LocalPostgres port remains the public API's fixed 5432 over its private Unix
socket. B needs an exclusive slot after the A/Arras service is stopped; the
process/socket inventory is not an OS lock.

## Retention decision

Proposed retention policy: preserve B PGDATA and all attempt evidence after
initialization through completed R04 closing evidence. If any transaction or
process outcome is uncertain, preserve PGDATA and the original request
indefinitely until that same request is reconciled; never retry against the
retained path. Preserve accepted B installation state after successful
registration. No person or role has yet accepted responsibility for this
retention, so the config leaves `retention_owner` null and execution remains
blocked.

## Remaining authority decisions

- Confirm the retained source is semantically suitable once the full B source
  transaction inventory and accepted model/report/view exist; resolve the R3
  incident-edge policy and R4 additive semantics before schedule freeze.
- Approve or replace the exact B origin bytes and actor/purpose.
- Name the retention custodian and approve the finite resource/deadline limits.
- Release the exclusive LocalPostgres slot after A/Arras shutdown.
- Use the already verified R4 exact 27-wheel lock selected by the B lock plan;
  produce and review a B-specific runtime receipt that binds that lock digest.
- Approve the final exact execution approval only after independent review of
  the complete source/origin/runtime/config/supervisor hash set.

The selected path and identity values are defaults for review and preparation;
they do not require separate owner choices unless collision checks fail or the
owner supplies replacements. The ordinary candidate values do not establish
provider-assigned installation, source-registration, epoch, namespace, or
consumer-registration identities.
