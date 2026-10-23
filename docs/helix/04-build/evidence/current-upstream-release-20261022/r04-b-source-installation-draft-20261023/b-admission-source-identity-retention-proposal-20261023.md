# B admission: source, identity, and retention proposal

Status: read-only audit and candidate proposal for Sol review. This is not an
approval, source acceptance, execution instruction, or native-run receipt. No
source/origin files were changed, copied, reserialized, or prepared. No
preflight, PostgreSQL, Truss acceptance, Spark, feed read, destination, or ACK
was run.

## Audited evidence

The retained A source exists at
`/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json` as a
regular, non-symlink, mode-0600 file owned by uid 501. Its exact length is
4,100 bytes and its current SHA-256 matches the B packet pin:
`498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b`. Parsing
the bytes read-only yields document ID `ashlar-current-045-lifecycle-20261021`
and UMF version `0.10.0`; no revision field is in the JSON. The external
revision `ashlar-current-045-lifecycle-r1` is retained A acceptance metadata,
not a value independently verified from these bytes.

The retained A origin is 128 bytes with SHA-256
`a16b9405b213649ebfbda85ce0bf47877646eccb93b09176190be32bcb674fed`; its
actor is `ashlar-truss045-r03` and must not be reused as B origin. The B packet
candidate is 141 bytes with SHA-256
`2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247` and exact
content:

```json
{"actor":"ashlar-r04-b-source-acceptance","reason":"independent R04 B source acceptance using the retained original UMF 0.10.0 model bytes"}
```

The candidate's separate actor/purpose is suitable for review, but no owner
approval record exists. Hash presence is not approval. The private B root,
PGDATA, attempt, and runtime paths proposed in the existing attempt proposal
were all absent at audit time; this is only a point-in-time observation and
does not reserve them.

## One proposed source and identity set

The current-core two-source requirements demand independent A/B installations,
source registrations, feeds, epochs, namespaces, consumers, and ACK scopes;
they do not require the UMF document bytes or document ID to differ. Therefore
the concrete candidate is to accept the exact retained A document bytes in a
new B installation, with a separate approved origin and new provider-issued B
installation/source identities. Before that can be selected, the owner must
confirm that this source's schema is suitable for the eventual B transaction
inventory. That inventory and the R04 source values are not currently
authoritative, so the proposal does not declare semantic suitability.

| Pin | Single proposed value | Status / basis |
| --- | --- | --- |
| B document ID | `ashlar-current-045-lifecycle-20261021` | Same ID is allowed in a separate fresh installation; owner must confirm source reuse. |
| B document revision | `ashlar-current-045-lifecycle-r1` | Reuse A's external revision metadata only as the candidate pin; require B's accepted-document readback to match it. |
| B source custody copy | `/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/source.umf.json` | Candidate only; copy exact 4,100 bytes and verify hash at the authorized attempt. It is outside the executor's output root. |
| B origin custody copy | `/private/tmp/ashlar-r04-b-install-20261023-01/source-custody/b-origin.json` | Candidate only; copy exact 141 bytes after owner approval and hash it. |
| B output root | `/private/tmp/ashlar-r04-b-install-20261023-01/root` | Candidate, absent at audit; mode 0700, current-user-owned, and otherwise empty before attempt creation. |
| B PGDATA | `/private/tmp/ashlar-r04-b-install-20261023-01/root/postgres-data` | Candidate fresh path; initialize once and retain after any initialization/transaction uncertainty. |
| B attempt | `/private/tmp/ashlar-r04-b-install-20261023-01/root/attempt-01` | Candidate one-use attempt; no reuse or automatic retry. |
| B runtime | `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv` | Candidate fresh Python 3.12.14 runtime, outside the output root. It must have a B-specific dev7 runtime receipt and a hash-complete offline dependency lock. |
| Catalog CAS | Expected catalog revision `0`; one `CatalogDocumentExpectation` for the proposed document ID at source-catalog revision `0` | Conditional default for a newly initialized, empty B catalog; must be confirmed by owner and checked by the worker before the commit. |
| Install owner role | `ashlar_r04_b_owner_20261023` | Candidate caller-selected role; owner must authorize it and confirm no collision with A/other roles. |
| Install registrar role | `ashlar_r04_b_reg_20261023` | Candidate caller-selected role; owner must authorize it and confirm no collision. |
| Registrar login | `ashlar_r04_b_reg_login_20261023` | Candidate distinct login; owner must authorize the session/role relationship. |
| Reader login | `ashlar_r04_b_reader_20261023` | Candidate distinct ordinary reader; owner must authorize its minimum grant. |
| Consumer ID | `ashlar-current-core-r04-b-20261023` | Candidate caller-assigned identity, distinct by B/R04 label; owner must freeze it before the one registration call. |
| Acceptance request key | `ashlar-r04-b-source-accept-20261023-r1` | Candidate unique idempotency key; owner must freeze before the single acceptance request. |
| LocalPostgres admission | Nominal port 5432, private Unix socket derived from the canonical B PGDATA path; one exclusive slot after the existing A/Arras server is stopped | Fixed by the Truss 0.0.49 public API. No concurrent service or unique TCP-port claim. Gate inventory is not an OS lock and must be repeated immediately before launch. |

The candidate runtime receipt location is
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/runtime-receipt.json`.
It is intentionally absent. The historical R3 receipt records `umf_access`
0.1.0.dev6 and is not a B runtime receipt. The B plan selects the exact public
access dev7, core 0.10, Truss 0.0.49, and Weft 0.4.52 wheels and selects the
R4 27-wheel CPython 3.12.14 lock at its verified path and digest. The B-specific
installed-runtime receipt remains unbound.

## Executor alignment required before any use

The attempt proposal and config now put the runtime and source custody beside
the B output root. The reviewed supervisor requires a fresh output root to
contain only the attempt directory before it writes its started marker, and
rejects source inputs inside that root. The selected sibling paths satisfy
that layout; they remain absent and unreserved.

The worker and supervisor now require the same exact hash-pinned external
origin path. That candidate path remains uncreated and the B origin remains
unapproved; it cannot be consumed until exact bytes are placed there under an
approved custody step.

The `b-install-config.json` selects candidate B paths and routine role,
consumer, and request defaults, but keeps execution approval, slot release,
origin approval, retention owner, B runtime receipt, and catalog-manifest digest
unset. It remains inert until the new receipt, exact source/origin custody
inputs, remaining authority decisions, and Sol-reviewed hashes are joined into
a separately reviewed execution proposal.

## Defaults inferable from the approved R04 scope

- A and B must have independent accepted Truss installations, feeds, epochs,
  namespaces, consumer registrations and protected ACK scopes, even if local
  IDs overlap. They may use the same UMF document bytes; the contracts do not
  require different model content.
- B installation has no data dependency on A R03 completion. It does require an
  exclusive shared Truss LocalPostgres slot after the currently active A/Arras
  server is stopped. R04 schedule freeze and execution still depend on A R03
  and the complete B acceptance/identity evidence.
- A deliberately fresh B installation supports catalog and source CAS
  predecessor `0` as the proposed default. This is conditional on a verified
  empty B catalog and is not permission to overwrite or initialize an existing
  path.
- The B installation sequence ends after genesis consumer registration and
  shutdown. It must not read feed pages, publish to a graph destination, or ACK.
  Protected destination/receipt/ACK pins are later R04 requirements, not B
  installation defaults.
- The 600-second, 1-GiB, and 16-MiB limits in the current executor are
  implementation ceilings only. The contracts do not authorize a resource
  budget or name a retention custodian.

## Owner decisions still missing

1. Confirm that the exact original model is suitable for B's separately authored
   source transaction inventory; approve reuse of the same document ID and
   external revision metadata, or provide a new source/revision with custody.
2. Approve or replace the B origin candidate and its exact bytes/hash. Neither
   A's origin nor the candidate's presence is authority.
3. Check the selected private path namespace, B roles/logins, consumer ID, and
   request key against actual A/local state at the attempt boundary; approve
   source session/grants and confirm the expected one-document catalog CAS.
4. Produce and review the fresh Python/runtime cohort's complete dependency
   lock and B-specific receipt, then approve the one-attempt deadline/resource
   limits and exact source custody paths. Resolve the executor/path mismatch
   above before preparing an execution approval.
5. Name the person or role responsible for retaining B PGDATA and evidence.
   Proposed policy: preserve PGDATA after initialization and through completed
   R04 closing evidence; preserve it indefinitely after uncertainty until the
   original request is reconciled. Do not erase or retry an uncertain or
   accepted installation. No custodian is currently named.
6. Release and verify the exclusive 5432 slot after A/Arras shutdown. The R04
   contracts do not require waiting for A R03 to authorize B installation, but
   no owner decision currently schedules the B slot.

The routine path and identity strings in the config are selected defaults for
review and collision checking; they are not separate owner-choice blockers.
The complete R04 semantic schedule, A R03 completion, independent B
feed/transaction inventory, destination receipt and ACK scope, and closing
evidence remain separate later gates. No user input or execution approval is
requested by this review proposal.

## Governing evidence

- `README.md`, `b-install-attempt-proposal.json`, `b-install-config.json`,
  `b-installation-readiness.json`, and `b-runtime-compatibility-check-plan-20261023.md`.
- `source-byte-reuse-candidate-20261023.md` and the retained A source/readback
  evidence it cites.
- `../../../../02-design/contracts/CONTRACT-001-publication-boundary.md`,
  especially the independent two-source and approved R04 slot-by-slot request
  freeze requirements.
- `../../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md`,
  especially independent source custody and complete-vector/ACK requirements.
- `../../../../current-upstream-release-20261023/truss049-r3-readiness-20261023/README.md`.
