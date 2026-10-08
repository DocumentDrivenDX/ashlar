# End-to-end toolkit implementation plan

Owner goal2026-10-08: stand up Truss, consume UMF in Truss and Ashlar, add/evolve
Ashlar schemas, stream Truss and additional sources into UC managed Delta, and
provide a runnable setup/publish/query workflow. This supersedes the earlier UMF
deferral for this integration work. Existing meaning, identity, history and
publication requirements remain binding. Use small examples, not scale benchmarks.

## Deliverable and acceptance

A clean user can follow one documented workflow to start the selected Truss
runtime, submit a retained UMF document, write graph data, run the feed consumer,
resolve a completed Ashlar publication and perform a native singleton lookup.
A second schema revision and a second source can be applied without silently
renumbering identities, dropping unknown content or bypassing revision barriers.

Acceptance requires actual end-to-end execution, not a running PostgreSQL server,
source-only DDL, local simulator or successful resolver metadata probe. Retain
independent expected outputs and target/profile versions. Replay must preserve
current/history/tombstones, same-identity/version conflicts must refuse, an
interrupted publication must retain the previous descriptor, and source progress
must advance only after the required complete publication/durable handoff.
Demonstrate a create, property update, parallel edge, isolated node and delete.

## Ordered slices

1. **Local Truss deployment substrate and source selection.** Start isolated
   PostgreSQL17.9 with1CPU/512MiB. Pin the Truss/UMF source revisions and reconcile
   the current0.11 storage/installation inventory with accepted Truss decisions.
   Install the selected runtime through its public host interfaces; do not
   concatenate draft SQL fragments or substitute legacy0.2 silently. Native
   roles/producers/feed registration and complete transaction boundaries belong
   to the selected Truss profile. Server readiness alone is only infrastructure.
2. **Shared UMF schema intake.** Use UMF's existing TypeScript APIs at a pinned
   source/package/envelope version. Retain exact original documents and hashes;
   report known supported constructs, unknown retained assertions and enforcement
   classes. Compose stable catalog identity mappings for both consumers, keeping
   Truss catalog IDs authoritative where imported. Add the Ashlar schema-registry
   contract and revision compatibility/refusal rules before its implementation.
3. **Small Truss producer/feed.** Implement the declared mutation and complete
   feed boundary under Truss contracts. Register the consumer, reconstruct full
   current carriers from the native property/lifecycle/revision records, and
   preserve opaque source cursors/transaction boundaries. Do not replace the real
   Truss source with invented whole-entity journal rows and call it equivalent.
4. **Ashlar ingestion/publication.** Add a source-adapter port, staging/raw custody,
   idempotent history/current/delete apply and serialized immutable publication.
   Use small private UC tables on existing authorized compute. Wire the Python
   resolver to authenticated native transport, effective policy and retained pin
   custody. Preserve original source/revision/unknown bytes and source progress.
5. **Other source adapters.** Add JSONL/stdin and a PostgreSQL transactional outbox
   source profile, with declared key/version/replay/transaction semantics and
   durable checkpoints. Both use the same ingestion boundary; neither claims
   Truss-native semantics. Schema additions and an additive revision must run.
6. **Runnable example and user packaging.** Supply setup commands, a shared UMF
   fixture, producer changes, bounded consumer execution, native query and replay/
   interrupted-publication examples. Record live evidence separately from local
   tests and expose incomplete/unsupported outcomes. Keep secrets out of Git.

## Current authoritative starting points

Ashlar has candidate0.3 DDL and Python resolver/native-verifier ports under
CONTRACT-001–004. Truss's current documentation records a0.11/46-table source
packet but unstarted runtime and unfinished native installation/adoption. Its
accepted TypeScript/Bun portable-core and PostgreSQL generic-catalog directions
apply. UMF's checked-out source has implemented0.7 relationship APIs and earlier
field/key/facet capabilities; exact package/API selection must be pinned when
composing schema intake. Branches may evolve independently: retain per-step
source hashes and do not mix incompatible profiles.

## First execution

The isolated container ashlar-e2e-truss-pg17 is healthy on PostgreSQL17.9,
localhost15432, capped at512MiB/1CPU. tools/start_truss_sandbox.py creates/reuses
only its labelled container and generates a private database password without
printing it. The current Truss 0.11 declarations are now installed in this sandbox; no runtime
or feed has yet been implemented.
Other running containers are untouched. Use docker exec for local administrative
setup; design a least-privilege application role before the runtime integration.
No production data, broad database grants, retention cleanup, warehouse resize
or scale benchmark is part of this setup.

## Shared intake implementation checkpoint

The pinned UMF16c35e8d reader/validator now produces exact-byte shared artifacts
for the original Truss source-review model, an additive optional-string revision
and an unknown-assertion variant. Six bounded integration checks pass against
real APIs. UMF experimental warnings remain visible; no complete-interpretation
or target-enforcement claim is manufactured. CONTRACT-005 defines the intake
boundary. Next persist the same artifact in both native registries and implement
the selected binding/catalog acceptance; no full Truss runtime/feed is delivered.

## Native storage installation checkpoint

The exact 0.11 declaration source SHA256
1ac7cc82405ff581072d45ad586f54d8c48343eaecc7c011195535ed359e37f9
now installs transactionally on PostgreSQL17.9 using an explicit isolated UTF8
compatibility helper for one generated expression and a final statement
terminator. Native membership matches all 46 source tables and 442 columns;
two catalog functions are present and schema head remains 0. Five small helper
byte-conversion cases pass. Preserve original-source and derived-execution
hashes, failed receipts and the full native inventory. This does not establish
constraint/privilege equivalence or complete runtime adoption.

Next implement the selected atomic schema acceptance and stable binding, then
mutation/feed operations; storing a raw UMF document alone must not advance the
accepted catalog head. Application roles need their explicit privilege profile
before user-facing runtime access.

## Native shared schema custody checkpoint

The raw shared artifact now persists in both the isolated PostgreSQL intake
store and a private UC Delta intake table. Three synthetic documents retain
exact source/diagnostic bytes; identical replay preserves originals and a
same-key different valid document refuses without changing either store.
The separate raw registry does not mutate accepted Truss head (still 0).
Twelve focused local tests pass, including custody corruption/duplicate/unknown
refusals. Native receipts preserve the initial rejected Delta inline-CHECK DDL
and its terminal-state recovery through separate ALTER constraints.

The next runtime boundary remains complete target binding and atomic accepted
catalog persistence, including stable identity mapping and native report/effect
accounting. Truss HEAD 3f578b2 was consulted for this iteration; its source
contracts still mark native producers/guards/complete runtime unfinished. Do
not replace these with a schema_doc insert or advance a data publication from
raw registry custody. Additional-source adapters and publication wiring remain
part of the same active end-to-end goal.

## Stable identity planner checkpoint

The pure ID planner now retains authoritative supplied IDs across unchanged
identity, retirement and same-identity reactivation; distinct identity allocates
above the family highwater. It refuses duplicate state, missing active owners
and exhaustion, and preserves retired reservations. Eighteen focused local
tests pass. A separate host projection uses the pinned real UMF reader to
record all three original authored elements in the additive fixture with source
positions/digest and original diagnostics. Neither synthetic test IDs nor that
identity inventory are installed catalog acceptance.

Next compose the complete target semantic inventory with these identities and
locked native state, then persist it together with immutable acceptance report
and complete original effects. No native catalog head is advanced by this
planner. Source/feed and publication integration remain required.

## Actual semantic inspection and binding checkpoint

The pinned real UMF interpretation APIs now produce full source-qualified
receipts. This revealed revision 2 caption nullability optional is unknown under
the selected API, although structural intake is valid. Preserve its original
bytes/native intake row; its target binding is blocked. Separately authored
revision 3 uses absent-allowed and produces a candidate string-record binding.
Schema-property inspection requires 0.8.0 and its actual 0.7 refusal is retained.
No envelope rewrite or private interpretation fills that version gap.

The initial selected string-record target planner maps complete explicit members
to document/owner-qualified identities for the stable ID planner, preserving
source diagnostics and refusing unbound assertions, foreign/duplicate/missing
receipts and mismatched source custody. Twenty-two focused local tests pass.
Engine enforcement, broader value/key/relationship bindings and complete native
acceptance remain required; candidate plans cannot advance any accepted head.
No Databricks workload occurred in this iteration.

## Streaming source adapter checkpoint

The binary JSONL/stdin adapter now produces transaction-complete custody batches
with exact byte offsets, original control/event bytes and independently checked
ordered count/digest. One small synthetic create/update transaction runs through
the CLI; its output reconstructs the complete source byte-for-byte. Twenty-eight
local tests pass, including truncation, duplicate delivery, malformed control,
wide cursor and bounded transaction refusals. This supplies the common batch
boundary without claiming event interpretation or Truss-native semantics.

Next connect durable raw staging and replay/conflict custody to serialized apply/
publish, then add PostgreSQL outbox and the complete qualified Truss feed. The
reader exposes no acknowledgement and no checkpoint advance; publication remains
required before source progress. Native schema acceptance and the remaining
bindings remain active work. No Databricks workload ran in this iteration.
