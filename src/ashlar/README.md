# Publication resolver candidate

Python3.9+ standard-library core implementing the proposed
[CONTRACT-004](../../docs/helix/02-design/contracts/CONTRACT-004-publication-resolver.md).
No credentials, cloud SDK, writes or automatic deployment are included.

resolve_publication requires an explicit trusted table/UUID inventory, supported
profiles/revisions and a backend. It refuses missing/ambiguous descriptors,
duplicate JSON members, malformed pins, unsupported revisions and native identity
mismatches, and returns defensive immutable metadata. Unknown original JSON text,
large integers and exact decimals are retained. Snapshot versions never follow
latest physical heads implicitly.

NativeBackend builds read-only parameterized descriptor SQL and pinned metadata
probes through an injected authenticated Executor. The Policy must independently
validate descriptor authority, effective caller policy and retained data availability.
There is no permissive default policy. This is the first runtime slice, not a
complete native transport, publisher, authorization service or support claim.
The existing SQL package supplies table definitions and singleton templates.

Run the small local suite from repository root:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The suite covers refusal order, exact metadata, immutable pins, native UUID/version
mismatches and pinned schema checks. No live warehouse or Spark is used. Next wire
an authenticated transport and qualified policy/custody provider into these ports;
keep their integration claims separate from these simulated checks.

SchemaIntake.read adds exact-byte shared UMF custody under CONTRACT-005. Supply
an explicit document revision and trusted validator source pin. It retains the
original source and diagnostic artifact, checks digests/identity/validation flag
consistency and refuses malformed input. It does not authenticate the producer
or perform target catalog acceptance; trust and admission are separate.

plan_catalog_ids in catalog.py plans exact document/owner-qualified IDs from a
complete admitted target inventory and trusted prior state. It preserves retired
reservations and same-identity reactivation; allocation stays above supplied
highwaters. Native acceptance must serialize/revalidate/persist the plan with
its full semantic and report effects. Pure planning does not provide acceptance,
authorization, field binding or a native allocator.

plan_string_record_binding in binding.py consumes independently trusted original
UMF interpretation custody for core0.7 singleton string Records, retaining the
full original report. It emits candidate identities or source-qualified blocked
assertions; engine enforcement remains unimplemented. The host interpretation
CLI calls actual pinned UMF APIs rather than defining private UMF meanings.

jsonl_batches in source.py emits immutable transaction-complete raw custody from
a bounded positioned binary stream under an explicit feed/epoch. The stdin CLI
is tools/read_jsonl_source.py. It preserves original bytes and verifies commit
count/digest; it neither interprets operations nor acknowledges progress. Durable
stage/replay, admitted schemas and complete publication remain required.

DeltaBatchStage in staging.py persists complete exact source custody through
parameterized Delta SQL and a required exclusive-writer policy. It checks native
UUID before/after, exact full readback and identical replay/conflict semantics.
StagedBatch is raw custody only; it authorizes no source acknowledgement. The
local development policy in the native checker does not qualify remote/native
fencing, application grants, graph apply or publication.

plan_apply in apply.py plans one complete explicitly-versioned whole-entity
transaction against trusted prior state and mandatory schema admission policy.
It retains history/tombstones/delivery claims, refuses conflicts/stale changes
and validates final typed endpoints while preserving parallel/isolated graph
identities. Returned state is immutable and prior state remains unchanged on
refusal. Native persistence/publication and Truss property-feed reconstruction
are separate unfinished integration steps.

changes_from_batch in whole_entity.py verifies original complete batch custody
and consumes an explicit signed64 whole-entity event profile. It never guesses
versions from property positions or aliases another source profile. Unknown
executable fields block normalized admission. Use its Change values only with
the mandatory target schema/constraint policy in plan_apply. The native nine-
event example is unpublished materialization evidence, not durable production
apply/recovery or a Truss property-feed adapter.

publish_batch in publisher.py coordinates a mandatory durable backend through
prepared/applying/applied/committing/committed phases. Uncertain submissions use
original apply/commit recovery ports; committed replay only acknowledges the
original descriptor. Native attempt custody, source fencing, complete validation
and immutable manifest/checkpoint producers are unfinished mandatory integration
work. Local mock tests establish orchestration order, not native durability.

DeltaAttemptStore in attempt_store.py retains exact original request/result/
descriptor artifact bytes as a contiguous immutable phase prefix, using bound
Delta MERGE/readback and mandatory exclusive-writer policy. Sessions verify UUID
and invalidate access after release. Native five-row custody/reload is verified;
real effect/commit proof, source fencing/grants, manifest and acknowledgement
producers remain required. A stored phase label is not publication authority.

PostgresOutbox in outbox.py reads bounded committed native source groups and
verifies exact original bytes/membership. OutboxTransaction preserves the outer
PostgreSQL previous/position separately from its contained JSONL batch offsets.
Protected development producer DDL and native role/rollback/replay evidence are
available. Registered source authority/retention, application effect recovery
and outer-cursor publication/acknowledgement integration remain mandatory.

DeltaManifestStore in manifest.py provides bound immutable manifest append and
exact original readback under mandatory writer and independent publication
admission policies. A validation flag is insufficient: the policy must prove
original effects, source progress, schemas, retained version pins and authority.
recorded_at is a canonical UTC microsecond decimal string at this API boundary;
the native carrier is TIMESTAMP. Fifty-nine focused local checks pass, including
replay/conflict and pre-effect denial. Native execution and producer/resolver
wiring remain unfinished; this component creates no default permissive policy.

DurablePublisher in durable_publisher.py connects publish_batch to the Delta
attempt store. Fresh instances reload original request/result/descriptor bytes
and resume uncertain phases through mandatory effect recovery. The effects
provider owns native operations, complete pin/admission proof and source
acknowledgement; it must return exact JSON strings for result/descriptor custody.
There is no default effect provider. Sixty-two local checks pass; native composed
execution remains unfinished.
