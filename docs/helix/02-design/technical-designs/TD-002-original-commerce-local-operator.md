---
ddx:
  id: TD-002
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: CONTRACT-006
    kind: informed_by
  - id: ADR-001
    kind: informed_by
---

# Explicit original-commerce local operator

## Scope and technical approach

Provide a committed installed workflow for separately initializing a fresh local
installation and publishing the original UMF0.8 commerce source. CONTRACT-001's
explicit original-commerce source section and fresh-empty installation exception
own the authority boundary. CONTRACT-003 owns native columns; CONTRACT-004 owns
publication-bound reads. Existing qualified E publication establishes feasibility,
not authority for another installation. This design adds a separately selected local development profile.

Reuse the actual public owners: declared-version UMF verification and
`CommerceAdmission`, `OriginalCommerceSourcePorts`, `OutboxSourceRegistration`,
`OutboxAuthorityRegistration`, `RegisteredOutboxSources`, `RegisteredOutboxAcks`,
`ExistingOutboxAuthority`, `OriginalCommercePublicationOperator`, `NativeDriver`,
`StoredPublisher`, `DeltaAttemptStore`, and `LocalDeltaTransport.initialize`.
Do not copy their validation, reinterpret original transactions or repair SQL.
Preserve exact 21 source events, model, graph, carrier bindings and independent
11-object/10-edge/0-tombstone/21-history bags. Evolution R1 is a different source
profile; original publication does not initialize an evolution run.

## Proposed CONTRACT-001 surface (review before code)

Append an “Explicit original-commerce local operator setup” subsection after the
existing explicit original-commerce publication section. The following is the
proposed normative interface, not an additional authority conferred by this TD:

- `initialize-original-commerce --configuration PATH` consumes bounded closed
  profile `ashlar-original-commerce-local-initialization/0.1` and creates only a
  separately authorized, exclusive new private local root. It MUST refuse existing
  roots, tables, journals, reservations or catalog conflicts; no replacement,
  deletion, migration or implicit resume is permitted. Actual native UUIDs and
  version-zero emptiness MUST be observed for all six tables before the canonical
  journal/reservations are initialized through `LocalDeltaTransport.initialize`.
- `publish-original-commerce --configuration PATH` consumes bounded closed profile
  `ashlar-original-commerce-local-publication/0.1`. It MUST reopen the exact retained
  original installation and ordinary registered source/ACK ports, then delegate to
  `publish_original_commerce`. It MUST NOT initialize tables/journals, create PG
  roles, grant privileges, discover credentials or fall back to legacy helpers.
- Shared configuration selects absolute model/graph paths and original content
  digests, public UMF producer configuration, explicit runtime/JAR locations,
  original installation ID, canonical root/journal, local table namespace and
  exact operation capacity. Initialization also selects an unused handoff output;
  publication selects the retained handoff plus its independently chosen digest
  and a fresh publication output. Each file/namespace group has one owning type;
  unknown fields, duplicate JSON keys, boolean/float integer substitutions,
  nonfinite values, oversized input and aliases MUST refuse before effects.
- The handoff contains observed six-target table/path/UUID/version correspondence,
  original journal/installation/capacity and selected source/runtime content pins.
  It MUST contain no credentials and grant no authority. Current native registry,
  reservations, root and shared `.evolution-operator.lock` identity, source/ACK
  sessions and independent current policy MUST be admitted again by the actual
  existing installation owner. Its digest is correspondence, never a grant.
- Fresh local setup authority is confined to an explicitly selected new private
  development root under an actual held non-reentrant POSIX installation lease.
  The setup owner MUST renew the original root/lock dev-inode, owner/mode, selected
  source bytes and runtime/JAR correspondence around each effect and closing.
  It MUST NOT claim UC, Truss or remotely authenticated production authority.
- Source selection uses the exact original feed/epoch, service schema, scope,
  installation, consumer, source schema/signature and writer/reader/ACK roles.
  The initial supported connector is explicitly selected isolated loopback trust
  against an already provisioned dedicated local PostgreSQL fixture. Connections
  MUST use exact ordinary LOGIN roles and explicit host/port/database, empty
  password, `/dev/null` passfile, disabled SSL/GSS and finite connect/statement/lock
  limits. No inherited DSN, environment/password file discovery or automatic PG
  setup is permitted. A forwarded port selects a connection and proves no route
  authentication. Other authentication profiles require separate governance.
- Initialization/publication execute in separate fresh processes with explicitly
  selected Python3.11/Spark3.5.3/Delta3.2.1 and the exact four existing publication
  JAR profile. Runtime locations are operator inputs; developer versions/content
  profiles bound acceptance. Neither command installs SDKs/downloads dependencies.
  Import-only inspection and process supervisor preparation are explicit operator
  steps. Query Spark4 and GraphFrames child runtimes remain separate workflows.
- Closing admission and cleanup MUST be attempted on every acquired interval,
  including body error/cancellation, preserving the original cancellation identity.
  Initialization evidence/report failure after durable creation MUST leave the
  owned installation retained and incomplete; no automatic cleanup of native data
  or reporting failure presented as rollback. Publication retains existing initial,
  same-invocation replay and exact head-one pre-native append recovery semantics;
  it MUST NOT imply general process-restart publication recovery.

## Component changes and ownership

New `src/ashlar_host/original_commerce_local_configuration.py` owns immutable
closed initialization/publication config types, bounded parsing, file snapshots,
explicit local connection selection and secret-free handoff encoding. Exact typed
ports have no caller-controlled skip flags or arbitrary policy plugins. Use the
existing `ProducerConfig`, `InvocationFile`, `DeltaTarget` and capacity owners.

New `src/ashlar_host/original_commerce_local_setup.py` owns fresh root/lock/native
creation and existing-installation publication composition. Public entry points
are `initialize_original_commerce_local(config)` and
`publish_original_commerce_local(config)`, each requiring its exact owning config.
The fresh policy is private to this owner, tied to the acquired root/lock/context;
it cannot be supplied as a fake callback. Consume public UMF-generated physical schemas and actual native
DESCRIBE DETAIL/history/count observations; existing publisher
performs original full-row checks and durable ACK ordering.

New `src/ashlar_host/original_commerce_local_cli.py` owns command-file custody,
exclusive evidence outputs and redacted failure dispatch. `src/ashlar/cli.py`
registers the two additive commands lazily. No imports of tools, temp recipes,
private vendor APIs or another host module's private methods. No changes to
existing evolution/query/resume defaults or source owners. Dependency checks:
`python3 tools/check_module_boundaries.py`; register only necessary public edges.
Tests are in three corresponding new test modules. The existing domain-pack
publication guide receives complete installed command/config examples using
operator-selected absolute paths, not retained `/private/tmp` artifacts.

## UMF-owned physical model and generated DDL

Ashlar already authors physical runtime models in
`docs/helix/02-design/models/ashlar-delta-runtime/`. The pinned public UMF
`generateDeltaDDL` API produces `schemaJson`, definition and SQL, retained in
`sql/ashlar-delta-v03/runtime-carriers.generated.json`; attempts, manifest and
history already have authored models. The existing generator revision is
fac1497a5ac5cb39bbaaac34eb0da8dc8b208c69. Commerce logical model admission at
UMF0.8/c7c95e is a distinct boundary: neither may be relabeled as the other.

The six initial local tables must come from an explicitly authored UMF physical
profile matching the qualified E carrier: canonical object/edge/tombstone/history,
local `attempts` corresponding to `publication_attempt_phase`, and local `manifest`
corresponding to `publication_manifest`. Existing generated models have NOT NULL
columns where the historical local E initializer created nullable columns. Do not
silently change nullability, clustering, properties or names, and do not carve
journal tables out of model ownership. Author a separate local physical-model
variant only where required to preserve the complete E schema; reuse existing
unchanged models otherwise. Exact input/output pins and semantic correspondence
are checked by the public UMF generator before packaging. No guessed field list,
handwritten replacement CREATEs or regex extraction becomes the schema authority.

New generated local artifact/model resources must be packaged for installed use.
A bounded Ashlar local dialect binding selects the exact Delta path/table name
from closed config and applies UMF `schemaJson` and declared definition properties;
it must preserve complete ordered types/nullability/metadata and supported layout.
If public generation lacks path-target binding, implement that narrow rendering
seam with exact generated definition/model correspondence, not a second UMF type
interpreter. Any unsupported metadata/layout refuses. Tests compare complete
native schema/protocol/properties to generated expectations and the original E
qualification; creation never migrates the retained E installation.

The existing `tools/generated_carriers.py` is a toolkit-only loader and cannot be
an installed dependency. Move only the necessary bounded stale-input/provenance
validation into a public owning module or consume an existing packaged public
owner. Model resources/generation changes are separately reviewed before source
implementation; no new generic UMF extension is established as necessary yet.

## Configuration and resource lifetime

Select configuration independently before executable effects. Capture its bytes
once with a bounded owner, renew after cleanup before releasing evidence. Validate
source/producer/runtime selections before mkdir or Spark launch. Source/role config
is passive and closed; actual privileges, role, schema, signature, batch and ACK
are checked through public source/session owners under current native lease.

Fresh setup creates a private directory exclusively, captures created-entry
identity before opening, checks held/named identity and shared lock on renewal,
then creates six native tables. Observe actual UUIDs/version-zero bags and invoke
public journal initialization. Independently attempt transport close, Spark stop,
substantive closing renewal and descriptor release through `finish`; release
handoff only afterwards. A partial native setup remains retained for inspection;
future initialization refuses it, with no orphan adoption or implicit repair.

Publication reopens handoff under bounded exact bytes; compare all six targets to
current journal/reservations/native identities, source and runtime. Construct the
public original operator using current ordinary sessions and an owned Spark
context. Delegate the entire source append/publication/replay/readback/ACK lifecycle
to its existing owner. Final report requires source/native/runtime closing and
configuration custody after Spark cleanup. A report never substitutes a lease.

## Diagnostics and bounds

CONTRACT-006 owns privacy and loss. Reuse existing lifecycle/capture/redaction
owners; emit bounded stage/exception-class/cleanup-state references, not DSNs,
passwords, connection locals, raw source SQL or inferred success. Preserve original
raw source/publication observations in private bounded evidence where the original
publisher requires them. Telemetry cannot grant admission, commit or completion.

Keep the exact small source, local[1], 512MiB driver and finite 300-second native
selection; a supervising process owns group cleanup and overall finite bound.
Do not claim a Python check is a hard cancellable JVM deadline. Inspection emits
candidate runtime correspondence, not a new support claim. Actual installed
qualification follows independent source/recipe review and serial engine execution.

## Testing and implementation sequence

1. Closed configs and handoff: exact original inputs; unknown/duplicate/bool/float/
   nonfinite/oversized/alias refusal; secret-free bytes; mutate handoff UUID/capacity/
   source or installation and refuse before effects.
2. Actual setup lifecycle with inert native seams: UUIDs must come from returned
   native detail, complete schema/version-zero/empty bags, existing-root refusal,
   lock/root substitution, competing owner, failed acquisition and closing drift;
   cancellation plus cleanup failure preserves identity and attempts all cleanup.
3. Existing publication composition: exact public operator/ports, source privilege/
   signature/role/checkpoint refusal, original head0 append and exact head1 reuse,
   head>1/different bytes/no journal refusal, durable ACK-after-vector and closing
   source revocation withholding report. Exercise actual owner methods rather than
   duplicate simulators. Query/resume paths must never call initialization.
4. Installed CLI help without SDK imports, successful typed dispatch and redacted
   refusal; strict ResourceWarning tests and module boundary negative controls.
5. Separately reviewed small native recipe: fresh initializer, public publisher,
   full original bags/current closing, then existing query workflow with unchanged
   four SQL cases. No re-run of qualified SDK/component tests absent a new concern.

No migration of existing installations is part of this slice. Removing additive
commands disables new setup; retained native state/source progress must remain
intact. Missing journal or ambiguous effect is a refusal requiring explicit
operator investigation, not selection of fresh to repair it.
