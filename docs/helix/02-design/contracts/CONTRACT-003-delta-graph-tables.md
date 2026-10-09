---
ddx:
  id: CONTRACT-003
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-002
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
  - id: FEAT-004
    kind: informed_by
  - id: ADR-001
    kind: informed_by
---

# Contract: Delta graph tables

**Contract ID**: CONTRACT-003
**Type**: schema
**Version**: proposed ashlar-delta/0.3
**Status**: Draft

## Purpose

Define actual warehouse tables close to Truss's accepted generic object/edge
layout, with rebuildable scalar-column projections for graph tools and filtered
analytics. Singleton lookup MUST be native Databricks SQL over Delta; no Fabric
Graph dependency is permitted for that path.

**Architecture:** Canonical state resides in Unity
Catalog managed Delta tables. Latency is an operational measurement, not an
architecture acceptance gate; preservation, identity and publication correctness
remain contract requirements.

## UMF model and generation

UMF MUST own reusable Delta DDL generation and explicit versioned extensions for
missing Delta semantics. Author the eight runtime carriers in the
[canonical UMF documents](../models/ashlar-delta-runtime/README.md), preserving
exact columns, nullability, clustering and table properties through
`umf.delta.definition` 0.1.0 and `umf.delta` 0.1.0. Installation MUST consume
checked generated output and refuse stale source/model hashes. Generation creates
managed CREATE proposals; migration and retention changes require explicit intent.

The microsite MUST consume the model for its runtime schema diagram and complete
physical fields. Logical identity Keys and edge endpoint Relationships state
publisher requirements, not Delta FK enforcement. Logical value families MUST NOT
replace native Delta types. A reusable physical endpoint-column binding requires
explicit correspondence. Optional adjacency, degree, typed and coordination
layouts require their own UMF coverage; the eight runtime carriers alone do not
cover them. Identify a manually authored conceptual map separately from a
UMF-generated diagram.

## Scope and Boundaries

The [0.3 DDL](../spikes/SPIKE-001-table-layout/sql/delta-layout-v03.sql) defines
the revised candidate surface. Preserve the
[0.2 baseline](../spikes/SPIKE-001-table-layout/sql/delta-layout-v02.sql) and
[0.1 baseline](../spikes/SPIKE-001-table-layout/sql/delta-candidate.sql) with their
original profiles; no in-place migration is implied. Publisher validation MUST
check degree direction and nonnegative counts rather than assume Delta CHECK
support. Retain Truss catalog IDs and source-local object/edge IDs without
renumbering. Exact UMF binding and accepted source catalogs require independently
qualified profiles; historical storage evidence cannot supply them.

## Physical policy and capacity requirements

The candidate default is unpartitioned identity-hash liquid clustering, an initial
64MiB file target and exact hash-plus-full-key predicates. targetFileSize is not a
maximum or promised file geometry. File-size and overlap-triggered maintenance
choices require measured ingest/read/write cost and complete preservation evidence;
blind range sweeps or per-batch cleanup promises are not admitted defaults.
Current physical heads MUST be reconciled independently of selected publication
pins before new writes. Autonomous maintenance must not silently repoint readers.

Canonical current, raw source, property journal, tombstone and derived roles are
independent preservation obligations. Raw custody cannot be replaced by parsed
current fields. Property-journal clustering must preserve source origin/version
meaning; an experimental source-delivery layout cannot silently replace the
selected position profile. Tombstones retain entity_version and latest-deletion
fencing; no TTL or resurrection permission follows from a row's existence.

Capacity/performance accounting MUST include nodes, current edges, independent
raw/journal/tombstone roles, projections, staged inputs, logs, retained snapshots,
failed work and overlapping releases. Cached point reads, edge-only bytes or an
arithmetic extrapolation cannot qualify complete scale/capacity. Each claim MUST
name corpus, runtime, cache/concurrency/workload and measurement scope.

INSERT operations MUST name target columns or verify complete source/target
column-order correspondence. Schema-revision metadata MUST match the selected
source/profile before manifest advancement. Partial role commits are not a
publication and MUST preserve the prior descriptor for inspection/recovery.
Graph-tool runtime and protocol claims require separate execution evidence;
bounded graph releases do not qualify the full planning graph.

## Normative Surface

| Table | Logical key | Meaning and rules |
| --- | --- | --- |
| object_current | source_system, type_id, id | Canonical current object; isolated objects are valid |
| edge_current | source_system, rel_type_id, id | Canonical directed edge with independent identity and typed endpoints |
| property_journal | Source-profile origin: feed/epoch/delivery ID/event ordinal for native records; explicit legacy scalar profile separately | Property-level evidence, missing/null flags, revision and direct raw origin reference |
| source_record | source_feed, source_epoch, delivery_id | Complete exact transport envelope, cursor, kind and digest; preserves uninterpreted content |
| tombstone | source_system, entity_kind, type_id, id | Highest accepted deletion version; no TTL inferred |
| publication_manifest | publication_id | One immutable descriptor of all exact table versions and source progress |
| adjacency_forward / adjacency_reverse | source_system, rel_type_id, edge_id | Optional narrow endpoint access; independent edge identity retained |
| degree_summary | source_system, type_id, id, rel_type_id, direction | Optional independent-edge counts at the same structural boundary |
| publisher_fence | stream | Optional coordination state; publisher epoch/sequence distinct from producer origins |
| apply_receipt | apply_batch_id | Optional durable batch proof; uniqueness enforced by publisher |
| node_type_a | node_key | Example typed serving projection; not canonical |
| edge_ab | edge_key | Example typed edge projection preserving parallel edges |

IDs are signed 64-bit source-local integers in this initial Truss-compatible
profile. A source requiring another identity representation MUST use an explicit
profile/adapter; no hash or numeric narrowing is implicit. `logical_key_json`
retains the producer's exact logical primary-key tuple separately from Truss's
internal ID. The `(source_system,type_id,id)` tuple is physical source identity,
not an assertion that UMF logical identity is a generated surrogate.

All `*_json` fields are STRING containing validated exact JSON text. Property
maps use stable producer catalog property IDs as text keys. Missing member is
absent; JSON null is explicit null. `retained_json` holds unknown extension/data
content, with no silent rebinding. Exact parser and renderer are required:
JavaScript number decoding, decimal rounding and time-zone normalization are
not implicit. Native JSONB author lexical representation is retained as delivered;
this Contract does not promise recovery of a lexeme already normalized by Truss.

Edge source/target IDs resolve in the same source_system in this profile. A
cross-source relationship requires a profile revision carrying endpoint authority
explicitly. Endpoints MUST resolve by both id and type, never id alone. Catalog
relationship endpoint restrictions MUST be validated before publication. The
same endpoint pair MAY have distinct edge IDs; a producer's multiplicity rule
may reject that but MUST NOT collapse identities during projection.

`property_journal` preserves property-level source events; null property_id is
reserved for entity lifecycle events. `old_present/new_present` distinguish
absent values from explicit JSON null. Raw source event/time text MUST be retained
where interpretation is unsupported. Version scope and source transaction
boundaries are adapter inputs requiring proof; the scalar fixture entity_version
MUST NOT be inferred by taking a maximum across unrelated property versions.
Whole-entity fixture batches in CONTRACT-001 remain a synthetic profile. A Truss
property-feed adapter reconstructs accepted object/edge state at a completed
source boundary while retaining the original journal. Its native feed semantics
and missing ordering guarantees require explicit reporting before adoption.

### Graph key projection

For ASCII source-system s, numeric type t and numeric id i, node key is
`N` + decimal character count of s + `:` + s + `:` + decimal t + `:` + decimal i.
Edge key substitutes `E` and relationship type. The length prefix prevents
source-name delimiter collisions; integer components use canonical signed
base-10 without leading zeros. This encoding is injective in the selected
profile; it is not a truncated hash. src/dst are node keys of typed endpoints.
Non-ASCII source names require an explicit encoding revision, not locale coercion.

The `ashlar-graph-release/0.1` profile uses the separately named `ashlar-key/1`
encoding for Unicode sources. Its key is `ashlar-key/1:` followed by unpadded
base64url of UTF-8 compact JSON `[kind,source,type,id]`. Kind is `node` or `edge`;
source preserves Unicode scalar spelling without normalization; type and ID are
canonical signed-64 decimal strings. Decode must reproduce the exact canonical
encoding. This profile does not relabel earlier ASCII keys or infer storage IDs.

A complete release binds original manifest carriers and all consumed native
UUID/version pairs. It preserves isolated nodes, independent edge IDs, self-loops,
parallel edges and typed endpoints. Duplicate identity, missing endpoints,
unsupported native carriers or exceeded explicit row budgets refuse the entire
release. Budgets never authorize truncation. Exact canonical props/retained text
remains available alongside an explicit capability/loss inventory: preserving
text does not imply selected scalar interpretation or any engine compatibility.
Source semantic admission, reader authorization, retention and pin custody must
hold until complete result release. External materialization, activation, refresh
and rollback require their own engine-specific validation.

Serving tables keep exact props/retained text plus explicitly selected scalar
columns and boolean presence flags. Conversion failure MUST block projection or
produce a named residual; it MUST NOT convert unsupported values to null silently.
No generic decimal-to-DOUBLE conversion is allowed. Graph exports may use exact
text plus a separate evidenced analytic cast. Native typed columns remain bounded
by their declared precision/range. JSON strings are preservation carriers, not
proof that every graph consumer can interpret their contents.

### Exact commerce graph arithmetic projection

The opt-in `ashlar-commerce-exact-graph-arithmetic/0.1` profile MAY project
original quantity values to signed-64 integer columns and authored scale-2 money
values to signed-64 integer coefficients. These are finite native carriers:
quantity's logical mathematical Integer domain remains unbounded, and the money
coefficient is not the logical Decimal value. The adapter MUST retain the full
original value and presence carriers, exact money tokens (including negative
zero), authored scale, and an explicit coefficient-to-value mapping. It MUST NOT
use floating-point conversion or infer source validity from native capacity.

Public UMF source admission MUST precede this projection. The adapter MUST check
native representability separately for every consumed source value and every
intermediate subtraction, addition and multiplication over the complete
unfiltered candidate bags, using arbitrary-precision arithmetic. Bounds checks
MUST precede native execution and MUST NOT be hidden by filtering, cancellation,
ordering or limits. Native results MUST independently match exact original-source
oracles, including multiplicities. Overflow or any unsupported expression MUST
refuse the complete query before releasing results. The profile's finite carrier
capacity MUST NOT narrow the original schema or qualify an engine's general
support for unbounded integer or decimal arithmetic.

### Publication and lookup

Publication MUST write/validate each table, capture its Delta version, then append
one complete manifest row. Only one fenced publisher may commit a manifest for
a stream; competing publishers require a later coordination design. Readers
resolve one manifest and time-travel every table to its recorded literal version.
Unmanifested newer table writes MUST remain invisible to boundary reads.
Retention MUST protect every referenced version; expired versions return
RECOVERY_REQUIRED. No cross-table atomicity claim follows from independent MERGEs.

Independent physical write lanes may leave committed versions before manifest
validation. A validation refusal MUST retain the previous manifest and MUST NOT
advance source progress; unmanifested versions require explicit state inspection
before recovery, not blind mutation replay.

Native lookup filters object_current by source_system, type_id and id at the
manifest's fixed version. Clustering/data skipping is the candidate optimization;
uniqueness and referential integrity require publication checks. Ordinary NOT NULL
columns do not enforce semantic keys, type rules or multi-table integrity.

Graph tools that cannot pin Delta versions MUST read immutable export tables from
one completed publication, retaining its descriptor. A mutable table/view pointer
swap is not assumed atomic across graph sources. Export materialization, engine
refresh and retained snapshots are measured separately; copying 1B nodes for every
microbatch is not an acceptable default. Frequent native publications and less
frequent bounded graph releases are separate policies.

## Precedence and Compatibility

CONTRACT-001 governs accepted state; CONTRACT-002 governs read outcomes. This
Contract maps them physically while preserving source evidence. Catalog additions
must not force canonical per-type columns; derived projection columns may evolve
through a versioned projection with explicit rebuild and loss report.

Delta clustering feature/protocol compatibility MUST be verified with each reader.
Liquid and partitioned/Z-ordered layouts are separate candidates. Type/id columns
must have statistics; key columns are never high-cardinality partition directories.
No PostgreSQL index, FK or JSONB capability is presumed to exist on Delta.

The edge statistics candidate includes entity_version and apply_batch_id alongside
lookup_hash/source_system/rel_type_id/id without changing logical identity or
columns. Existing tables require explicit measured statistics backfill; older
publication snapshots retain their original statistics. Update-only eligibility
in MERGE ON requires separate insertion/source-fencing qualification.

The journal statistics candidate includes apply_batch_id alongside
feed/epoch/source-position/id without partitioning or changing logical columns.
Statistics backfill and pruning gains require measured evidence at actual pinned
versions; they do not establish freshness or scale guarantees.

## Error Semantics

| Condition | Outcome | Recovery |
| --- | --- | --- |
| Duplicate identity, bad endpoint or broken type restriction | Validation refusal | Correct source/projection before manifest |
| Exact cast fails or meaning is unsupported | Projection refusal/residual | Preserve exact input; select explicit mapping |
| Manifest table version expired | RECOVERY_REQUIRED | Rebuild consistent publication |
| Unknown Delta reader feature | Compatibility refusal | Compatible export profile |
| Native singleton returns several rows | Integrity failure | Do not choose an arbitrary row |
| External graph release exceeds target limits | Unsupported projection | Bound scope or select another engine |

## Examples

Truss source `pilot`, TypeA catalog id 1, object id 123 maps to node key
`N5:pilot:1:123`. AB relationship id 7, edge id 800 maps to `E5:pilot:7:800`.
Singleton lookup uses source_system='pilot', type_id=1, id=123 on Delta directly.
The same edge's src/dst carry the endpoint node keys; no graph import renumbers it.

## Experimental narrow serving variant

The normative candidate DDL still duplicates full bags in serving tables.
`native_stream_narrow.py` tests an alternative with only source/type/id,
entity version, and two promoted scalar properties in the serving fixture.
Exact property-ID maps and all retained content remain in canonical state;
property 103 is deliberately unpromoted. A consumer requesting complete content
MUST resolve canonical state using the same immutable publication table-version
vector, typed identity, and matching entity version. A scalar-only external
graph export MUST declare residual properties and retained content unavailable
in that export and provide an explicit canonical retrieval path where supported.
No automatic completeness claim follows from the scalar graph projection.

Narrow serving requires independent validation of missing/null/cast policy,
source reconstruction, edge mappings and publication recovery. Validate canonical
exact text, scalar promotion, journal old/new values and version parity; hydrate
each published version against independent complete source carriers before
adopting the variant in the normative DDL and ADR.

A scalar surface MAY use canonical VERSION AS OF directly instead of a physical
serving copy, with injective keys and explicit residuals. The manifest records
only actual table versions and MUST NOT invent a serving-table version. Complete
reads consume the same canonical version; parsing cost, connector support, policy
and query performance require separate qualification.

## Validation Requirements

Require native DDL creation, duplicate/endpoint/exact-value refusal controls,
interrupted-publication recovery, measured lookup pruning/latency with workload
scope, and separate engine mapping/protocol/identity/policy/progress evidence.
Tuning objectives do not gate the Unity Catalog Delta architecture.

- [x] Delta DDL executes in the recorded dbw-aidev-cus synthetic sandbox.
- [ ] Duplicate/endpoint/exact-value checks and interrupted publication proven.
- [ ] Record native lookup latency and pruning with workload/compute/cache scope; tuning objectives do not gate the UC Delta architecture.
- [ ] Each engine proves mapping, protocol, identity, policy and progress limits.

## Physical design refinement to carry into the next DDL revision

The logical current-state tables remain generic Truss-compatible carriers.
Canonical identity-oriented clustering uses an optional publisher-computed
`lookup_hash` with an explicitly versioned exact-tuple encoding and validation.
It cannot replace native key predicates or identity checks. Preserve the existing
DDL as the executed baseline until the revised profile is authored and verified;
experimental hash/publisher columns are not silently normative additions.

Add narrow adjacency keyed by source authority, relationship and independent
edge ID, retaining source/target type and ID. Indexing layouts for forward and
reverse traversal may differ; parallel endpoint pairs never collapse. Degree
summaries are derived from that same published structural state and declare what
counts they represent. A logical expansion cap is not a physical scan bound.

Publisher batch IDs, fencing epochs and durable receipts are coordination
metadata. They must not overwrite or manufacture producer source-feed positions
or event ordinals. The property journal remains source evidence, with exact
missing/null and old/new token semantics. Catalog-managed atomic transactions are
an optional implementation profile with explicit protocol/reader qualifications;
the immutable manifest remains the consumer boundary in either implementation.

Graph mapping adapters consume selected typed scalar projections with complete
native identity and explicit residual/cast declarations. Their physical copies
and refresh cadence are optional workload decisions, not an automatic copy of
all canonical bags for every microbatch. Preserve an exact canonical retrieval
path where the selected consumer supports it. UMF vocabulary/source binding requires an explicitly pinned supported profile.

### Revision 0.2 field and projection rules

`lookup_hash` is required derived metadata in both current tables. Compute it as
lower-case SHA-256 hex of UTF-8 compact JSON with named members in exact order:
`source_system,type_id,id` for objects; `source_system,rel_type_id,id` for edges.
The publisher validates this value; readers still filter the full native tuple.
The Python encoder uses ensure_ascii=False. Default DEL escaping or serializing
numeric IDs as JSON strings changes the bytes and MUST NOT be substituted.
The encoding must have adapter differential evidence before a non-SQL producer
uses it. Hash equality does not establish native identity.

Nullable `apply_batch_id` in current/journal tables permits imported rows while
keeping coordination distinct from producer history. A selected apply protocol
must issue and validate unique batch IDs and retain durable receipts. The fence
and receipt DDL does not itself enforce race/replay protection or cross-table
atomicity. Catalog-managed transactions require a separate explicit participant
feature profile and compatibility qualification.

Adjacency logical identity is the canonical edge tuple; structural versions are
publisher projection revisions, not replacements for property event versions.
Property-only updates need not rewrite unchanged adjacency. Degree counts retain
parallel edges; self-loops count once per direction. Missing summary rows mean
zero only under declared complete relationship/direction coverage. Every derived
table that a request consumes must appear in its immutable publication vector.

Optional typed tables are examples, not an obligation to duplicate every type or
bag. Reader support and workload evidence decide projection materialization;
revised DDL execution does not establish any external graph-engine support.

### Property-feed qualification boundary

Before claiming a Truss feed adapter, require a versioned source profile specifying: (1) transaction-complete boundary and bootstrap snapshot/offset handshake; (2) origin uniqueness across feeds/epochs and exact replay/conflict rules; (3) the native version scope for each property and lifecycle event; (4) revision ordering and stable property identity; (5) current-state reconstruction or transaction-consistent full carriers, including row-home properties and retained data; (6) endpoint creation/deletion ordering; and (7) retention/checkpoint recovery guarantees. Do not fill these fields from whole-entity synthetic fixtures.

For the existing journal representation, absent means `present=false` and SQL NULL token; explicit JSON null means `present=true` and the exact text `null`. A present value requires a non-NULL JSON token. Invalid flag/token combinations must be refused before publication. Preserve producer tokens as delivered, including decimal/exponent and timezone text. Every accepted origin appears once in the pinned journal; retries with conflicting content at an existing origin are refused. Journal reconstruction requires the qualified source ordering profile and complete retained history. Truss as-of behavior cannot be advertised from mere storage parity. Every profile MUST define retention/checkpoint recovery before purge; no purge is implicitly authorized.

### Truss feed profile reconciliation

A source profile selecting Truss draft layout 0.2 MUST preserve `(xid,seq)`
ordering strictly below its safe watermark. seq or statement time alone is not
a checkpoint. ver is the record version after change; property rows of a change
share entity/version/xid. Draft semantics require actual transport qualification.

The source carries `change`, `revision`, `source` and `reservation` records. A change includes exact old/new JSONB text, revision and origin (including unknown host keys). Create/delete envelopes include props and retained values. Retain/rebind/transform semantics require separate application rules. Revision documents have exact bytes and content hashes; native source/null distinctions require an explicit transport encoding. The 0.2 Ashlar journal alone cannot preserve origin, all source record kinds or complete revision documents. Do not advertise a lossless Truss adapter from current fixture results.

The supplemental [source_record carrier](../spikes/SPIKE-001-table-layout/sql/source-record-candidate.sql)
MUST store every complete exact transport envelope and cursor, including unknown
fields/kinds, before interpretation. Encode xid/seq as canonical decimal strings;
never flatten their tuple into source_position or publication sequence. Rebuildable
journal/current projections require qualified source ordering/retention and a
versioned envelope encoding, digest definition and delivery-ID derivation.

A release progress entry for this source must expose the original tuple cursor plus safe watermark and source-profile version. Source acknowledgement follows durable publication and recovery state; lost acknowledgement can replay safely. Never advance the checkpoint into a partially consumed source transaction or silently discard provenance/reservation records. Unknown source operations may be preserved without interpretation, but publication of current state requiring their meaning must stop with an explicit unsupported-operation residual. This separates lossless retention from supported interpretation.

The draft feed requires retained history past every registered consumer position unless that consumer is explicitly removed; bootstrap from a consistent snapshot and cursor remains to be designed and tested. A source ahead of the safe watermark is reported as not yet publishable, separately from publishable backlog. Transport freshness is measured from source evidence; it cannot be inferred from fixture statement timing. Raw revision preservation remains separate from admitted UMF interpretation/bindings.

Ingestion MUST use bound parameters or a verified byte-preserving transport,
never interpolated unverified SQL literals. Exact received-time and apply-batch
metadata must survive replay; they are ingestion metadata, not source payload
identity. Read-only duplicate/conflict classification alone does not prove atomic
idempotent append or multi-writer uniqueness.

A Truss mapping MUST distinguish tuple cursors from scalar fixture positions,
use qualified injective delivery IDs, transaction-complete batching and durable
acknowledgement ordering. Journal rows alone lack all structural/current fields;
side records may lack journal-style cursors. Require direct journal/raw references,
a source-profile revision and transaction-consistent extraction.

### Proposed 0.3 cursor/reference extension

The versioned DDL incorporates source_record and adds source_cursor_json/source_delivery_id to object_current, edge_current, property_journal and tombstone. The reference resolves the exact raw row under the same source_feed/source_epoch namespace. Cursor JSON preserves native components as strings; cursor identity is the qualified profile's tuple, not JSON member ordering. Native-source publication requires non-null valid cursors and resolvable references. Include the source_record table's actual version in every publication consuming these references. Do not flatten a source cursor into scalar source_position.

Scalar source_position becomes nullable for native tuple profiles; it remains required under the explicit legacy whole-entity fixture profile. Native journal origins are keyed by feed/epoch/delivery ID plus stable event ordinal within that envelope. The producer profile declares the ordinal mapping and validates uniqueness. Current/tombstone references identify their last accepted causal record; reconstructing full current carriers still requires a consistent extraction boundary and complete structural data. These fields preserve provenance; they do not prove a reconstructing adapter.

This is an additive draft schema change plus relaxed scalar nullability, not an in-place migration. Existing 0.2 publications retain their original profile/version vectors. Native DDL, semantic references and producer/publisher behavior require separate execution evidence. Exact cursor format and producer encoding remain source-profile qualifications.

A complete-feed worker profile MUST establish complete manifests, trusted
registration/generation and verifier-observed committed application. Carrier,
fence and receipt storage do not manufacture those authorities.

Native singleton builders MUST validate identity text in the signed-int64
profile before casting and bind values through parameters. Complete native tuple
predicates and the published literal version remain required alongside hashes.

Forward/reverse adjacency reads MUST use bounded parameterized keysets at
published structural versions, retaining parallel-edge identity and self-loops.
Continuation order is `(rel_type_id,edge_id)` under fixed source/type/id,
direction and publication. The caller service must establish cursor-context custody.

### Publication recovery requirements for the 0.3 layout

These are proposed protocol requirements; the executed 0.3 DDL remains unchanged.
A publication profile MUST identify its writer authority and the mechanism that
prevents stale owners from committing. A fence row read before a separate write
is insufficient. Until that mechanism has evidence, the implementation profile
is restricted to one externally serialized publisher with no automatic takeover.

A durable application receipt MUST bind the batch ID to exact staged content,
source profile and complete input boundary, previous publication, and validated
output table-version vector. The existing apply_receipt columns do not contain
that complete binding. A future schema revision or qualified supplemental record
is required; sequence and payload_digest alone MUST NOT imply publication success.
Digest encoding and canonical comparison are profile-versioned requirements.

| Durable state observed after interruption | Required recovery outcome |
| --- | --- |
| Raw capture only; no output receipt or manifest | Resume validation from the retained complete input; source progress remains at the prior publication |
| Some output commits; no complete output receipt | Keep the prior manifest visible; inspect actual commits and retained stage before repair; never assume a timeout means rollback |
| Complete validated output receipt; no manifest | Revalidate authority, predecessor and exact versions, then publish the same batch once; do not reapply its producer events |
| Manifest exists; acknowledgement absent | Verify manifest/receipt/input binding, return the same publication and retry acknowledgement only |
| Existing batch ID with different input, predecessor or output vector | Refuse conflict and retain evidence; no replacement manifest |
| Missing pinned version, expired input evidence or uncertain writer authority | Block recovery and source acknowledgement; require reconciliation |

Readers MUST resolve a single immutable manifest and consume only its recorded
versions. Internal latest-state writes are not a consumer publication. Every
publication ID MUST identify one immutable descriptor; duplicate descriptors
must be rejected or verified byte/semantic equivalent under the selected profile.
The ordinary manifest DDL provides no uniqueness enforcement. A source adapter
MUST acknowledge only its qualified complete boundary after durable publication
and required trusted downstream proof; an Ashlar receipt cannot manufacture the
Truss worker profile's host-observed authority.

Retention MUST protect every version referenced by an active publication or
registered reader and the raw input/receipt evidence needed for replay. Expiry MUST follow the qualified finite-retention policy in CONTRACT-004;
no implicit safe purge horizon is admitted. Maintenance creates a new version and may be
published only after preservation validation; it does not silently update an
existing descriptor. These rules require interruption, lost-acknowledgement and
stale-owner tests before runtime support is claimed. Single-table or prepublication reference controls do not prove this recovery protocol.

### Projection invalidation and unchanged version reuse

A publication MUST validate derived rows against its canonical structural state.
A property-only canonical change MAY reuse previously published adjacency and
degree versions when endpoint, independent-edge identity and declared coverage
are unchanged. A structural_version is a projection revision; it MUST NOT be
compared to an unrelated canonical property entity_version as proof of freshness.
The validation report MUST identify which projections changed, which versions
were reused, and their direction/relationship coverage.

| Accepted change | Required invalidation |
| --- | --- |
| Property/retained content only | Canonical carrier and applicable typed property projections; structural projections may remain unchanged |
| Edge create/delete or endpoint/type change | Affected forward/reverse adjacency and degree counts within declared coverage; typed edge release when included |
| Node create/delete | Included typed node projection; incident-edge closure checked in final state; node deletion cannot leave surviving edges |
| Revision changes scalar mapping or type inclusion | Affected typed projection rebuilt or explicitly unavailable until validated; canonical/source meaning remains preserved |

Only the selected source profile can decide whether an operation is interpretable
and which native events it creates. This matrix does not define Truss retain,
rebind, transform or recreation semantics. Unknown structural effects MUST block
publication rather than justify reuse. Missing coverage MUST be reported as
unavailable; it MUST NOT silently become an empty adjacency or zero degree result.

### Read-plan locator validation

Before issuing a multi-table read, the executor MUST require a committed version
for every table that the selected plan consumes, including hydration and structural
validation dependencies. It MUST reject malformed versions and identifiers, retain
one publication identity, and protect admitted versions from later caller mutation.
Unused optional projections need not exist, but their absence cannot become zero
results for a plan that requires them. The plan inventory is an executor input;
complete inventory selection and publication authority require separate validation.

A read-plan locator MUST be a defensive immutable copy. Its decoder must refuse
duplicate descriptor members and independently establish descriptor custody,
semantic projection validity and policy. A builder or decoded-locator helper
alone supplies none of those authorities.

### Proposed durable receipt surface

The separate [publication-receipt candidate DDL](../spikes/SPIKE-001-table-layout/sql/publication-receipt-candidate.sql)
proposes ashlar-publication-receipt/0.1. It does not revise the executed 0.3 table
set. Its logical key is `(stream,apply_batch_id,phase)` with phase `intent` or
`complete`; the publisher MUST enforce uniqueness and phase semantics. Records
are immutable. Record both phases as append-only evidence, rather than replacing
an incomplete record and losing the recovery intent.

The exact UTF-8 receipt_json is the binding envelope; receipt_digest is its
lower-case SHA-256. It MUST contain the fields below. profile_version, stream,
apply_batch_id and phase MUST agree with their table columns. recorded_at is
operational metadata outside replay identity. Native source IDs, cursor components
and worker generations retain qualified text representations within their original
boundary/authority envelopes. Do not narrow them to the old BIGINT fence column.

| Envelope field | Intent | Complete | Validation |
| --- | --- | --- | --- |
| profile_version, stream, apply_batch_id, phase | Required | Required | Exact supported profile and table-column agreement |
| predecessor_publication_id | Required, nullable only for explicit bootstrap | Same as intent | Predecessor descriptor must exist unless a separately qualified bootstrap boundary authorizes its absence |
| authority_context | Required | Required | Complete selected authority context, including generation/registration where applicable; stored context is not trusted proof |
| source_boundaries | Required | Same exact boundaries as intent | Versioned complete boundary records for every input feed/epoch; a highest tuple alone is insufficient |
| input_table_versions | Required | Same as intent | Qualified retained stage/raw table names and actual committed versions, with complete input membership and byte-digest definition |
| input_digest | Required | Same as intent | Digest of the qualified retained input representation; profile must define membership, ordering and encoding before use |
| output_table_versions | Absent | Required | Every consumed canonical/history/raw/derived table at actual validated versions, including reused unchanged versions |
| validation_report | Absent | Required | Checks and declared projection coverage, exact input/output binding, source-profile and validator version; no unsupported capability promoted |

A complete record MUST bind to the existing intent and its original input,
predecessor and authority context. The recovery protocol MUST independently verify
whether the authority remains valid; it cannot reuse an expired registration just
because the bytes match. Receipt uniqueness, stale-writer refusal and atomic
manifest installation remain protocol obligations outside this ordinary DDL.

An identical key retry MUST preserve the original record after comparing exact
receipt bytes and their digest. A byte-different envelope at the same key is
RECEIPT_CONFLICT even if its JSON objects appear semantically equivalent; retain
producer input and stop publication. This profile deliberately makes byte encoding
part of replay identity. A caller needing reordered equivalent JSON must reuse
original retained bytes or select a different explicitly qualified profile.

Recovery from intent alone inspects actual output commits and repairs missing
work without guessing a vector. Recovery from a complete receipt revalidates its
input/output binding and installs one immutable manifest. A manifest reference
must identify the complete receipt by stream, batch, digest and actual receipt
version in its validation report; a mutable latest lookup cannot establish custody.
Lost acknowledgement returns the same manifest and never appends new source
history. The source's trusted downstream-application proof remains independently
required where its worker profile demands it.

### Explicit private local operation capacity

Private local Delta transport MUST retain the 4MiB serialized UTF8 operation
intent default. An explicitly selected `ashlar-private-local-operation-capacity/0.1`
profile MAY permit exactly 8MiB for a bounded original fixture whose retained
complete operation genuinely requires it. The closed profile has only `profile`
and integer `max_intent_bytes` equal to 8388608; unknown versions, limits, fields,
Boolean numeric flags and implicit selection MUST refuse before effects.

The nondefault capacity MUST bind the original installation registry,
reservation, journal and every operation intent/digest. Reopening an installation
under missing or changed capacity MUST refuse; it MUST NOT rescue a lost original
journal, authorize replacement mutations or alter unknown-outcome recovery.
Default original registry and operation bytes/identities MUST remain unchanged.
Complete UTF8 operation size MUST be preflighted before journal intent submission
and native effects; exceeding the selected finite capacity MUST refuse without
truncation. The separate core publication phase payload ceiling remains 4MiB.
An outer transport framing allowance MUST NOT enlarge logical source domains,
change original values, substitute incomplete source/effect inventories, claim
remote fencing or turn a provisional applied state into a publication or ACK.

Acceptance coverage MUST exercise exact default/nondefault boundaries, malformed
profile refusal, unchanged default bytes, mismatched reopen without new effects,
complete original payload retention and default refusal before journal/native
mutation. A consumer claiming a completed publication MUST retain closure and
original manifest/ACK evidence separately from interrupted native effects.

### Private local export custody

A private local host MAY export an immutable graph release using a separately
named `ashlar-private-local-graph-custody/0.1` receipt when its original manifest
has no retention-target inventory. A present malformed or mismatched retention
inventory MUST refuse; this profile MUST NOT rescue it. It MUST preserve the original manifest and
release bytes without adding inferred retention metadata. The receipt MUST bind
both exact byte digests, the complete original native table UUID/version vector,
the graph roles, source-admission custody and the exact published source ACK.
The trusted host MUST hold its admitted writer, source and read-pin interval
through resolution, complete independent source-to-carrier comparison, export,
and closing native-vector and source/ACK checks. A failed closing check MUST
withhold both the release and its custody receipt.

An offline engine consumer MUST explicitly select this profile and receive
separately trusted release and receipt digests. It MUST reject unknown profiles,
missing or duplicate fields, altered manifest text, incomplete or changed vectors,
and mismatched role, source or ACK bindings. The existing retention-report profile
MUST remain strict; absence of retention metadata MUST NOT trigger fallback.
Digest equality establishes custody only under the independently admitted host
trust boundary; an untrusted caller cannot authorize a release by hashing it.

This profile admits rematerialization of the retained immutable export. It MUST
NOT imply continued canonical snapshot availability, future retention, production
source authority, Unity Catalog access or engine activation. Each engine MUST
separately prove its mapping, exact values, queries and release lifecycle.

### Engine release protocol qualification

A release adapter MUST record the actual Delta protocol/features, reader/runtime
version, catalog/storage access route and publication binding. It MUST establish
that the selected reader consumes the exact release before promoting support. A
failed or unsupported feature negotiation blocks that engine's activation while
native canonical reads remain available. If a separate compatible materialization
is required, select and verify its protocol after creation, compare every field
and identity, and bind its own actual versions; no canonical downgrade is implied.
Feature removal, storage export and access provisioning are separate actions.

The [PuppyGraph Delta connection documentation](https://docs.puppygraph.com/connecting/connecting-to-delta-lake/)
requires access to both metastore and storage and describes Unity Catalog/Azure
credential vending configuration. The inspected page does not supply a complete
reader-feature support matrix for these releases; connector version/runtime tests
remain necessary. [Fabric Graph limitations](https://learn.microsoft.com/en-us/fabric/graph/limitations)
identify OneLake/Mirrored Database sources, scalar ingestion types and unsupported
nested map/struct types; this does not qualify these Azure Unity Catalog release
locations as Fabric input. Fabric requires the separately scoped OneLake release.
GraphFrames qualification likewise requires the selected Spark Delta reader and
actual pinned DataFrames, beyond the syntax-checked adapter. Actual engine sessions must qualify the chosen access route and reader profile.

A source adapter claiming Truss layout 0.2 MUST preserve shared object/edge ID
allocation and unique relationship/source/target edge rules. Generic parallel-edge
fixtures qualify carrier capacity only; incompatible producer duplicates must be
refused, never collapsed.

Wire timestamp validation MUST compare independently encoded native instants,
including sub-millisecond and pre-epoch controls. Comparison against the same
encoder cannot prove preservation. UTC-microsecond encoding and compact token
checks require separate escaped/pretty-token qualification; historical normalized
payloads MUST NOT be silently relabeled exact.

### PuppyGraph runtime identity and activation requirements

A PuppyGraph adapter MUST retain independently queryable graph identity carriers
such as carrier_id/carrier_key and compare them with the release source. Carrier
parity MUST NOT replace graph endpoint/identity checks. JSON-number parsing or
floating-point identity conversion is forbidden. Aliases belong to serving
adapters, not canonical tables.

A production adapter MUST qualify activation and rollback rather than infer
atomic switching from schema upload. Readers remain bound to one validated
release while its successor is built and checked. Retained releases and dual
engine overlap enter capacity accounting; expiry requires proof of no reader pins.

External UC runtime access requires separately authorized access configuration
and reader authentication. Native Databricks singleton reads remain independent
of external graph-runtime access.

### Adjacency ordering qualification boundary

Adjacency ordering is relationship-first `(rel_type_id,edge_id)`; clustering
and file pruning qualification MUST use that exact order. A reduced fixture
without structural_version cannot qualify complete adjacency coverage.

Derived adjacency qualification MUST verify the complete schema, explicit
structural revision, full-row parity, contractual page results and source/publication
authority. Low-entropy range writes, CTAS constraints or cached timings cannot
establish universal batching, automatic maintenance or reverse access support.

### Write and adjacency scheduling requirements

Adjacency continuation remains `(rel_type_id,edge_id)` within a fixed endpoint,
direction and publication. Physical clustering and writer ranges must be assessed
against this contractual ordering rather than an edge-ID-only proxy. Keep
canonical identity clustering independent of derived adjacency tuning. Optional
bucket/Z-order copies require full-field preservation and measured write/maintenance
cost; no layout promotion follows from sampled parity or reduced file count alone.
Explicit changed-key ranges, thresholds and isolation/scheduling policy require
qualified evidence. Same-identity physical write conflicts do not establish
source-generation authority or a production writer fence. Serialized current
writes and ordinary pinned reads must preserve exact vectors; scheduling changes
cannot silently change consistency or imply freshness/latency support.

## JSONL streaming adapter development profile

Proposed ashlar-jsonl-transactions/0.1 admits exact-byte raw source batches,
separately from event interpretation, current/history apply and publication.
The binary stream contains begin, zero or more event lines and commit, each
with exactly one LF-terminated UTF8 JSON object. Reject duplicate JSON members,
nonfinite numbers, incomplete lines and unknown/nested control. Unknown event
payload members are retained verbatim and are not executable graph semantics.

Begin is exactly kind=begin and nonempty opaque batch_id. Event requires
kind=event and a nonempty delivery_id unique within that transaction; arbitrary
additional payload is retained. Commit is exactly kind=commit, matching batch_id,
integer record_count and records_sha256. The digest is SHA256 over the ordered
event lines, each preceded by its exact uint64 big-endian byte length; original
LF bytes are included. Matching count/digest proves membership for this explicit
source profile, not producer authority, source retention or safe Truss watermark.
No per-event checkpoint is admitted. Empty transactions have the SHA256 empty
input digest and a zero count; they still require explicit begin/commit custody.

SourceBatch contains profile, feed, epoch, batch_id, cursor_before, cursor_after,
ordered SourceRecords, exact begin/commit bytes and records_sha256. Each record
contains delivery_id, exact line bytes/digest and its ending byte offset as
canonical nonnegative decimal text. Feed/epoch and positioned cursor are trusted
host inputs. Different files must not be assigned the same immutable feed/epoch
without original producer/replay correspondence. Resume requires positioning at
the previously published complete boundary and retaining the original source;
a claimed cursor alone is not custody proof. Never infer XID or graph version
from a byte offset.

The reader defaults to 1000 records and 1MiB total transaction bytes, including
control lines. It refuses before yielding an over-limit/partial batch. Hosts
bound line reads before allocation; the supplied stdin CLI reads at most 1MiB+1
bytes at a time. Prior completed batches may be yielded before a later malformed
transaction; that later failure never admits progress into its partial contents.
Exact replay has the same namespace/batch/delivery/cursor/raw/digest values. The
durable consumer must reject same-key changed bytes and preserve original raw
custody, not merely parsed payloads.

No acknowledgement API is supplied in this reader. Durable staging, source
fencing, schema/operation interpretation, apply validation and immutable complete
publication must precede checkpoint/acknowledgement. Unknown operations retain
raw custody but block dependent current-state publication. This development
profile is distinct from Truss's native transactional feed and PostgreSQL outbox;
their bootstrap, authority, transaction/version and recovery contracts remain
required. The full end-to-end goal includes all three source paths.

## Durable raw batch staging boundary

Proposed source-batch-stage/0.1 is a supplemental raw transaction custody table,
not a replacement for source_record/current/history or a completed publication.
Its nine nonnull STRING columns are source_profile, feed, epoch, batch_id,
cursor_before, cursor_after, records_digest, batch_json and batch_digest. The
logical key is (feed, epoch, batch_id). batch_json is the deterministic stage
encoding of the full SourceBatch, including original begin/commit/event bytes
in base64; batch_digest is SHA256 over its UTF8 text, checked natively. Unknown
source payloads remain byte-exact. Ordered event membership digest remains
separate from this stage encoding digest.

DeltaBatchStage consumes the exact complete JSONL batch, verifies bounded raw
source reconstruction and all supplied metadata, then requires an injected
authorized exclusive-writer context through native UUID verification, parameter-
bound MERGE and exact full readback. No permissive writer policy is supplied.
An identical original row is a replay; any changed field under the same batch
key raises SOURCE_BATCH_CONFLICT without replacing original custody. Missing,
ambiguous, mismatched readback or replacement UUID refuses a staging receipt.
A submission/observation exception is unresolved or failed native work requiring
original statement-handle/custody recovery; never blindly submit a replacement.

StagePolicy.writer must hold authorization and exclusion/fencing for every
admitted writer path until verification and retain recovery custody on unknown
outcome. Its context manager yields None; a returned false/success token is
not policy completion. This port does not itself install native authority,
grant restrictions or concurrency uniqueness. A development profile using administrative writes and local cooperating-process
locks MUST declare its remote/adversarial fencing limitation.

The immutable StagedBatch result records table/UUID, source namespace/batch ID,
batch digest and candidate cursor_after. It is only raw-stage custody, with no
accepted schema, current/history apply, source acknowledgement or checkpoint.
Per-delivery identity/version conflicts across different raw batches still
require the downstream source_record/apply admission; raw batch membership
does not establish those invariants. Publication must validate complete source
and effect membership, schemas, durable replay and pins before progress.

## Whole-entity apply planning boundary

The proposed pure planner consumes an explicitly admitted whole-entity source
profile, not Truss's per-property change feed. EntityKey is the complete
(source, object-or-edge kind, type_id, id); IDs use the selected signed64 range.
EntityState contains exact entity version, schema revision, property/retained
JSON object text and, for an edge, two same-source typed object endpoint keys.
Change adds feed, epoch, original delivery ID/digest and create/replace/delete.
Native source normalization must verify original raw custody and the exact
version/carrier semantics before supplying these values; taking a maximum
property position or guessing a whole-entity version is forbidden.

ApplyState retains current, immutable original history keyed by entity/version,
tombstones and original delivery claims keyed by feed/epoch/delivery. A trusted
complete prior state and explicit schema_policy are mandatory. That policy must
authorize and validate the exact revision, carrier/operation, identity and
relationship constraints for every change, including replay; success returns
None. The pure planner installs no schema/constraint/authority and supplies no
permissive policy. Synthetic planner inputs MUST NOT be represented as accepted catalog authority.

Identical original delivery replay changes no state. Changed original delivery,
entity/version assigned to a different original delivery or non-increasing
version refuses. Create cannot overwrite or resurrect an existing identity;
replace/delete require a live entity. Delete must carry exact prior property,
retained and endpoint meaning with its new version/revision; it removes current
but preserves the original delete in history/tombstones. Every accepted distinct
change remains in original history. A later source lifecycle allowing identity
resurrection needs an explicitly different admitted profile.

Complete boundary validation requires every remaining edge endpoint to resolve
to the exact live typed object. Endpoint creation later in the same transaction
is permitted; a node deletion leaving an incident edge refuses. Parallel edge
IDs and isolated nodes remain distinct. The planner copies prior containers and
returns immutable defensive mappings only after the entire transaction passes;
a refusal leaves prior state unchanged.

This is current/history/tombstone planning, not native table apply, property_journal
derivation, immutable manifest publication or checkpoint advancement. Persisting
its effects requires serialized native state revalidation, retained delivery and
version claims, per-property/native source obligations where applicable, complete
all-table parity and the publication contract. Source acknowledgement still
follows a completed durable publication. Truss-native reconstruction, other
value/key/relationship profiles and full end-to-end native execution remain
required rather than being replaced by these synthetic whole-entity checks.

## Explicit whole-entity JSONL and native apply

changes_from_batch composes complete raw transaction validation with an explicit
ashlar-whole-entity/0.1 event profile. Required fields are kind=event, delivery_id,
source_profile, source_system, entity_kind, type_id, id, entity_version,
schema_revision, operation, props_json and retained_json. Edge events additionally
require exactly two endpoints, each with type_id and id. Identity/version values
are canonical signed64 decimal strings; nontext values, aliases, leading zeroes
and narrowing refuse. Versions are source-authored whole-entity versions. No
property-feed maximum, display-name mapping or hidden source default is used.
Unknown or missing executable members block this profile while original raw
source custody remains available separately. Exact JSON property/retained text
is passed unchanged to the mandatory admitted apply planner.

Whole-entity examples MUST distinguish fixture schema/property IDs from accepted
catalog authority, preserve explicit null and exact retained large-integer tokens,
and avoid inferring semantic support merely from stored text.

Native whole-entity application writes touched canonical identities and original
source history; intermediate multi-table effects are not a publication. History
must retain original full event bytes and digests. Source schema admission, native
writer/recovery authority and complete immutable publication remain separate.
Current-row publication-clock metadata alone does not establish publication.

## PostgreSQL transactional outbox source profile

Proposed ashlar-postgresql-outbox/0.1 is a separate additional source, not a Truss
native mutation/journal/feed substitute. Fresh private source DDL is
sql/ashlar-outbox/01-postgresql.sql. One selected namespace has a singleton
signed64 nonnegative head and immutable-by-admitted-writer batch rows keyed by
positive position with exact unique opaque batch_id, UTF8 payload text and
SHA256 byte digest. Payload is a 1-byte to1MiB opaque complete JSONL group. Native
append preserves bytes; the consumer independently verifies the contained
begin/event/commit count/digest and identity before admitting a source batch.
Opaque storage alone is not graph or schema interpretation.

The SECURITY DEFINER append function fixes its search path and locks the head
through the actual caller transaction. Exact existing batch bytes return the
original position without allocation; different bytes raise OUTBOX_BATCH_CONFLICT.
Fresh allocation inserts payload/digest and advances head in that same outer
transaction. The returned scalar is pending until actual commit; rollback
restores both payload/head. Position exhaustion refuses. The selected namespace
serializes admission/commit order on that head; cross-host blocking, fairness,
resource and lost-commit qualification remain separate. No scale/SLO follows
from the small sequential test.

PUBLIC has no schema/table/function access. The NOLOGIN writer role has only
namespace usage and append execution; direct batch/head DML is denied. The
NOLOGIN reader has only namespace usage and source-table SELECT, no append.
Native owner/admin authority remains separate; this development installation
does not provision application login/credential or remote service authority.
It changes no Truss table, catalog revision, mutation or feed registration.

PostgresOutbox reads a committed head then its bounded ordered original prefix.
Default page is10 groups, maximum32; each original payload is at most1MiB.
Head/cursors are canonical decimal strings in signed64 range, never host floats.
Missing, duplicate, unordered or corrupt original groups and an ahead-of-head
checkpoint refuse. Hashes and full original JSONL group membership/identity are
verified before the complete page is returned. Exact original byte payloads
and contained unknown content survive read/parse; unbound executable meaning
remains the downstream admission boundary.

OutboxTransaction carries profile, registered feed/epoch, previous, native
position, payload_digest and the original contained SourceBatch. Native
PostgreSQL sequence positions are distinct from that contained JSONL's local
byte offsets. Publish/acknowledge only the outer source cursor under an explicit
registered adapter; passing its inner SourceBatch to a JSONL publisher does not
admit PostgreSQL progress. Source connection/schema identity and immutable
feed/epoch registration are trusted host obligations; arbitrary caller strings
do not establish producer authority. Retain committed rows beyond every
registered consumer boundary. No cleanup, acknowledgement or new epoch reset
is supplied by this reader.

Application producers must couple source append to their own actual write
transaction and implement original lost-commit recovery; this profile is not
automatic capture of arbitrary SQL changes. Require committed/rolled-back groups, replay/conflict, role restrictions and
original-byte pagination tests. External write/effect correspondence, source/epoch
registration and publication/checkpoint wiring require separate qualification.

### Native clock custody

New native fixture applications retain one original server microsecond clock
through DurableSQL before graph effects. The request binds the exact source
batches, outer checkpoints and namespace digest, authenticated authority and
warehouse. Completed responses replay without a replacement request; pending
handles recover through GET; submission uncertainty without a handle refuses.
Clock values must be canonical nonnegative signed64 microseconds and fit the
Python datetime carrier without floating-point rounding.

Reconstructed effect plans MUST match original retained clock/request/effect
custody exactly. A server clock is neither Delta commit time nor publication,
retention, accepted catalog ID or remote writer-fence evidence. Independent
full-column parity must derive expectations from original source events.

## Native setup admission renewal

The setup_native.py host installer consumes only checked UMF-generated carrier
proposals and retains original SQL submissions/UUIDs in its same-host journal.
Before creating a schema or carrier it MUST renew current actor and complete
catalog/schema owner-lane effective-permission admission. Raw API pages are
retained before interpretation; inherited, unknown and partial content follows
the bounded effective-permission reader's refusal rules. Native table admission
MUST verify a current Unity Catalog MANAGED table. It MUST compare every installed
carrier's exact ordered columns with the generated model and renew owners,
permissions and original UUIDs before reporting readiness. Generated source/model
custody MUST still match at completion. No CREATE IF NOT EXISTS or replacement
submission may hide an existing target or uncertain native outcome.

The raw-registry and carrier-setup tools share NativeOwnerLane for these native
observations. This is qualified development owner admission, not remote fencing,
immutable administrator restrictions, publication readability or accepted schema.
It changes no grants, predictive optimization or table retention settings beyond
explicit original UMF-generated CREATE intent.

## Execution evidence

Current results and remaining work belong in the [execution plan](../../04-build/end-to-end-plan.md).
The [original contract text](../../04-build/evidence/contract-history-20261009/CONTRACT-003-delta-graph-tables.original.txt)
preserves prior execution diaries verbatim; those results do not amend this contract.
