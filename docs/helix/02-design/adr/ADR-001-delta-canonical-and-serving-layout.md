---
ddx:
  id: ADR-001
  type: adr
  activity: design
  status: proposed
  authoring:
    home: repo
  links:
  - id: FEAT-002
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
---

# ADR-001: Generic Delta canonical tables with typed serving projections

## Context

Ashlar must support low-latency singleton lookup directly on Databricks and a
planning graph of 1B nodes with more edges. The physical layout should remain
close to Truss while mapping explicitly to PuppyGraph, GraphFrames and Microsoft
Fabric Graph. Truss-style property-ID maps, typed endpoints, retained content and
property history must survive replication. PostgreSQL indexes, foreign keys and
partition mechanisms do not transfer automatically to Delta.

## Decision

Use Unity Catalog managed Delta tables as canonical storage. Performance
measurements guide physical tuning and capacity planning; they do not determine
whether this storage architecture is used.

Use generic `object_current` and `edge_current` carriers, preserving native
source/type/entity identities, exact property values and uninterpreted retained
content. Add rebuildable typed scalar projections for selected workloads and
engine mappings. Native singleton lookup reads canonical Delta at a pinned,
readable publication boundary, independently of external graph refresh.

### Physical access and maintenance

Start canonical tables without explicit partition directories and use
identity-oriented liquid clustering. Derive `lookup_hash` with SHA-256 from the
exact source/type/id tuple; retain the complete tuple as the semantic key and
exact lookup predicate. Hashes provide pruning, never identity, uniqueness or
endpoint authority. Edge identity includes relationship and edge ID; endpoints
alone cannot identify parallel edges. Use bound query parameters.

Use 64MiB as an initial file-size tuning target, not a correctness constraint.
Select partitioning, Z-order alternatives, file targets and statistics from
whole-workload costs, including reads, ingest, maintenance and retained storage.
Include journal batch IDs and identity columns in data-skipping statistics.
Trigger maintenance from file overlap and read amplification rather than running
full cleanup after every batch or blindly visiting every hash range.

Preserve predictive optimization. Publication readability must depend on the
actual optimization configuration and retained snapshot guarantees. Physical
maintenance cannot repoint an existing immutable publication: exposing a
maintained snapshot requires a separately validated publication.

### Table roles and source preservation

The durable data roles are `object_current`, `edge_current`, `property_journal`,
`source_record`, `tombstone` and `publication_manifest`. Complete raw source
records and accepted semantic journal events preserve different information;
neither substitutes for the other. Preserve transport envelopes, source cursors,
revision documents, provenance and uninterpreted operations. An unknown operation
that can affect current-state interpretation stops publication.

Raw-record idempotence uses qualified feed/epoch/delivery identity with byte-exact
cursor/kind/payload/revision conflict detection. Delta MERGE is not a concurrency
safe unique constraint. Require serialized, fenced writer authority or another
independently proved protocol. Advance source checkpoints only after the relevant
publication is durable; remote acknowledgement requires its own recovery proof.

Optional structural roles are forward adjacency, reverse adjacency and degree
summaries. Choose direction and relationship coverage from query requirements.
An unavailable projection means an unsupported capability, never zero edges.
Typed node/edge projections carry independent edge IDs and complete typed endpoint
keys, retaining isolated nodes, self-loops and parallel edges.

Publisher authority and apply receipts must be durable, with complete predecessor,
input and output bindings, even if the chosen mechanism stores them outside Delta.
Enforce semantic uniqueness, relationship validity and final-boundary endpoint
integrity through the publisher; descriptive model constraints are insufficient.

### Publication and write amplification

Bind every consumed table by native identity and committed version in an immutable
publication vector. Table versions need not share numbers or commit times. Rewrite
only changed roles: a property-only update need not rewrite unchanged adjacency,
degree or unrelated projections. An unchanged role may reuse its previously
validated committed version.

Use immutable validated predecessors and complete authoritative change sets for
incremental publication. Replacing exhaustive comparison requires proof of source
completeness, writer authority, affected-row preservation and projection coverage.
Use exhaustive bootstrap and periodic audits without making full-graph comparison
a requirement for every batch.

### Engine releases and scale

Publish engine-specific immutable releases with explicit publication bindings,
identity mappings, scalar casts, residual content and Delta feature compatibility.
GraphFrames consumes pinned Spark frames. PuppyGraph and Fabric require independent
release activation, readback, rollback and refresh semantics. Export compatible
snapshots when a reader cannot consume canonical Delta features directly.

Keep Fabric exports within the actual target's supported element and capability
limits; do not assume a bounded export supports the full planning graph. External
releases may refresh less often than native publications, but must expose the
publication they represent.

Account for canonical data, raw records, journal fanout, tombstones, forward/reverse
adjacency, typed releases, retained versions, staging, logs and failed work in
capacity and cost estimates. The 5B-edge planning assumption is not a measured
production history profile. Retention and expiry require reader-pin and recovery
proof; capacity estimates cannot authorize destructive cleanup.

## Module Boundaries

**Source Applicability**: source; the portable `ashlar` library, installed
`ashlar_host` application package and checkout-only experiments have distinct
semantic, custody and transport responsibilities.

| Module | Responsibility / Owned Types | Public API | Allowed Dependencies | Forbidden Dependencies |
| --- | --- | --- | --- | --- |
| `src/ashlar/schema.py`, `catalog.py`, `binding.py`, `semantic_policy.py`, `typed_source_policy.py` | Retained model/catalog bindings and public-UMF receipt custody | SchemaIntake, binding/policy admission | Portable value codecs and original UMF receipts | Runtime SDKs, tools, private UMF semantic implementation |
| `src/ashlar/source.py`, `apply.py`, `whole_entity.py`, `source_checkpoint.py` | Source batches, prospective graph state and qualified feed checkpoints | SourceBatch, plan_apply, checkpoint binding | Model/binding types, portable structural codecs | Live engine clients, ambient credentials, host orchestration |
| `src/ashlar/publication.py`, `publisher.py`, `stored_publisher.py`, `recovery.py`, `manifest.py` | Immutable descriptors, attempt state and original-operation recovery | resolve_publication, publisher/backend protocols | Source state and explicit storage/policy ports | Tools, SDK discovery, telemetry as durable authority |
| `src/ashlar/native.py`, `staging.py`, `attempt_store.py`, `schema_registry.py`, `pins.py`, `authority.py`, `retention_policy.py` | Native SQL, pin/authority and retained-store adapters | Executor/store/policy APIs named in CONTRACT-004 | Core descriptors and injected native transport | Source semantic reinterpretation, tools, automatic permissive policy |
| `src/ashlar/weft_binding.py`, `weft_query.py`, `weft_decode.py`, `weft_path_decode.py`, `graph_release.py`, `singleton.py` | Qualified consumer bindings, buffered decoding and graph-release projection | read_weft, exact scalar/path decoding, graph release and singleton APIs | Resolver, admitted model and explicit transport/policy ports | Producer ACK mutation, host result repair, compiler semantic substitution |
| `src/ashlar/weft_related_keys_decode.py` | Pure required-root one-hop String-key carrier validation; RelatedKeysDecodeConfig and DecodedRelatedKeys | decode_related_keys, governed by CONTRACT-004 | Standard-library immutable values/JSON; explicit admitted descriptors/model pins | Runtime SDKs, host/tool imports, ambient settings, bag deduplication, inferred native integrity or truncation truth |
| `src/ashlar/weft_installation.py` | Indexed compiler package custody, installation availability and bounded executable transport | InstallationConfig, inspect_package, install, open_installation, compile_request | Standard-library byte/file/process adapters and injected trusted index/platform observations | Producer ACK mutation, ambient trust/platform discovery, compiler result repair, automatic network or native engine selection |
| `src/ashlar/weft_paths_package.py` | Bounded verification of the explicit Paths distribution profile and its complete artifact/receipt closure | PathsInstallationConfig, inspect_package | Standard-library byte/file adapters and injected trusted index/platform observations | Self-registration, implicit profile selection, relaxation of the old installation profile, native SDKs or source/ACK authority |
| `src/ashlar/weft_paths_installation.py` | Local Paths installation, restart verification, retained schema bytes and bounded one-shot compiler transport | PathsInstallationConfig and inspect_package re-exports; install, open_installation, compile_request, installed_schema_bundle | Owned package verification APIs and standard-library file/process adapters | Compiler fallback, request/response rewriting, ambient trust discovery, native SDKs or producer ACK mutation |
| `src/ashlar/weft_paths_distribution.py` | Trusted Paths realization selection, observed platform admission and operator-owned file locations | PathsDistributionPaths, PathsDistributionError; install_paths_distribution, open_paths_distribution, compile_paths_distribution, installed_paths_schema_bundle | Public Paths installation APIs and standard-library platform observation | Operator overrides of trust pins, realization or platform; compiler fallback, host/tool/SDK imports, source/ACK authority or protocol repair |
| `src/ashlar/weft_paths_keys_package.py` | Bounded verification of the separately selected PathsKeys package; current source/producer proof and retained historical qualification remain distinct | PathsKeysInstallationConfig, inspect_package | Standard-library byte/file adapters and injected trusted index/platform observations | Self-registration, historical proof relabeling, old-profile relaxation, native SDKs or source/ACK authority |
| `src/ashlar/weft_paths_keys_installation.py` | PathsKeys installation, restart custody, retained schemas and bounded compiler transport | install, open_installation, compile_request, installed_schema_bundle | Public owned package verification and private installation mechanics | Profile fallback, protocol repair, ambient trust discovery, native SDKs or producer ACK mutation |
| `src/ashlar/weft_paths_keys_distribution.py` | Fixed trusted PathsKeys realization selection and observed platform admission | Named PathsKeys distribution composition APIs | Public PathsKeys installation APIs and standard-library platform observation | Operator trust/profile/platform overrides, host/tool/SDK imports or source/ACK authority |
| `src/ashlar/_weft_installation_mechanics.py` | Shared exclusive writes, staging and bounded transport over immutable internally owned closed layouts | Private implementation support; no package export | Standard-library file/process adapters and owned immutable values | Package proof/profile ownership, producer/composition/tool/SDK imports or caller-selected unchecked layouts |
| `src/ashlar/durable_publisher.py`, `effect_validation.py`, `protocol.py`, `schema_policies.py`, `quarantine.py`, `maintenance.py`, `retention.py`, `retention_policy.py`, `profile_custody.py`, `lineage.py`, `origin.py`, `assertions.py`, `report_parts.py`, `outbox.py` | Durable effect/authority composition, maintenance, profile custody and protocol codecs | Named module APIs governed by CONTRACT-001–005 | Core records and injected policy/transport interfaces | Tool imports, ambient SDK construction, silently permissive authority |
| `src/ashlar/truss_input.py`, `truss_feed.py`, `csv_source.py` | External source framing and exact retained input translation | Source adapter APIs | Core source/schema records and supplied public receipts | Shadow semantic validators, implicit source installation discovery |
| `src/ashlar/__init__.py` | Portable package exports | Explicit core package surface | Owned portable library modules | Host composition, engine SDK construction or new semantic ownership |
| `src/ashlar/cli.py`, `__main__.py` | Installed command dispatch and typed application construction | CLI main, including publish-commerce and query-commerce | Public core APIs and explicit `ashlar_host` configuration/commerce entrypoints | Native SDK imports/construction, tool imports, semantic ownership or private cross-module access |
| `src/ashlar/source_config.py`, `weft_distribution.py`, named `*_source.py` adapters | Named source profiles and trusted compiler configuration | Source conversion and indexed compiler composition | Public core APIs and standard-library file/process adapters | Imports of `ashlar_host`, tools or native SDKs; compiler/source semantic substitution |
| `src/ashlar_host/__init__.py`, `config.py`, `commerce.py` | Typed immutable application and diagnostic configuration, runtime/input admission and public workflow selection | HostError, ProducerConfig, PrivatePostgresConfig, PublishCommerceConfig, QueryCommerceConfig, QueryCommercePathsConfig, DiagnosticsConfig, DiagnosticsLimits, SecretText; commerce.publish_commerce and commerce.query_commerce | Explicit configuration, packaged inputs and owned host phase adapters | Checkout tools, implicit endpoint/profile selection, configuration treated as source or ACK authority |
| `src/ashlar_host/diagnostics.py` | Sanitized event identity, bounded local run capture and closed-snapshot retrieval | CONTRACT-006 event/run validation and capture; read_diagnostics | Standard-library immutable values and file adapters, explicit DiagnosticsConfig and injected signal sink | SDK imports, ambient settings, raw payload/secret capture, publication/ACK mutation ports or checkout tools |
| `src/ashlar_host/otel.py` | OpenTelemetry (OTel) SDK construction, exact signal mapping and bounded export/shutdown | CONTRACT-006 log/span/metric emission and bounded close adapter | Explicit DiagnosticsConfig, validated diagnostic values and lazily loaded selected OTel SDK/exporter | Native/publication SDKs, ambient endpoint discovery, raw payloads, compiler/result repair, publication/ACK mutation ports or checkout tools |
| `src/ashlar_host/source.py`, `source_identity.py`, `commerce_admission.py`, `resources/`, `schema_rows.py` | Original input custody, public UMF producer invocation, finite source admission and package-relative model/graph/SQL resources | Owned source/resource ports used by the commerce entrypoints | Public core source APIs, explicitly configured Bun/Git and clean pinned UMF source, owned native/source correspondence ports | Checkout-relative resource discovery, private UMF imports, shadow semantic validation, receipt flags treated as publication authority |
| `src/ashlar_host/path_admission.py`, `path_capture.py`, `path_execution.py` | Immutable path artifact/schema admission, bounded result capture and provisional execution within a caller-owned publication hold | PathAdmissionConfig, admit_path_artifact; PathCaptureConfig, capture_string_frame; PathExecutionConfig, execute_commerce_path | Public core exact decoders, owned admission/capture APIs and explicit schema, source, reader and execution ports | Engine construction, compiler installation/discovery, source/ACK authority, compiler SQL/result repair or successful report release before outer cleanup |
| `src/ashlar_host/path_schema.py` | Offline validation against exact installed Paths schemas | make_offline_path_schema_validation; owning PathSchemaValidation port in path_admission | Installed schema bytes, lazy JSON Schema validator and offline reference registry | Network schema retrieval, compiler or source authority, native clients |
| `src/ashlar_host/commerce_path_request.py`, `commerce_path_oracle.py` | Original commerce request construction and independent source-derived expected results | commerce_path_request, commerce_path_cases, original_commerce_path_oracle | Public publication-reader request and original model/graph values | Compiled SQL as expected-result authority, invented identities/relationships, tool imports |
| `src/ashlar_host/paths_query.py` | Separately selected indexed Paths workflow and outer publication/cleanup lifecycle | query_commerce_paths; configuration owned by config.QueryCommercePathsConfig | Trusted Paths distribution APIs, public admission/execution and explicit producer/reader/runtime ports | Legacy compiler fallback, ACK advancement, report release before closing checks and cleanup |
| `src/ashlar_host/delta_publication.py`, `delta_query.py`, `driver.py`, `delta_custody.py`, `graph_sql.py`, `publication_reader.py`, `indexed_query.py`, `relationship_plan.py`, `relationship_query.py`, `runtime.py`, `lifecycle.py` | Native transport/journal custody, publication composition, held query execution, runtime selection and cleanup | Host-internal driver/reader/phase ports; applications enter through commerce.publish_commerce or commerce.query_commerce | Public core publication/resolver/source/compiler APIs, admitted policy ports, configured Spark/Delta and owned ACK/session adapters | Tool imports, rewritten compiler SQL or repaired results, fabricated source/ACK authority, success release before required closing checks and cleanup |
| `src/ashlar_host/ack.py`, `postgres.py`, `connection.py` | Protected source acknowledgement and reconciliation, ordinary PostgreSQL sessions and private-profile connection construction | Host ACK/session ports: AckScope, ProtectedOutboxAck, AckOutcomeUncertain, Session | Public core descriptor/checkpoint/pin APIs, mandatory policy and connection ports, packaged ACK SQL and explicitly selected psycopg profile | Caller flags as ACK authority, source progress inferred from query results, checkout tools or unqualified production credential discovery |
| `tools/` adapters | Checkout-only engine/source experiments and review utilities | Named runner/checker entrypoints | Owned public core/host APIs and explicitly selected SDKs | Installed package dependency on tools; newly introduced private cross-module access |

**Integration Owners**: UMF -> core schema/typed-source policy receipt boundaries
and `ashlar_host.source` for explicit public-producer invocation; Truss ->
`truss_input.py`/`truss_feed.py`. Weft -> `weft_binding.py`/`weft_query.py` for
portable publication reads, `weft_installation.py` for indexed transport,
`weft_distribution.py` for its trusted compiler composition, and
`weft_paths_package.py`/`weft_paths_installation.py` for the separately selected
Paths package and executable transport; `weft_paths_distribution.py` owns their
trusted application composition. The separately selected PathsKeys package and
transport belong to `weft_paths_keys_package.py` and
`weft_paths_keys_installation.py`; `weft_paths_keys_distribution.py` owns its
fixed trusted composition. Shared private `_weft_installation_mechanics.py`
owns exclusive writes, staging and bounded transport only. Each profile retains
its own proof constants, defaults and verification; historical qualification
cannot become new execution evidence. Installed `ashlar_host` indexed/relationship
query adapters own their held native execution; `path_admission.py`,
`path_capture.py` and `path_execution.py` own path artifact and result custody.
Delta SQL -> `native.py` plus `ashlar_host.delta_custody`/`driver`; PostgreSQL
ACK -> `ashlar_host.ack`, with `postgres` session handling and `connection`
construction. OpenTelemetry -> `ashlar_host.otel` for SDK signal mapping and
export; `ashlar_host.diagnostics` owns the local capture/retrieval projection,
and `ashlar_host.config` owns its typed settings under CONTRACT-006. Private
graph-engine experiments retain their named `tools/run_*`
and `tools/check_*` owners. Vendor/source translation stays at these boundaries;
a private host profile does not establish production source or engine authority.

**Construction Policy**: the CLI or named host entrypoint builds configuration,
transport and policy ports once and injects them. `ashlar_host` package import
exposes configuration without loading SDKs or starting native clients. Its commerce
entrypoints validate the selected configuration and runtime before constructing
phase-specific clients. Package-relative resources own the installed model, graph,
validator script and SQL; explicit external producer/runtime paths remain operator
configuration. `connection.py` owns credential acquisition only within the selected
private PostgreSQL profile. Required reader/session/transport closure and Spark
stop must complete before a successful report is released; cleanup failure must
not replace an existing primary failure or turn an uncertain outcome into success.

The `ashlar_host.commerce` and `ashlar_host.paths_query` composition roots receive
an explicit `DiagnosticsConfig` or `None`. They validate it before operation
effects, construct the owned diagnostic capture and OTel adapter once, and inject
their public ports; `None` disables diagnostics without SDK discovery. OTel SDK
construction belongs only to `ashlar_host.otel` in the selected Python 3.11 host
runtime. The portable core remains Python 3.9 compatible and has no OTel dependency.
Diagnostics observe the owning workflow's outcome and preserve primary failures;
their loss or export failure cannot authorize publication, ACK or result release.

`weft_paths_distribution.py` fixes the trusted index revision/digest and
realization, observes the supported host platform, and accepts operator locations
only for the index, package and local installation. It supplies these values to
the typed Paths installation configuration. The `ashlar-databricks-paths` build
selects `weft-compile/0.4.0`, `weft-sql/0.4.0`, `weft-ir/0.4.0`,
`weft-backend/0.3.0` and `spark4-delta4-paths-candidate`. Package verification binds
the exact source, executable, features, schemas, backend manifest and complete
declared corpus. Verify the installed tree and ready record before
making the component available, including on restart; retain the verified schema
bundle with the installation. Failure leaves it unavailable without fallback.
Index trust comes from application composition, never the caller's package or a
passing corpus. These duties follow Weft CONTRACT-003's DIST-F1–F4 and DIST-L1;
installation grants no native execution, publication, source or ACK authority.
Path execution receives admitted ports and returns provisional evidence; the
outer composition owns reader/client closure and final report release.

The only `src/ashlar` files permitted to import `ashlar_host` are the exact
composition roots `src/ashlar/cli.py` and `src/ashlar/__main__.py`. A matching
basename in a nested package grants no exception. Other core modules, including
source and compiler composition adapters, cannot depend on the installed host.
Host modules may compose owned public core APIs; neither installed package may
import checkout tools. Concrete SQL coupling in native adapters is deliberate;
no container or interface-per-function is required. Core invariants and exact
shared surfaces remain owned by CONTRACT-001–005.

**Boundary Check**: `python3 tools/check_module_boundaries.py`; enforce the
project map with recursive Python AST imports across `src/ashlar`,
`src/ashlar_host` and `tools`, plus an individually identified existing-debt
inventory. Use the same command locally, in pre-commit and CI. Baseline entries
identify exact importing file, target, symbol, owner and removal trigger; no
directory exemption admits new violations. Check core-to-host composition roots,
core/host-to-tools edges, portable-core SDK imports (including `opentelemetry`), producer/consumer direction
and private cross-module accesses. Dynamic imports, runtime reflection,
dependency cycles and value/type ownership require explicit semantic review;
static import success alone cannot establish encapsulation.

## Configuration and diagnostic boundaries

Entrypoints validate a single typed immutable configuration before constructing
SDK clients or performing effects. For the new application configuration composition, record each nonsecret setting's origin:
explicit arguments > environment/secret files > development-only .env >
environment TOML > default TOML > field defaults. Existing explicitly qualified runtime profiles retain their selected configuration
contract; new precedence cannot silently reinterpret them. Operator-owned endpoint,
warehouse, source installation, authority and supported profile fields are
required and have no defaults; never silently select shared/default compute. Each
setting has one owner. Committed defaults and environment TOML contain only
developer-owned, nonsecret values; endpoints, credentials and other operator-owned
handles come from injected sources. An injected environment selector chooses the
reviewed environment file. Operator overrides of developer-owned values are
incident overrides; standing environment differences belong in that file.
Secrets use secret types
and never enter printable settings, fingerprints, SQL diagnostics or subprocess
capture. Library calls receive explicit configuration/policy ports rather than
reading process environment. Browser configuration is a separate public,
browser-safe closed boundary and cannot contain credentials.

OpenTelemetry (OTel) governs diagnostics, independently of durable attempt/manifest/ACK custody.
Publication, source admission, resolver-held read and ACK/reconciliation are
operation spans; durable phase changes and refusal categories are events;
aggregate outcomes are low-cardinality metrics. Preserve optional valid trace
context, use links for independently retried work, and never invent trace IDs.
The host owns one export route and bounded JSONL run capture with safe evidence
references. Redact/allowlist before every sink, reserve bounded record/queue
sizes, disclose sampling/drop/capture limits, and attempt bounded flush at close.
Exporter failure cannot authorize ACK or change commit classification. Protocol
stdout stays clean; stderr shows meaningful outcomes. Raw model/rows, SQL values,
credentials and source payloads are excluded from telemetry by default. Exact
signal schema/version, ordering, retention and export mapping belong in a
separately reviewed contract before an OTel integration claim.

## Publication assurance policy

Publication/recovery uses precise reviewed safety and conditional liveness
specification, implementation correspondence and existing bounded tests.
[TD-001](../technical-designs/TD-001-publication-recovery.md) owns states, transitions, properties
and assumptions. A separately reviewed bounded analysis may increase assurance;
it cannot establish a production writer fence or native snapshot retention.

## Alternatives

| Option | Benefits | Costs | Evaluation |
| --- | --- | --- | --- |
| Generic property bags only | Closest fixed table surface to Truss | JSON parsing and poor property statistics; awkward scalar graph mapping | Keep as canonical, not sole analytic surface |
| Typed canonical tables for every type | Native scalar filtering and direct graph mapping | Canonical schema migration per type; diverges from Truss | Not proposed as canonical |
| Shared canonical plus scalar serving projections | Preserves Truss semantics; selected types map cleanly | Extra storage, synchronization and revision checks | Proposed physical layout |
| Shared promoted columns only | Fewer serving tables | Global wide sparse schema, unrelated-type evolution | Optional workload-specific projection |
| Property EAV as canonical Delta | Property journal resemblance | Reassembly and join amplification for object reads | Use for semantic history |

## Consequences

Canonical schema evolution remains generic. Selected analytical properties add
versioned scalar columns and projection validation. Unknown content remains
preserved in canonical retained text. The publication manifest and engine releases
add synchronization and storage costs in exchange for explicit, reproducible
query boundaries.

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Singleton latency or cost grows at 1B nodes | Capacity or service objectives missed | Measure cold/warm/concurrent reads and bytes/files; tune execution/layout while retaining UC Delta and exact identity |
| Clustering features rejected by external Delta reader | Integration fails | Verify exact protocol/features; explicit immutable compatible export |
| Serving copies and reverse edges dominate cost | Storage/refresh budgets missed | Promote only measured workload; count canonical+all projections+retained versions |
| Truss property feed mapped as whole-document writes | Lost version/transaction semantics | Preserve journal; explicit adapter and differential corpus |
| Independent table writes leak partial state | Incorrect query answers | Version manifest and pinned reads; engine-specific immutable release proof |

## Validation

Verify complete carrier, identity, endpoint and multiplicity preservation;
source replay/conflict handling; interrupted publication recovery; pinned reads;
retention; and engine-specific release behavior. Measure singleton, traversal,
ingest and maintenance performance with workload, compute and cost scope. Keep
new checks small and reuse existing measurements. Connector support requires
actual connector-specific execution evidence.

The table contract is [CONTRACT-003](../contracts/CONTRACT-003-delta-graph-tables.md).
Historical measurements and implementation observations are retained in
[build evidence](../../04-build/evidence/documentation-history-20261009/ADR-001.original.txt).

## References

- [Truss ADR-002](/Users/erik/Projects/truss/docs/helix/02-design/adr/ADR-002-storage-strategy.md)
- [Databricks clustering](https://docs.databricks.com/aws/en/tables/clustering)
- [Fabric limitations](https://learn.microsoft.com/en-us/fabric/graph/limitations)
