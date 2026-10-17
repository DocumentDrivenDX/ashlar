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
| `src/ashlar_host/supply_chain_graphframes_cli.py` | Installed existing-publication common GraphFrames command and candidate runtime inspection | run_supply_chain_graphframes_command, run_inspect_graphframes_runtime_command | Actual original common owner and isolated runtime, closed native setup selection | Publication, compiler execution, custom provider or preparation as authority |
| `src/ashlar_host/graphframes_isolated_runtime.py`, `graphframes_runtime_probe.py` | Genuine selected-worker runtime admission while canonical reader uses another SDK | hold_isolated_graphframes_runtime, bounded worker probe | Existing current-interpreter runtime policy, exact complete namespaces/files/JARs, owned process transport | Parent metadata replacing actual child admission, source/export authority or production fencing |

| `src/ashlar_host/supply_chain_graphframes.py` | Original supply-chain common GraphFrames correspondence | prepare_supply_chain_graphframes_profile; execution through graphframes_release.execute_supply_chain_graphframes_release | Actual held common owner, independent original carrier oracle, isolated GraphFrames runtime | Caller source policies, export hashes as authority, scalar promotion or authored-scenario claims |
| `src/ashlar/schema.py`, `catalog.py`, `binding.py`, `semantic_policy.py`, `typed_source_policy.py` | Retained model/catalog bindings and public-UMF receipt custody | SchemaIntake, binding/policy admission | Portable value codecs and original UMF receipts | Runtime SDKs, tools, private UMF semantic implementation |
| `src/ashlar/source.py`, `apply.py`, `whole_entity.py`, `source_checkpoint.py` | Source batches, prospective graph state and qualified feed checkpoints | SourceBatch, plan_apply, checkpoint binding | Model/binding types, portable structural codecs | Live engine clients, ambient credentials, host orchestration |
| `src/ashlar/publication.py`, `publisher.py`, `stored_publisher.py`, `recovery.py`, `manifest.py` | Immutable descriptors, attempt state and original-operation recovery | resolve_publication, publisher/backend protocols | Source state and explicit storage/policy ports | Tools, SDK discovery, telemetry as durable authority |
| `src/ashlar/native.py`, `staging.py`, `attempt_store.py`, `schema_registry.py`, `pins.py`, `authority.py`, `retention_policy.py` | Native SQL, pin/authority and retained-store adapters | Executor/store/policy APIs named in CONTRACT-004 | Core descriptors and injected native transport | Source semantic reinterpretation, tools, automatic permissive policy |
| `src/ashlar/phase_read.py`, `src/ashlar_host/phase_absence.py` | Nominal core phase-read observations and bounded zero-only full-ledger absence owner | PhaseReadRequest, PhaseAbsence; PhaseAbsenceOwner under CONTRACT-001 | Actual open core session, original run correspondence, active transport history witness and native identity | Caller boolean grants, SQL interception, retained/completed row reuse, source/ACK caching, mutation/recovery optimization |
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
| `src/ashlar_host/__init__.py`, `config.py`, `commerce.py` | Typed immutable application and diagnostic configuration, runtime/input admission and public workflow selection | HostError, ProducerConfig, PrivatePostgresConfig, PublishCommerceConfig, QueryCommerceConfig, QueryCommercePathsConfig, DiagnosticsConfig, DiagnosticsLimits, SecretText, load_diagnostics_config, with_diagnostics_transport; commerce.publish_commerce and commerce.query_commerce | Explicit configuration, packaged inputs and owned host phase adapters | Checkout tools, implicit endpoint/profile selection, configuration treated as source or ACK authority |
| `src/ashlar_host/diagnostics.py` | Sanitized event identity, bounded local run capture and closed-snapshot retrieval | CONTRACT-006 event/run validation and capture; decode_event, validate_signal_loss, read_diagnostics | Standard-library immutable values and file adapters, explicit DiagnosticsConfig and injected signal sink | SDK imports, ambient settings, raw payload/secret capture, publication/ACK mutation ports or checkout tools |
| `src/ashlar_host/diagnostic_composition.py` | Explicit diagnostic dependency admission, run/adapter ownership and host invocation wiring | CONTRACT-006 selected workflow composition | Public typed configuration, capture and OTel lifecycle ports; injected business workflow | Native/publication/ACK ports, ambient discovery, business exception cleanup classification changes or checkout tools |
| `src/ashlar_host/otel_profile.py` | Selected immutable sixteen-package diagnostic dependency profile | SDK_VERSIONS under CONTRACT-006 | Standard-library immutable mappings | SDK construction, discovery, mutable profile overrides or work authority |
| `src/ashlar_host/otel.py` | OpenTelemetry (OTel) adapter composition, explicit parent/retry and span-creation binding, and owned worker lifecycle | make_otel_run, OtelStartupError; OtelRun signal sink, operation_context and bounded shutdown under CONTRACT-006 | Explicit DiagnosticsConfig, public sanitized diagnostic APIs, selected OTel API context values and bounded private worker protocol | In-process SDK provider construction, ambient endpoint discovery, raw payloads, publication/ACK mutation ports or checkout tools |
| `src/ashlar_host/_otel_worker.py` | Private isolated SDK construction, actual context/signal mapping, bounded queues and transport | None; private worker protocol owned by the OTel adapter | Public sanitized event validation, selected OTel SDK/encoders and explicit injected settings | Native/publication SDKs, ambient configuration overrides, compiler/result repair, publication/ACK mutation ports or checkout tools |
| `src/ashlar_host/source.py`, `source_identity.py`, `commerce_admission.py`, `resources/`, `schema_rows.py` | Original input custody, public UMF producer invocation, finite source admission and package-relative model/graph/SQL resources | Owned source/resource ports used by the commerce entrypoints | Public core source APIs, explicitly configured Bun/Git and clean pinned UMF source, owned native/source correspondence ports | Checkout-relative resource discovery, private UMF imports, shadow semantic validation, receipt flags treated as publication authority |
| `src/ashlar_host/path_admission.py`, `path_capture.py`, `path_execution.py` | Immutable path artifact/schema admission, bounded result capture and provisional execution within a caller-owned publication hold | PathAdmissionConfig, admit_path_artifact; PathCaptureConfig, capture_string_frame; PathExecutionConfig, execute_commerce_path | Public core exact decoders, owned admission/capture APIs and explicit schema, source, reader and execution ports | Engine construction, compiler installation/discovery, source/ACK authority, compiler SQL/result repair or successful report release before outer cleanup |
| `src/ashlar_host/path_schema.py` | Offline validation against exact installed Paths schemas | make_offline_path_schema_validation; owning PathSchemaValidation port in path_admission | Installed schema bytes, lazy JSON Schema validator and offline reference registry | Network schema retrieval, compiler or source authority, native clients |
| `src/ashlar_host/commerce_path_request.py`, `commerce_path_oracle.py` | Original commerce request construction and independent source-derived expected results | commerce_path_request, commerce_path_cases, original_commerce_path_oracle | Public publication-reader request and original model/graph values | Compiled SQL as expected-result authority, invented identities/relationships, tool imports |
| `src/ashlar_host/paths_query.py` | Separately selected indexed Paths workflow and outer publication/cleanup lifecycle | query_commerce_paths; configuration owned by config.QueryCommercePathsConfig | Trusted Paths distribution APIs, public admission/execution and explicit producer/reader/runtime ports | Legacy compiler fallback, ACK advancement, report release before closing checks and cleanup |
| `src/ashlar_host/delta_publication.py`, `delta_query.py`, `driver.py`, `delta_custody.py`, `graph_sql.py`, `publication_reader.py`, `indexed_query.py`, `relationship_plan.py`, `relationship_query.py`, `runtime.py`, `lifecycle.py` | Native transport/journal custody, publication composition, held query execution, runtime selection and cleanup | Host-internal driver/reader/phase ports, ReaderCleanupState under CONTRACT-006; applications enter through commerce.publish_commerce or commerce.query_commerce | Public core publication/resolver/source/compiler APIs, admitted policy ports, configured Spark/Delta and owned ACK/session adapters | Tool imports, rewritten compiler SQL or repaired results, fabricated source/ACK authority, success release before required closing checks and cleanup |
| `src/ashlar_host/managed_pin_reader.py` | Explicit ordinary loopback PostgreSQL pin-reader connection/transaction ownership | Closed pin-reader selection and actual native transaction lifetime under CONTRACT-002 | Existing psycopg3, explicit private passfile reference, public Session/PostgresPins contracts and primary-preserving lifecycle | Libpq service/environment discovery, role switching, Docker credentials, provider imports, role/grant/pin mutations or configuration as authority |
| `src/ashlar_host/managed_singleton_configuration.py`, `managed_singleton_cli.py` | Closed installed managed-read invocation and finite worker lifetime | `ManagedSingletonConfig`, `parse_managed_singleton`, `run_managed_singleton_command`; CLI `read-managed-singleton` | Exact typed source/SQL/pin/producer selections, supervised process capture, concrete singleton composition and bounded private diagnostics | Arbitrary provider/SQL callbacks, implicit endpoint selection, source/ACK changes, unbounded workers or results released before owner closing |
| `src/ashlar_host/native_singleton_capture.py` | One bounded aggregate metadata capture for an effective row limit of one; original native values and all-exit iterator closing | `capture_native_singleton`, `SingletonCaptureError`; transport maps guard refusals | Exact `VerificationBounds`, prevalidated schema/measurements, lazy Spark expressions and lifecycle | Source/native authority, identity caches, JSON value reinterpretation or unbounded driver transfer |
| `src/ashlar_host/evolution_singleton.py`; `driver.HeldEvolutionPublication` | Source-qualified singleton reads over an authenticated original combined-source publication | `EvolutionSingletonSelection`, `read_evolution_singletons`; `NativeDriver.hold_evolution_publication` | Actual registered source/ACK owners, original full vector and retained receipt observation, independent full-row oracle, bounded report after all closing | ACK submission, caller-minted holds, subset vectors, latest fallback or results released before provider closing |
| `src/ashlar_host/configured_local_graph_release.py`, `configured_graph_custody.py` | Complete selected configured-publication graph and distinct immutable JSONL saved-file correspondence | `read_configured_local_graph_release`, `build_configured_graph_custody`, `admit_configured_graph_custody`; runtime `export_configured_local` | Minted full-vector configured read scope, core graph release, independent source-prefix bags; private loader's explicit versioned branch | Latest fallback, protected ACK claims, scalar promotion, invented live permission or pair release before all original owners close |
| `src/ashlar_host/managed_carriers.py` and packaged `resources/managed-delta-v03` | Original canonical managed eight-table generated asset custody and ordered columns | `ManagedCarriers`, `load_managed_carriers`, `validate_managed_installation` | Exact generated JSON and nine original model/index inputs through package resources | Generator reimplementation, local six-table profile substitution, checkout imports or retained installation facts as current native authority |
| `src/ashlar_host/managed_source_oracle.py` | Retained original materialization-clock correspondence and independent configured-prefix four-role row derivation | `retained_materialization_clock`, `configured_prefix_carrier_rows`, immutable clock and detached row evidence | Public configured source/transaction APIs, exact managed carrier columns and bounded SDK result decoder | Snapshot rows as an oracle, publisher SQL generation, live clock replacement, new source interpretation or retained facts as native authority |
| `src/ashlar_host/managed_publication_selection.py` | Existing configured-JSONL journal and original artifact/source correspondence | Immutable selected completed publication and complete original target inventory | WAL-aware bounded read-only SQLite, public source configuration/schema/model and manifest APIs | Journal initialization/repair, new effects, checkout imports or retained flags as current admission |
| `src/ashlar_host/effective_grants.py`, `native_owner_lane.py` | Original bounded effective-permission observation and fresh native owner-lane admission | `effective_grants`, `NativeOwnerLane`; legacy tools reexport these same owners | Explicit workspace SDK, raw observation retention and public writer-inventory validation | Permission mutation, inferred actor/owner authority, remote fencing promises or tools imports |
| `src/ashlar_host/process_capture.py` | Retained-session process supervision with finite deadline and bounded streams | `capture_process` (at most 600 seconds); original producer wrapper remains at most 60 seconds | Explicit argv/environment, owned process group and mandatory primary-preserving cleanup | Shell interpretation, descendant session escape guarantees, SDK/publication authority or unbounded capture |
| `src/ashlar_host/managed_record_admission.py` and packaged `resources/managed_record_values.ts` | Original configured Record preflight through the fixed c45 public UMF producer | Explicit `ProducerConfig`, original intake/bindings/source Record inventory and bounded raw receipts | Public UMF `src/index.ts` exports, public bounded producer transport, clean pinned Git source and selected executable identities renewed at closing | Checkout tools/private UMF imports, inferred native acceptance/source authority, source rewriting or OS/dependency isolation claims |
| `src/ashlar/typed_source_config.py`, `src/ashlar_host/typed_record_admission.py`, `configured_typed_source_admission.py`, `resources/typed_record_values.ts` | Original core 0.8 scalar source configuration and fresh public Record admission | Explicit immutable development bindings, ordered complete carried-value receipts | Public pinned UMF API; exact numeric tokens, presence and file/receipt closing; host reference files at most 256 KiB and model at most 64 KiB | Private semantic interpretation, inferred catalog/source/ACK authority, empty delete payloads or numeric Weft qualification |
| `src/ashlar_host/configured_typed_weft_binding.py`, `configured_typed_weft_projection.py` | Publication-bound original typed query correspondence and independent result oracle | Original admitted core 0.8 bytes, development mappings and full vector; exact source-prefix projection/count expectations | Indexed Spark041 unchanged compiler artifacts, owning guards, exact scalar codecs and held native/source closing | Inferred query names, numeric token rewrites, native result cells as expected values, absence collapsed into null or metadata as authority |
| `src/ashlar/typed_graph_source.py`, `typed_graph_source_config.py`, `src/ashlar_host/typed_graph_admission.py`, `configured_typed_graph_source_admission.py`, `resources/typed_graph_values.ts` | Exact source/poststate transport, joint public Record/dataset proof and strict apply/replay | Separate core 0.8 graph configuration, qualified Record/relationship ID mappings and retained original receipts | Clean pinned public UMF073 API; independently reconstructed full transactions and endpoint identities; immutable-file source custody | Fake planning policies, stripped context warnings, unvalidated edge properties, metadata as source/native authority or automatic graph-query admission |
| `src/ashlar_host/configured_source_admission.py` | Installed configured-source admission before database setup | `ConfiguredSourceAdmissionConfig`, `validate_configured_source`; CLI `validate-configured-source` | Original pinned JSONL configuration, existing binding/replay inspection and public managed Record admission; bounded private receipts after input closing | Source/model rewrites, database or ACK effects, native authority claims, implicit producer settings |
| `src/ashlar_host/file_progress.py` | Durable local immutable-file consumer progress | `JournaledJsonlProgress`, `JournaledFileProgress`, `RetainedFileProgress`, `retained_entries`, `LocalProgressOutcomeUnknown` | Complete original source/checkpoint chain, independently resolved descriptor and mandatory current admission held across local commit; detached retained entries supply correspondence only | Protected producer ACK claims, ordinal substitution for byte cursors, missing resolver or admission, uncertain commit treated as failure without reconciliation |
| `src/ashlar_host/configured_source_local.py` | Original configured local source and transaction specifications | `open_configured_local_source`, `ConfiguredLocalSource`, `ConfiguredLocalBatchSpec` | Fresh public UMF admission, renewable original inputs and receipts, exact complete batch/checkpoint/predecessor requests and detached portable prefix states | Native snapshot or writer authority inferred from portable state, protected source ACK claims, rewritten source records or foreign request admission |
| `src/ashlar_host/configured_local_publication.py` | Sequential configured-source local Delta publication | `ConfiguredLocalPublicationDriver` | Concrete local effects, attempt and manifest stores, independent source-derived carrier bags, mandatory current admission and descriptor-bound local progress | Arbitrary replacement backends, parity used as authority, substituted recovery plans, protected producer ACK claims |
| `src/ashlar_host/configured_local_runtime.py`, `configured_local_cli.py` | Installed isolated configured-source lifecycle and reads | `ConfiguredLocalRuntimeConfig`, `initialize_configured_local`, `publish_configured_local`, `read_configured_local`, `query_configured_local`, `run_configured_local_command`; CLI `run-configured-local` | Closed explicit runtime/source/query selection, owned finite worker, actual local lease/registry/native protocol/schema/retention admission, source preflight and publication driver | Cloud endpoint selection, remote fencing or protected ACK claims, metadata reuse outside the continuously held cooperative lease, public success before closing |
| `src/ashlar_host/configured_local_read.py` | Selected-ordinal configured local read custody | `open_configured_local_read`, `ConfiguredLocalRead`, `read_configured_local_singleton`; installed runtime `read` operation | Existing-only progress, exact committed phases, complete source-prefix bags and UUID/version vector; expiring ports and independent closing checks | Latest-head fallback, missing-proof repair, native mutations or checkpoint advancement, PostgreSQL pin or protected ACK claims |
| `src/ashlar_host/configured_query_aliases.py` | External alias custody for an original configured publication | `open_configured_query_aliases`, `ConfiguredQueryAliases` | Exact held scope/transport, fresh local namespace, external Delta identity/location and pinned schema; guarded owned-metadata cleanup | Manifest relabeling, source-table deletion, foreign replacement cleanup, remote fencing claims |
| `src/ashlar_host/configured_weft_binding.py`, `configured_weft_compile.py` | Original-model query binding and indexed compilation | `configured_count_star_request`, `ConfiguredWeftCompileConfig`, `compile_configured_count_star` | Held original model/vector, physical name projection, public compiler/schema admission, exact recompilation and closing | SQL repair, inferred alias authority, skipped obligations, substituted model versions or compiler profiles |
| `src/ashlar_host/configured_weft_query.py` | Publication-bound configured query execution | `ConfiguredWeftProvider`, `query_configured_count_star` | Same scope/alias/transport, public resolver and indexed execution, independent source oracle, provisional buffered results | Alternate execution transport, native construction, source/progress/ACK mutation, result availability before outer closing |
| `src/ashlar_host/configured_weft_projection.py`, `configured_local_query.py` | Required STRING projection/global count admission and command report | `configured_projection_oracle`, `ConfiguredLocalQueryConfig`, `run_configured_local_query`; `query_configured_projection` in the query executor | Original policy/property homes, independent complete source-prefix rows, explicit finite query settings, exact typed carriers and protocol texts | SQL interpretation/repair, native-result-derived oracles, inferred optional/other-scalar support, premature report availability |
| `src/ashlar_host/managed_sql_read.py`, `managed_singleton.py` | Bounded authenticated dedicated-warehouse reads and installed managed-singleton composition | Explicit SDK connection; current native owner/source/retention/full-vector read and closing | Public resolver/singleton/protocol/snapshot APIs, concrete pin-reader owner, current effective-permission and native intake checks | Durable mutations/publisher/replay, shared/default compute, arbitrary SQL/provider policy callbacks, tools imports or authority inferred from configuration |
| `src/ashlar_host/ack.py`, `postgres.py`, `connection.py` | Protected source acknowledgement and reconciliation, ordinary PostgreSQL sessions and private-profile connection construction | Host ACK/session ports: AckScope, ProtectedOutboxAck, AckOutcomeUncertain, Session | Public core descriptor/checkpoint/pin APIs, mandatory policy and connection ports, packaged ACK SQL and explicitly selected psycopg profile | Caller flags as ACK authority, source progress inferred from query results, checkout tools or unqualified production credential discovery |
| `src/ashlar_host/fabric_release_journal.py` | Private local candidate intent, original creation-handle correspondence, predecessor CAS and process-shared lifecycle leases under CONTRACT-003 | ReleaseIntent, LocalActive, CreationCorrespondence, FabricReleaseJournal, FabricMutation; local bookkeeping, bounded admission-evidence/predecessor CAS and live local mutation permits only | Standard-library SQLite/POSIX custody and owned lifecycle cleanup | Cloud clients, loaded-graph policy construction, source/ACK grants, receipts or local pointers as native admission |
| `src/ashlar_host/fabric_deployment_http.py` | Finite ordinary delegated workspace deployment transport; distinct native synchronous creation and asynchronous original-operation correspondence | FabricDeploymentHttpConfig, FabricCreatedGraph, FabricAcceptedCreation, FabricDeploymentResponse; open_fabric_deployment_http and explicit loopback test acquisition; exact creation/operation/job methods | Public bounded process capture, HTTP configuration scalar checks, lifecycle cleanup and standard-library HTTPS | Private HTTP credential access, implicit polling/retry/refresh, journal or loaded-graph admission, source/ACK authority |
| `src/ashlar_host/onelake_release_upload.py` | Fresh OneLake immutable-byte projection upload/readback under a real local candidate mutation lifetime | OneLakeUploadConfig, OneLakeReleaseFile, OneLakeReleaseTable, OneLakeUploadReceipt; open_onelake_release_upload, upload/readback/observations and explicit loopback test acquisition | Public passive finite account selection, bounded capture, lifecycle cleanup, live FabricMutation and standard-library HTTPS | Private token access, overwrite/delete/retry, source/ACK or native Delta grants, loaded-graph admission or receipts as authority |
| `src/ashlar_host/commerce_fabric_deployment.py` | Actual original-commerce source/ACK/native held deployment preparation and recovery through original creation/initial-load observations | CommerceFabricDeploymentConfig, HeldCommerceFabricDeployment, HeldCommerceFabricAdmission, open_commerce_fabric_deployment; nominal admission interval and explicit staging/readback/submit/recovery/load observations | Public original-commerce hold, Delta metadata/readback, invocation file pins, journal mutation, OneLake upload, deployment HTTP, mapping generator and lifecycle cleanup | Activation or loaded-policy grants before independent native definition/full-bag admission, private credentials, implicit portal success, refresh/recreate or latest-head recovery |
| `src/ashlar_host/commerce_fabric_admission.py` | Actual original-commerce native definition/full-carrier/endpoint admission, durable predecessor activation and private cooperative source-bound reader policy | admit_commerce_fabric_deployment, CommerceFabricLoadedGraphPolicy; nominal source_interval plus hold/admit | Public held deployment/source types, journal reader/mutation ports, ordinary/deployment HTTP APIs, strict GQL executor, public retained JSON codec and qualified carrier query shapes | Callback/Boolean native admission, receipt-only authority, nested source acquisition, private credentials, dropped native semantics, production isolation or generation claims |
| `src/ashlar_host/fabric_graph_definition.py`, `fabric_ordinary_http.py`, `commerce_fabric_release.py` | Canonical carrier mapping/query shapes, finite ordinary GQL HTTP selection and actual source/graph-policy composition | make_fabric_graph_definition, fabric_carrier_queries; selected_configuration; execute_commerce_fabric_gql and optional nominal source_interval composition | Public canonical release columns, bounded process/HTTP ownership and actual source/native policy ports | Shadow native result parsing, credential disclosure, fabricated source or loaded-graph grants |
| `ashlar_host.medical_singleton_request`, `medical_singleton_query`, `medical_singleton_local` | Pure original08 request/oracle, held indexed query and reader composition | `MedicalSingletonQuery`, immutable `MedicalSingletonBuffer`, `query_original_medical_singleton` | Original medical admission/read-source/publication owners and public pinned indexed compile/admission/execution | Arbitrary callback authority, compiler SQL rewriting, tool imports or executor-to-reader dependency |
| `ashlar_host.medical_query_requests`, `medical_query_batch`, `medical_query_local` | Pure original full-row/path witnesses and finite same-reader batch composition | `MedicalQueryBatch`, `MedicalQueryBatchBuffer`, `query_original_medical_cases` | Original medical reader/source owners, public medical query provider, indexed admission/executor/decoder and lifecycle | Arbitrary SQL/callback authority, partial two-hop coverage, per-query Spark acquisition, compiler SQL repair or tool imports |
| `ashlar_host.medical_query_source` | Original medical mathematical Field population correspondence under an actual held source/publication | `MedicalQuerySource`, `medical_integer_projection` | Actual medical admission/public dataset, medical source registration, driver held-vector renewal and original lexical inputs | Callback authority, invented numeric facets, compiler SQL repair or source/ACK grants |
| `ashlar_host.medical_query_configuration` | Closed installed read/compiler/case selection and final compiler correspondence | `OriginalMedicalQueryConfig`, parser and selection renewal | Existing typed read config, public medical batch selection and indexed compiler distribution | Compiler installation, trust-pin overrides, source/native grants or private cross-feature helper imports |
| `tools/` adapters | Checkout-only engine/source experiments and review utilities | Named runner/checker entrypoints | Owned public core/host APIs and explicitly selected SDKs | Installed package dependency on tools; newly introduced private cross-module access |

The installed original-commerce evolution facade adds these owned boundaries:

| Module | Responsibility / Owned Types | Public API | Allowed Dependencies | Forbidden Dependencies |
| --- | --- | --- | --- | --- |
| `src/ashlar/commerce_evolution.py` | Immutable finite source preparation and independent complete prefix-union oracle | Named preparation and oracle under CONTRACT-003 | Public core source/value APIs and Python3.9 standard library | Host/tools/SDK imports, runtime discovery, authority or replacement UMF semantics |
| `src/ashlar_host/evolution_admission.py` | Bounded original/candidate/proof custody and exact public producer admission | EvolutionSourceSet source/transition admission, state_at and complete oracle; owned producer admission ports | Explicit config, public core preparation, packaged resources and configured public UMF producer | Checkout discovery, ambient Bun/Git, private UMF APIs, receipt flags as authority |
| `src/ashlar_host/commerce_evolution.py` | Fresh/resume lifecycle, source-qualified publication and original recovery | publish_evolution_transaction, resume_evolution_transaction; publish_commerce_evolution/resume_commerce_evolution under CONTRACT-001 | Explicit config, owned admission/driver/journal/ACK/lifecycle ports and supplied ordinary sessions | Source/role/grant provisioning, credential discovery/escalation, new uncertain intent, tools/private native access |
| `src/ashlar_host/evolution_plan.py` | Pure immutable original attempt envelope and journal custody | EvolutionAttemptPlan, EvolutionPlanJournal; compatibility exports through commerce_evolution | Standard library and mandatory independent plan-admission policy | Config/orchestration imports, native clients, source/ACK authority |
| `src/ashlar_host/evolution_run.py` | Fixed alternating original schedule and durable positive submission reservations | EvolutionRunRequest, EvolutionRunDefinition, EvolutionRunLedger, EvolutionRunAttemptJournal, EvolutionRunLedgerView | Pure plan types, standard-library SQLite/POSIX custody and mandatory independent current run/lineage/attempt policy | Native clients, source provisioning, absent handles as absence proof, mutable caller status as authority |
| `src/ashlar_host/evolution_composition.py` | Exact original eight-slot correspondence and independently admitted native lineage composition | OriginalRunCorrespondence, NativeEvolutionRunPolicy | Owned immutable source/run/plan types and public driver/current original-reservation authority ports | Private driver/transport access, original slot flags or native absence as authority |
| `src/ashlar_host/evolution_cli.py` | Closed installed fresh/resume invocation and explicit trusted operator-code composition | FreshEvolutionInvocation, ResumeEvolutionInvocation, run_evolution_command | Public full-eight run/configuration and actual driver/source/ACK owners, mandatory independent reservation policy, bounded original file custody | Ambient credential/runtime discovery, provisioning, SHA or slot status as authority, substitute publication execution |
| `src/ashlar_host/evolution_verification_types.py` | Closed existing-only ordinal, capture limits and explicit finite operator supervision selections | VerificationBounds, VerificationSupervision, VerifyEvolutionInvocation, VerifyCommerceEvolutionConfig | Typed producer selection and standard-library carriers | Native authority, effects, implicit defaults or deadline guarantee |
| `src/ashlar_host/evolution_verification.py` | Selected completed publication verification under unchanged full-eight lineage | verify_commerce_evolution | Public leased ledger, native full-bag and current source/protected ACK owners | Publisher recovery, private native access, preparation or ledger/progress writes |
| `src/ashlar_host/evolution_verification_cli.py` | Closed installed verification/singleton dispatch and bounded post-cleanup receipts | parse_verification_invocation, run_verification_command, run_singleton_command | Captured configuration/provider bytes, explicit operator supervision and closed nominal verification/singleton composition | Environment discovery, arbitrary handlers, caller oracle or SHA as source authority |
| `src/ashlar_host/evolution_operator.py` | Existing local installation and original-run ownership with ordinary source/ACK composition | ExistingEvolutionOperator, ExistingEvolutionLease | Existing installation reservation/registry, cooperative POSIX lease, query-only native SQLite readback, owned read-only original ledger inspection, public driver construction and native run policy | Installation initialization, credentials discovery, grants, missing custody as absence proof, local lease as exclusion of external native writers |
| `src/ashlar_host/existing_outbox_authority.py` | Current ordinary source and ACK role, access and original installation admission | ExistingOutboxAuthority, OutboxAuthorityRegistration, admit_source, admit_scope | Supplied ordinary sessions, actual role/privilege/catalog-signature readback and independent held existing writer lease | Constructor metadata, receipts or registration class as source/native permission; provisioning |
| `src/ashlar_host/evolution_producer.py` | Bounded public-UMF subprocess ownership and original receipt custody | Typed producer invocation for evolution_admission under CONTRACT-001 | Explicit immutable configuration/input snapshots, standard-library process and owned lifecycle ports | Ambient environment/PATH discovery, private source capture or UMF APIs, tools, native authority |
| `src/ashlar_host/source_sessions.py` | Original outbox registrations and ordinary source-session ownership | OutboxSourceRegistration, RegisteredOutboxSources.admit | Explicit factories/current native policy, owned AckScope/Session/lifecycle and public complete outbox/checkpoint APIs | Credential discovery, provisioning, commits, ACK mutation or registration metadata as authority |
| `src/ashlar_host/ack_sessions.py` | Original protected ACK registrations and ordinary session selection | OutboxAckRegistration, RegisteredOutboxAcks.acknowledge/reconcile | Supplied ordinary factories/current policy, owned protected ACK and source-registration ports | Credential discovery, provisioning, metadata as authority or cancellation converted to success |

`EvolutionSourceSet` owns reconstructed original semantic sources after checking
their complete packaged correspondence. It reconstructs state from explicit
consumed prefixes and derives all native graph/history/tombstone cells from the
independent original oracle, using the original transaction clocks for each
source and retaining unchanged rows' earlier materialization clocks. Qualified
source identity preserves overlapping local keys. Returned mappings are copies;
recorded custody establishes correspondence, while fresh producer verification
and current source, writer, publication and ACK authority remain separate gates.

`NativeEvolutionRunPolicy` composes independently owned current original-ledger
authority with the public driver's complete lineage gate. Every retained attempt
must match its original scheduled source-qualified request, progress, clocks and
oracle. Unprepared or retained reservations must refuse contradictory operation,
artifact, phase or named native commit custody. Completed slots require every
original graph, phase and manifest handle, exact native commit/receipt/snapshot,
immutable manifest and protected read-only ACK reconciliation. Started slots keep
their original operation recovery path; missing whole effect plans refuse.
`LocalDeltaTransport.inspect_committed` observes exact settled original evidence
without submission or journal updates. Correspondence and observations cannot
replace the independent original reservation authority or restore lost custody.

`RegisteredOutboxSources` brackets two complete original transaction reads with
current native source-policy admission and closes every acquired ordinary
connection through rollback and close. The evolution source-set driver requires
this injected owner; protected ACK retains its separate authority/session owner.
`RegisteredOutboxAcks` selects the original source-qualified protected scope and
reuses the protected ACK owner under supplied ordinary sessions and mandatory
current source/publication policy. Native scope binding must match the retained
namespace and immutable signature. Cancellation retains its exact exception
identity and carries original ACK uncertainty custody for explicit reconciliation;
it cannot trigger automatic retry-success. Source-set publication requires this
injected ACK owner and matching complete source registrations.

Cleanup attempts every owned callback and preserves the original cancellation;
otherwise cleanup cancellation takes precedence over an ordinary body failure.

`EvolutionAttemptPlan` retains the complete closed attempt envelope as immutable
original bytes: source-qualified request, generated and selected ordered steps,
no-op anchors, publication identity and clocks, prefix/schema/progress state,
resource registry, admitted source facts and original ordered operation identities.
`EvolutionPlanJournal` receives an explicit private file path and mandatory
semantic/current-authority policy. Fresh retention exclusively creates one file,
synchronizes its bytes and parent directory, and refuses any existing original.
Resume requires the owning attempt's trusted digest and reads without creating,
repairing or replacing custody. The mandatory policy independently admits all
native registry, anchor, source, schema and oracle meanings; structural validation
or caller-supplied hashes cannot establish native authority. Interrupted files
remain for explicit reconciliation. The local POSIX filesystem and enclosing
exclusive writer are assumptions; this port does not prove native submission,
publication or ACK. Journal payloads must not enter CONTRACT-006 diagnostics.

`LocalDeltaEffects.prepare` retains the exact ordered whole effect intent before
submission and returns its original ordinal identities. `observe` reads only
that retained whole plan and exact per-operation custody under current policy.
A missing operation record is a journal observation, never proof of native
absence. Recovery routes retained ordinals through the original recovery port;
an ordinal without a record reaches first submission only through the transport's
mandatory named-native-commit/history refusal checks. Submitted absent or
ambiguous commits remain uncertain and cannot authorize replacement mutations.
Opening and closing admission must complete before releasing plan/status results.
Fresh retention and initial execution refuse a missing whole plan when any derived
original ordinal already has custody; neither recreates that lost original. If
ordinal custody is also missing, only current authoritative fresh admission and
the transport's named-native-commit checks may establish a first submission.



The producer transport uses the selected installed host Python runtime for a fixed,
shell-free supervisor; caller arguments remain data and select only the pinned
producer executable. Its separate bounded status channel reports the producer
exit status while the owned session leader remains alive. Cleanup kills that
reserved process group before reaping its leader, then closes all owned channels.
It must refuse missing or malformed completion status and preserve cancellation
identity; successful producer exit alone does not establish semantic admission.
This transport assumption does not extend the UMF executable/source custody claim
to an operating-system or interpreter closure.

`FreshEvolutionTransactionConfig` and `ResumeEvolutionTransactionConfig` select
one original transaction independently of installation construction. Their
public transaction composition enters `NativeDriver.publish_evolution_attempt`
through owned ports. Held admission checks the complete source-prefix oracle,
original four-clock vectors, full progress/schema inventory, generated/elided
steps, ordered operation identities and actual UUID/prior-snapshot registry.
Independent current native policy remains mandatory. Fresh retains full intent
before effect and publisher phase custody; resume loads only the retained bytes
and original whole effect plan. Missing effect-plan custody after attempt-file
retention safely refuses and does not authorize restoration. Closing custody,
source admission and writer cleanup gate result release. These transaction APIs
are internal components of the fixed eight-step facade. Separate immutable
`FreshCommerceEvolutionConfig` and `ResumeCommerceEvolutionConfig` select supplied
ordinary source/ACK/native ports, explicit public producer/runtime, current policy
and original ledger custody. Fresh requires an independently authorized, already
empty installation; resume has no initialization or replacement option.

The run ledger exclusively retains its original definition and all eight slots
on the selected private local POSIX filesystem using FULL synchronous SQLite with
DELETE journaling. The developer-owned schedule alternates the two sources through
R1→R4; the complete source-qualified prefix/progress vector and ancestry come from
original attempts and actual descriptor replay. Each complete original plan captures
actual native prior anchors immediately before retention. A positively retained
slot precedes `publication-started`, which must be durably stored before any native
publisher phase or effect. Only the original retained/not-started slot under
independent complete-ledger and native policy can authorize preparation of its
exact original missing whole effect plan. Missing slots, altered ledger, known
original ordinal/native commit or started custody cannot establish that right.
Started attempts retain ordinary original-operation recovery when complete plan
custody remains; completed replay renews protected ACK and preserves descriptors.
Public producer/source correspondence, original ledger/native/source admission and
all owned cleanup must close before the full run returns. This cooperative writer
and local-filesystem custody model requires separately qualified actual installed
native execution and fresh-process recovery; it does not prove physical retention,
cloud authorization or adversarial administrator resistance.

`config.py` owns separate immutable fresh/resume types and validates the complete
selected profile before effects. The CLI is the construction root; supplied native
writer/source/ACK authority remains independently admitted. `resources/` owns the
immutable public checker and named original/augmentation inputs. Producer admission
verifies the developer-owned exact e44 source closure and Git identity at opening
and closing; a caller-selected inventory never establishes that trust. Update the actual
module checker for new classifications without enlarging debt exemptions; verify
allowed and forbidden imports through that checker. Semantic review covers
session authority, dynamically supplied ports and original-plan ownership.

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

The installed CLI selects explicit diagnostics through CONTRACT-006's file
boundary. `ashlar_host.diagnostic_composition` validates selected dependency
availability, constructs local capture and the OTel adapter once, and injects a
caller-owned run into the workflow. `ashlar_host.paths_query` owns its held-read
attempt and genuine phase observations; the composition owns run closure.
`None` disables diagnostics without SDK discovery. Missing/incompatible selected
dependencies and failed local capture construction refuse before business effects;
ordinary later worker/transport startup failure leaves local capture active with
unknown remote delivery. Exact typed startup disposition requires positive
worker dependency-import admission and owned cleanup before fallback; both host
and worker use the single immutable dependency profile. Diagnostic loss never changes native cleanup markers
or bypasses closing custody and report publication. OTel SDK
construction belongs only to its private `ashlar_host._otel_worker` in the selected
Python 3.11 host runtime. The adapter owns the worker's clean environment,
bounded request/response channel, deadline and termination/reaping. Settings
and secrets cross only the private initialization channel; command arguments,
environment and diagnostic artifacts cannot carry secrets. Explicit SDK
construction cannot by itself establish independence from SDK environment
defaults. Transport progress frames precede every owned request and retain the
original shared close deadline; parent supervision bounds DNS, parsing and
transport cleanup independently of socket inactivity timeouts. Missing or late
progress terminates the worker and reports affected delivery as unknown.
Initialization has a separate two-second ceiling before operation effects;
failed construction receives at most two further seconds for owned cleanup.
The parent retains the unreaped leader until its one-shot process-group
termination, then reaps it; default SIGCHLD handling and exclusive child ownership
are required. No other component may reap the worker or mutate its signal policy.
The portable core remains Python 3.9 compatible and has no OTel dependency.
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

The selected diagnostic host uses the optional `diagnostics` installation extra
and Python 3.11; portable-core dependency and Python-floor policy remains intact.
The extra pins API, SDK and OTLP HTTP integration versions named in CONTRACT-006;
qualification retains the complete resolved dependency lock and actual installed
version evidence. Package selection alone does not qualify delivery. The explicit
JSON configuration source derives origins without accepting a caller-supplied
provenance claim. Exact file, command and retrieval surfaces remain in CONTRACT-006.

Run construction transfers signal shutdown ownership only after successful local
capture admission. A failed partial construction closes resources still owned
by the composition. After transfer, run closure drains the adapter once under its
shared deadline. Composition closes the diagnostic run without passing a business primary, then
resolves CONTRACT-006 cancellation precedence. Ordinary diagnostic failure cannot
replace a business primary or pollute its native cleanup marker; the first
non-Exception cancellation takes precedence over an ordinary error. Every phase
hook after Spark acquisition executes inside the native cleanup owner's try/context. This ownership split avoids
abandoned workers and duplicate shutdown while permitting remote startup loss.
`publication_reader.py` owns optional read-only cleanup facts shared between one
reader and its provider. It observes actual transport close and PostgreSQL
rollback/close ports, preserving existing cleanup policy. Paths observes these
public facts independently of exception markers; custody drift stays a closing
failure. Generic context exit is insufficient evidence of cleanup failure.

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

The installed evolution phase store binds an exact verified original request and
qualifies observation and mutation by its original outbox feed and epoch as well
as stream, local batch ID and phase. Overlapping local batch IDs across sources
remain distinct; changed requests within the same qualified source identity
refuse rather than acquire replacement custody. Bounded complete stream/batch
rows are validated before source selection, so malformed or unknown source
custody cannot disappear behind native JSON predicates. The original request,
checkpoint and batch bytes remain unchanged. Native JSON expressions in the
scoped MERGE require the selected Spark/Delta profile qualification.

Scoped phase identity is selected for the original eight-step run journal only.
The existing one-transaction plan-journal path retains its original unscoped
phase SQL and original native operation identities; it does not adopt or
regenerate interrupted historical operations into the new run profile.

### Fresh original17 analytic composition

CONTRACT-003 owns the original17 invocation and representation meanings. The
host implementation separates immutable scenario rendering/decoding, admitted
materialization and owned native orchestration. It reuses the original query
plans and independent graph oracles; it does not import checkout `tools` modules.

| Owned module | Public boundary and construction | Allowed dependencies / exclusions |
| --- | --- | --- |
| `ashlar_host.pack_scenario_queries` | Pure original17 plan renderer, exact flat-map decoder and positional result admission; source-corresponded extraction of existing helpers | Portable core and fixed original plans/oracles; no SDK, process, file or authority acquisition |
| `ashlar_host.puppy_scenario_preparation` | Typed two-pack input selection and admitted four-table analytic materialization | Public finite source/file owners, original scenario projection and lazy DuckDB; no container or model POST |
| `ashlar_host.puppy_scenario_native` | Mint fresh scenario runtime owner from typed explicit settings and admitted preparation; bounded query/observe ports | Public bounded runtime mechanics and lazy selected Neo4j/Gremlin clients; no historical six-database checker or ambient credentials |
| `ashlar_host.puppy_scenario_cli` | Closed invocation parsing, one-protocol composition and report availability after closure | The three owning modules and public lifecycle/config/source ports; no private cross-module access or tools imports |

The CLI integration owner adds one lazy `run-puppy-pack-scenarios` route; import
and help must remain SDK-free. Configuration owns explicit selected paths,
secrets, protocol and finite limits; only the factory mints live runtime owners.
Shared HTTP/Bolt readiness and owned runtime cleanup should be exposed by a
narrow public port in `puppy_finite_local` rather than copied or accessed privately.
Its existing export and finite STRING behavior must remain unchanged.

Implementation order: extract source-corresponded pure helpers and reuse existing
renderer controls; materialize/admit both complete original profiles; expose the
fresh runtime port and verify readiness/one-POST failure behavior; compose the
public command and closing-before-report tests. Apply `tools/check_module_boundaries.py`
to the new modules without exceptions. Native qualification is a subsequent
small one-runtime execution per selected protocol, not a consequence of source tests.
Unresolved preparation prerequisite: ecology's exact original saved pair must be
restored from independently pinned originals before an invocation is admitted;
no source/publisher reconstruction is implied by this design.

The installed medical query command uses `medical_query_configuration` for its
closed read/compiler/case selection and `original_medical_local_cli` for the
existing supervised worker lifecycle. The public `medical_query_local` batch SDK
owns execution; these command adapters MUST NOT duplicate native/source authority
or import private helpers from other features.
