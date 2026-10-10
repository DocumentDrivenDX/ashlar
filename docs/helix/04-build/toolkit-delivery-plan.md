---
ddx:
  id: ashlar.toolkit-delivery-plan
  type: implementation-plan
  activity: build
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
  - id: TP-001
    kind: informed_by
---

# Ashlar toolkit delivery plan

## Scope

Owner direction, 2026-10-09: regroup against current specs and implementation,
review with Astra Ultra, then execute this ordered plan using independent lanes.
This plan replaces the execution order in [end-to-end-plan.md](end-to-end-plan.md);
historical execution notes belong under evidence, linked without duplicating them. The real-Truss workflow is a required deliverable with its own external
prerequisite. It cannot block other lanes.

Governing artifacts: [PRD](../01-frame/prd.md), FEAT-001 through FEAT-004,
[consumer test plan](../03-test/consumer-conformance-plan.md), CONTRACT-001 through
CONTRACT-005, and the latest owner scope amendment in the previous build plan. Desired behavior
in this plan includes cross-domain UMF models, exact typed graph ingestion,
Weft queries, publication-derived graph releases and runnable source workflows.
Any new public semantics require corresponding design/test reconciliation before
implementation. Draft spec status does not prove runtime qualification.

## Shared constraints

Canonical state remains Unity Catalog managed Delta. Keep predictive optimization
configuration intact. Exact original UMF/source bytes, typed identities, numerical
meaning, missing/null distinctions, unknown content, independent edge identity,
immutable publication vectors, replay/conflict and retention rules remain required.
UMF owns interpretation/validation/DDL; Weft owns query compilation. Implement
adapters to their public contracts, not shadow validators or compiler rewrites.
Current-core fixtures are separately authored; original UMF 0.8 pack documents
must be admitted at their declared version, never relabeled 0.7 for a compiler.

Use tiny synthetic pack fixtures on local Spark, Sail or DuckDB. Shared/default
Databricks compute must not be used. Any future Databricks run requires an explicit
dedicated Ashlar query endpoint. No renewed scale benchmark,
new paid capacity, broad grants, destructive migrations or fabricated accepted
Truss IDs. Measure duration/cost of functional runs without turning speed into an
architecture gate. Engine access failures are explicit per-engine dependencies.

## Design and verification foundations

Apply these foundations within P01, before dependent source changes in P03–P10.
Keep requirements and exact shared surfaces in their owning contracts; put run
results and implementation gaps in evidence.

| Concern | Required design | Verification and sequencing |
| --- | --- | --- |
| Modularity and encapsulation | Architecture maps actual modules, owned types, public APIs, allowed/forbidden dependencies, integration owners and composition root | Adopt a project-local dependency checker, shared by local/pre-commit/CI checks, before dependent features. Prove allowed and forbidden edges; test cycle/private-access checks where supported. Inventory existing violations individually and prevent new debt; name semantic review limits |
| Configuration | Entrypoints construct and inject one typed configuration object; declare precedence, one owner per key, required injected operator settings without defaults, committed developer-owned nonsecret values and secret handling. Browser libraries receive explicit inputs and remain independent of process environment | Test precedence, missing/invalid settings, isolated test configuration and redaction. Python application settings follow the selected runtime concern; review narrowly scoped library exceptions explicitly |
| Observability | OpenTelemetry governs applicable signal semantics. Contracts define safe event/mapping/retrieval surfaces; development runners retain bounded attributable diagnostics and clean protocol output | Sequence contract, capture/instrumentation and real receiver/mapping plus failure/privacy tests before a diagnostic pilot. Verify bounded queues/capture, loss reporting and shutdown. Service SLOs apply to services; measure overhead with small functional checks, preserving the prohibition on renewed scale benchmarks |
| Formal analysis | The publication/recovery design specifies states, guards, atomicity, safety and liveness separately, with stable property IDs tied to governing requirements and contracts | Select assurance per affected slice. Review precise specifications; executable analysis requires completed bounded exploration, reachable success/recovery witnesses and broken-mechanism negative controls. Review model/code correspondence and run implementation tests; explicitly record assumptions, excluded behavior and incomplete results |

Independent pack inventory, reviewed upstream fixes and documentation may continue
while foundation adoption proceeds. Affected feature work must satisfy its scoped
foundation checks; unrelated lanes do not inherit external engine prerequisites.

## Implementation slices

| Order / ID | Deliverable | Dependencies | Completion evidence |
| --- | --- | --- | --- |
| 1 / P01 | Refocus PRD/features/contracts/test plans on desired behavior and design; move execution history to evidence; keep only a short implementation-status section in README | None | Requirements/design remain traceable; execution diaries removed from normative docs without losing original receipts; task acceptance inventory covers rows below |
| 2 / P02 | Pin commerce plus all pack manifests/models/data; audit each pack's legal/provenance and representational needs; select additional representatives by distinct semantics | None, parallel with P01 | Original hashes/notices, model/table/graph inventory, exact dataset counts, frozen finite corpus matrix with each pack/semantic/engine case; no inferred unknown meaning; commerce first, supply-chain second, archaeology/ecology next, bounded medical last |
| 3 / P03 | Generalize additional-source intake beyond strings: exact scalar/presence and typed relationships using public UMF artifacts; support admitted current-core models independently; separately admit UMF 0.8 commerce and convert sample data to complete source transactions | P01 for contract; P02 only for pack-specific binding | Independent raw-to-entity oracle; decimal/integer/presence/endpoints, parallel/isolate cases; malformed/conflicting inputs refuse; original bytes retained; source authorization/fencing profile explicit |
| 4 / P04 | Run admitted-model graph ingest, update/delete, additive evolution, replay and interrupted-publication recovery; qualify commerce separately; run PostgreSQL outbox and two-source publications with durable source ACK | P03 for each source/model separately; outbox/combined-source work can use current-core models | Real receipts prove exact current/history/tombstone/versions/progress and old-publication visibility; missing-journal and uncertain-commit recovery exercised; singleton validates complete original fields; outbox restart/ACK and source-specific progress/epochs agree; overlapping local IDs from two sources remain distinct; no synthetic Truss authority claim |
| 5 / P05 | Extend Weft bindings, decode and host obligations for scalar families, relationships, joins, one/two-hop traversal and exact grouped counts | Current-core scalar/query work starts after P01; each typed binding follows its admitted P03 profile; native execution follows its own P04 publication | Exact original compiler artifacts execute on publication-pinned data; independent identities/multiplicities/values match; integrity, invalid scalar, unknown obligation, bounds and drift refusals tested |
| 6 / P06 | Build one reusable immutable graph-release interface from resolved publications, with reversible identities, independent edges and exact capability/loss reports | P01 for release contract; use existing qualified publication for first execution; each additional model requires only its own P03/P04 | Byte/value/identity parity and publication lineage; isolates/parallel/self-loop preserved; unsupported scalar mappings reported; mutable latest exports rejected; full identity/value oracle matches releases R1/R2; old R1 remains stable or explicitly refuses; interrupted import/refresh exposes no mixed release |
| 7 / P07 | Execute the shared graph corpus with GraphFrames, PuppyGraph Cypher/Gremlin, and Fabric Graph GQL in independent lanes | P06; each engine independent | Actual engine version, admitted release/model, runtime queries and independent oracle; recorded refresh/snapshot and credential/protocol limits; unavailable access remains pending, not a pass |
| 8 / P08 | Exercise all UMF domain packs through supported model/data admission; execute selected semantic representatives through ingest plus Weft and engine corpus, documenting unsupported packs/constructs explicitly | Per-pack P02/P03; runtime qualification depends only on that pack and selected engine/query path | Per-pack executable admission result; representatives cover distinct scalar, relationship, topology and evolution features; selection rationale and exact supported subsets |
| 9 / P09 | Package runnable CLI/setup/query and a concise guide, query gallery/support matrix and current schema-browser integration | Each completed source/query/engine lane independently; packaging starts with current qualified workflow | Fresh small local setup plus existing Databricks replay/query path run from documented commands; no hidden temporary paths, credentials or undocumented manual edits; checked package and site |
| 10 / P10 | Integrate real Truss public catalog, mutations and complete feed when available | External Truss only; no prerequisite to P01-P09 | Actual accepted IDs/reports/head and real feed registration/checkpoints/ACK; same create/update/edge/delete/evolution/replay workflow on Delta; retain source authority/fence/security limits |
| 11 / P11 | Final plan audit and reviewed release handoff on main | All deliverables required for claimed support | Requirement-by-requirement evidence and clean main/origin equality; unavailable live-engine or Truss execution stays outstanding, never silently removed from scope |

## Required corpus

P02 freezes named cases before implementation. Scalar coverage includes Unicode
strings, booleans, signed integers including safe-integer boundaries, exact fixed
and variable-scale decimals, dates/timestamps when admitted by public interfaces,
and explicit absent/null/value states. Unknown or unsupported families receive
an executable refusal case. Commerce integer quantities, decimal amounts, typed
relationships, joins, one/two-hop traversal and grouped counts are required passes;
refusal cannot close these promised capabilities. Optional pack constructs may
close classification only, with runtime support explicitly absent. No float conversion may stand in for exact numbers.

Graph cases include isolated nodes, parallel independent edges, self-loops,
cycles, typed endpoints and overlapping IDs from distinct sources. Queries cover
singleton, filtered list with observable limits, one/two-hop directed traversal,
relationship join, distinct-destination versus path multiplicity, grouped counts
and exact commerce settlement/refund arithmetic. Each engine gets named supported
cases and refusals; differing query languages share one independent result oracle.

Mutation cases cover create, update, delete, additive schema evolution, breaking
revision refusal, exact replay, content conflict, interrupted publication,
missing local journal and uncertain commit, epoch mismatch, retained old reads,
source ACK after durable publication, and two-source progress. No live source ACK
is inferred from a fixture checkpoint. Original pack data remains unchanged;
adversarial and evolution fixtures are separately named augmentations.

## Issue decomposition and parallel ownership

Primary agent owns shared contracts, integration, final review, commits and pushes.
Assign non-overlapping agents to pack inventory/oracles, scalar/source adapters,
Weft binding/execution, and engine adapters as capacity permits. Shared interfaces
are agreed before parallel edits. One agent owns each file set; cross-file changes
return to the integrator. Upstream capability requests run separately; local work
continues on available supported subsets without inventing missing APIs.

Task IDs P01-P11 are the execution checklist. If an external tracker is introduced,
back-reference these IDs and governing specs; no tracker creation is required now.
Finish the first runnable commerce slice before widening to every dataset.
Engine access inspection starts during P01/P02; missing access never serializes
GraphFrames, Weft, other datasets or packaging. Do not repeatedly poll inactive
owners or restart live sessions on observation timeouts.

## Validation and landing

Each behavior slice has focused positive and adversarial tests and independent
expected results; native claims need actual native execution. Review before
landing: a separate reviewer checks correctness, spec conformance and evidence
scope. Fix findings and rerun affected checks. Run package/browser checks where
applicable. Stage only owned reviewed files, commit to Ashlar main, push origin,
verify remote commit and required CI. No completed work remains only in a branch,
worktree or temporary directory. Preserve dirty upstream/user work.

## Risks and rollbacks

| Risk | Response | Rollback |
| --- | --- | --- |
| Missing UMF/Weft semantic capability | Record exact API gap, request upstream capability, continue supported independent paths | Keep original bytes and previous qualified profile |
| Managed UC cannot be read by external engine | Qualify publication-derived release access separately | Keep canonical data/publication unchanged |
| Fabric tenant/capacity or PuppyGraph unavailable | Inspect existing authorized access; finish other lanes; retain live test pending | No provisioning workaround without authorization |
| Scope balloons across packs | First commerce then representative semantics; full inventory keeps unsupported outcomes explicit | Revert incomplete adapter behind explicit profile |
| Documentation overstates support | Evidence-backed subset/version matrix and adversarial review | Correct claims without rewriting historical receipts |

## Exit criteria

P01-P11 deliverables are tested, independently reviewed and landed on main/origin.
The plan is complete only when actual promised Truss and engine workflows run;
access inventory, exported model shapes and local simulators cannot replace that
proof. If an external prerequisite remains unavailable after independent work is
exhausted, report that specific pending row and preserve the plan's scope.
