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
dedicated Ashlar query endpoint or dedicated cluster. Scale trials require a separate explicit owner
request and approved resource/cost limits. No unapproved paid capacity, broad grants,
destructive migrations or fabricated accepted Truss IDs. Measure duration/cost of functional runs without turning speed into an
architecture gate. Engine access failures are explicit per-engine dependencies.

## Design and verification foundations

Apply these foundations within P01, before dependent source changes in P03–P10.
Keep requirements and exact shared surfaces in their owning contracts; put run
results and implementation gaps in evidence.

| Concern | Required design | Verification and sequencing |
| --- | --- | --- |
| Modularity and encapsulation | Architecture maps actual modules, owned types, public APIs, allowed/forbidden dependencies, integration owners and composition root | Adopt a project-local dependency checker, shared by local/pre-commit/CI checks, before dependent features. Prove allowed and forbidden edges; test cycle/private-access checks where supported. Inventory existing violations individually and prevent new debt; name semantic review limits |
| Configuration | Entrypoints construct and inject one typed configuration object; declare precedence, one owner per key, required injected operator settings without defaults, committed developer-owned nonsecret values and secret handling. Browser libraries receive explicit inputs and remain independent of process environment | Test precedence, missing/invalid settings, isolated test configuration and redaction. Python application settings follow the selected runtime concern; review narrowly scoped library exceptions explicitly |
| Observability | OpenTelemetry governs applicable signal semantics. Contracts define safe event/mapping/retrieval surfaces; development runners retain bounded attributable diagnostics and clean protocol output | Sequence contract, capture/instrumentation and real receiver/mapping plus failure/privacy tests before a diagnostic pilot. Verify bounded queues/capture, loss reporting and shutdown. Service SLOs apply to services; measure overhead with small functional checks, keeping scale qualification separately requested and budgeted |
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

Prioritize runnable functionality. Each behavior slice needs basic happy-path
coverage and a meaningful failure case where applicable. Add broader verification
only for a demonstrated defect or a specific support claim; do not require
exhaustive runtime inventories, repeated custody audits or proof archives for
ordinary delivery. Reuse existing engine evidence and avoid retrying native runs
without a diagnosed fix. Native claims still need actual native execution. Review before
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

## Release continuation — current upstream cohort

This continuation orders the remaining release work against current public upstream releases. P01–P11 and their acceptance obligations remain in force; this section replaces their execution order where a newer dependency or release goal changes it. Historical original UMF 0.7/0.8 models, Truss 0.0.22 receipts and older Weft/engine evidence retain their declared versions and scope.

| ID | Outcome and dependency | Acceptance evidence |
| --- | --- | --- |
| R01 | Keep the qualified development baseline explicit: UMF `umf-v0.5.3` with core `0.10.0`, access `0.1.0.dev5` and codegen `0.1.0a2`; Weft `0.4.48`; Truss `0.0.44`. New public Weft `0.4.49` and Truss `0.0.45` require separate versioned adoption; the next Ashlar-required Weft target is a proposed `0.4.50` candidate, not yet a public or native-qualified release. Record exact source revision, package identity and public interface for each active profile. Migrate one compatible cohort at a time with targeted checks of changed public boundaries, optional dependencies and runtimes; retain original-version fixtures unchanged. | A reproducible dependency inventory and clean installed-package import/command smoke for each selected cohort. A version label or source comparison alone is not behavioral qualification. |
| R02 | On R01, qualify the current core 0.10 typed-graph profile through public UMF validation, a small local Delta create/update/delete publication, exact replay and publication-bound native singleton/Weft read. Execute both selected current-value paths: Backend03 grouped graph preparation and Backend02 SDK page/continuation, using their original compiler artifacts and publication pins. Keep UMF interpretation and DDL in UMF and compilation in Weft. | Exact original model/source/binding hashes, identities, values, presence, independent edges, full publication vector, grouping/multiplicity and SDK page continuation against an independent oracle, plus a meaningful refusal; selected local Python/Spark/Delta/JDK/JAR versions and elapsed/resource observations. An SDK preparation or component test alone does not qualify native execution. Historical managed runs remain separate versioned evidence. |
| R03 | Preserve the completed Truss 0.0.44 current-core catalog, mutation and complete feed/ACK lifecycle as versioned development evidence. Adopt public Truss 0.0.45 through a separately named current source/runtime profile after checking its accepted report, complete view, original source and installed runtime release against CONTRACT-005/P10. Keep the 0.44 profile and receipts intact. | A fresh 0.45 source acceptance and focused catalog/admission/refusal checks precede any 0.45 publication claim; exact accepted IDs/reports/head, source positions and durable ACK after publication remain required for the selected release. Local source authority does not grant production authority. Do not relabel Truss 0.0.22 or 0.0.44 receipts. |
| R04 | After R03 establishes actual accepted source and complete feed custody for the selected Truss version, freeze a fresh versioned current-core 0.10 eight-slot A/B request schedule and its independent source identities, epochs, installations and ACK scopes before the first native publication. Execute that exact schedule through local publication, retained old reads, replay/restart, source-specific durable ACK and evolution refusal. The completed 0.44 single-source lifecycle is a narrower baseline; the old `native-r2` 3/8 original UMF 0.8 run stays historical and incomplete. In an independent lane, complete P06/P07 held Fabric commerce upload, load, R1/R2 activation and interrupted-replacement witness. | Native evidence for every frozen current request on the selected version, complete publication vectors and source-specific progress/ACK, independent identities despite overlapping local IDs, exact values and no mixed Fabric activation. A lost PUT response and native 503 remain unresolved attempts until reconciled. Fabric execution requires dedicated capacity and approved resource/cost bounds. |
| R05 | Qualify production functionality and performance on the owner-selected workload and target, including the 1B-node/assumed 5B-edge planning scale. Fix acceptance thresholds for singleton latency, publication/ACK freshness and throughput, retention and cost from owner decisions before a trial. | One explicitly selected Databricks runtime/Delta/Unity Catalog configuration, dedicated compute and approved resource/cost ceiling; actual publication, ACK and native-read correctness plus measured latency, freshness, throughput and cost. Existing 1M/10M direct-insert layout results and provisional 250 ms/60 s goals are inputs, not a production pass. No shared default cluster or predictive-optimization disablement. |
| R06 | After R01–R05, reconcile every P01–P10 support claim, build wheel and sdist, install outside the checkout, run only changed-behavior and required final checks, obtain an independent review and prepare the release handoff. Recheck latest upstream versions at the candidate cut. | Evidence matrix qualifies exact versions/subsets/authority; package metadata, dual-license files and required resources are present; reviewed main equals origin and required checks pass on the selected revision. |

The [Truss 0.0.44 lifecycle receipt](evidence/truss-current-native-lifecycle-0.10-20261021/receipt.json)
now records one real seven-change page, local Delta publication, a matched protected
destination receipt and an advanced public checkpoint established by read-only
reconciliation. It does not qualify two-source R04 or Truss 0.0.45. The
[0.45 source comparison](evidence/truss-current-native-lifecycle-0.10-20261021/truss-0.0.45-source-comparison.json)
found unchanged signatures for the used public catalog preparation,
accepted-document and receipt-reconciliation calls, while packaged runtime SQL
and catalog behavior changed. First pin a new 0.45-only Ashlar profile and
installed release identity, keep the 0.44 branch, verify named records/fields
with the fresh accepted source, then run focused admission/refusal and one bounded
fresh-source publication/receipt check. A package replacement requires a fresh
Truss process; an unchanged signature or feed SQL digest is preparation evidence,
not a runtime pass. Select Truss `[local,native]`; its optional `[query]` Weft
dependency must be reconciled with Ashlar's separately selected Weft candidate
before enabling that extra. Preserve the frozen R04 schedule until the selected
source version and actual public acceptance are established.

R05 starts with one separately defined, small functional trial in the owner-selected
`client_dev` Unity Catalog catalog. Create a new per-run temporary schema, use only
that trial's owned objects, and delete them after retaining the results; verify their
absence. Owner authorization prefers at most $10 and sets a hard $100 ceiling for
the total Databricks trial. Before launch, identify the actual runtime, dedicated
serverless task, principal, production source authority, catalog/schema permissions,
fixture/request digest, finite operation and row bounds, concurrency, timeout and
estimated total DBUs/cost including startup. Reserve enough of the ceiling for
cleanup and stop before launch if the estimate cannot respect the hard ceiling.
Record actual usage/cost and check the ceiling during execution; serverless custom
executor caps, where available, are not an invoice cap. A new dedicated task uses
no retries and a timeout that includes observed startup and bounded work; the old
360-second layout-job timeout is not a valid default for the functional trial.
Keep predictive optimization enabled; a task-level `disable_auto_optimization`
setting, if selected, has a different meaning and needs its own justification.
The trial runs current-core schema creation, publication/update/delete, exact
replay, protected source ACK, retained native singleton and both current Weft
paths; retain native identity/vector, result, timing and cost evidence. The
read-only discovery receipt is
[managed-client-dev-discovery.json](evidence/current-upstream-release-20261021/managed-client-dev-discovery.json):
it identifies `client_dev` as a managed catalog and the historical unscheduled
dedicated layout job (`401862977233947`, concurrency one), which does not execute
this workflow. No new compute was started by discovery. The small functional trial
does not qualify 1B nodes/assumed 5B edges or imply that full materialization fits
its budget. After correctness and owner thresholds are fixed, that scale goal
requires its own feasible resource/cost estimate and explicit approval.
The [current public retail price observation](evidence/current-upstream-release-20261021/managed-current-retail-pricing.json)
records $0.47 per DBU for the selected Central US premium automated serverless
meter; it is an estimate input, not an account invoice or spending limit.

R02 and R03 can proceed independently once R01 pins are fixed; R04 source and engine lanes are independent. Reuse valid receipts and run a focused check when a changed pin or demonstrated gap affects a claim. A missing external prerequisite remains an open row, never a simulated pass. Development measurements describe their local environment and do not replace R05. A scoped prerelease may carry only demonstrated claims; full P11 completion requires every applicable row and any audit findings to close.

The R04 Fabric lane remains pending. A [read-only reconciliation](evidence/current-upstream-release-20261021/fabric-held-readonly-reconciliation.json)
on 2026-10-21 observed the same workspace capacity assignment and HTTP 404
`PathNotFound` for the exact first node directory of both the lost-response A
attempt and the capacity-refused B attempt. This establishes present absence at
those paths only; it cannot reconstruct the lost PUT response or qualify upload,
graph creation, activation or interrupted replacement. No write or compute was
requested in that reconciliation.
