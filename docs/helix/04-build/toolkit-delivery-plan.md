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
| R01 | Retain the qualified UMF `umf-v0.5.3` baseline as historical: core `0.10.0`, access `0.1.0.dev5`, codegen `0.1.0a2`; Weft `0.4.48`. The focused UMF `umf-v0.5.4` maintenance profile adopts access `0.1.0.dev6`; it records codegen `0.1.0a3` as an inspected cohort asset, not an Ashlar runtime dependency, and reuses the exact same core `0.10.0` wheel. The `.5.4` release is GitHub-assets-only, so Ashlar pins the required access/core wheel URLs and hashes directly. Its cohort Weft `0.4.49` is not adopted: Ashlar still pins `0.4.48`. Public Weft `0.4.51` excludes Ashlar's managed `.4` page target; the reviewed private `0.4.52` candidate is at commit `32ec02f0c3fd24a97b7726500dc5992c6475908d`; its Mac wheel SHA-256 is `598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46`, build receipt SHA-256 `1396c4fc2b50ac34eb413fd40e59d54b3bea3e678c563d8edc0b743a15ffcf05`, and focused functional receipt SHA-256 `16b987ebb2e0ad5e419676e5a63cd827ca3eb94716fd75d3afff31374c10e441`. This private candidate is unpushed; its Mac checks do not qualify native UC, a Linux artifact, public release or an Ashlar pin change. Truss `0.0.44` remains qualified historical evidence; `0.0.45` is the retained package/source-preparation profile with qualification in progress. Truss `0.0.49` is the selected new provider/admission profile; Truss `0.0.48` remains the retained accepted source and migration predecessor, with its original UMF 0.10 source acceptance at revision 1 and separate ACL-scoped source/view recovery preserved. The `.49` migration, fresh source acceptance, graph, feed, ACK, destination and production-native qualification remain pending; upstream Linux migration evidence does not establish Ashlar or macOS native support. Record exact source revision, package identity and public interface for each active profile; retain original-version fixtures unchanged. | The [UMF 0.5.4 adoption receipt](evidence/current-upstream-release-20261022/umf-0.5.4-adoption.json) records wheel custody, focused page/admission checks and unchanged core carrier regeneration. The [Weft 0.4.51 gap receipt](evidence/current-upstream-release-20261022/weft-0.4.51-managed-page-gap.json) records the exact public-wheel refusal. The [Truss 0.0.48 adoption packet](evidence/current-upstream-release-20261022/truss-0.0.48-adoption-preparation.json) records the committed source acceptance and separate ACL-scoped source/view recovery; it did not execute graph, feed, publication, destination or ACK. The [Truss 0.0.49 adoption packet](evidence/current-upstream-release-20261022/truss-0.0.49-adoption-preparation.json) records the exact published wheel, manifest, migration recipe, Mac exact-payload qualification separately from hosted Linux qualification, and offline provider controls. The private `0.4.52` Mac candidate passed four focused managed-page cases, two Ashlar read controls with UMF `.5.4`, and selected Truss source-F compiler controls; the selected Linux artifact, public push, native UC and release qualification remain pending. These local checks do not qualify native UC, private consumers, production installability, release publication or an Ashlar Weft pin change. A version label or source comparison alone is not behavioral qualification. |
| R02 | On R01, qualify the current core 0.10 typed-graph profile through public UMF validation, a small local Delta create/update/delete publication, exact replay and publication-bound native singleton/Weft read. Execute both selected current-value paths: Backend03 grouped graph preparation and Backend02 SDK page/continuation, using their original compiler artifacts and publication pins. Keep UMF interpretation and DDL in UMF and compilation in Weft. | Exact original model/source/binding hashes, identities, values, presence, independent edges, full publication vector, grouping/multiplicity and SDK page continuation against an independent oracle, plus a meaningful refusal; selected local Python/Spark/Delta/JDK/JAR versions and elapsed/resource observations. An SDK preparation or component test alone does not qualify native execution. Historical managed runs remain separate versioned evidence. |
| R03 | Preserve the completed Truss 0.0.44 current-core catalog, mutation and complete feed/ACK lifecycle as versioned development evidence. Retain the 0.0.45 committed seven-operation source seed and unapplied offline update/delete request as historical preparation. Keep the original 0.0.48 accepted UMF 0.10 source, report, complete view, IDs and registered consumer unchanged. Add exact public Truss 0.0.49 as a new versioned provider/admission profile. Before resuming the existing consumer, take the selected database and durable recovery-registry backups, then use Truss's public latest-only migration route from exact 0.0.48 manifest c433… to 0.0.49 manifest 288801…; verify the original source and complete view under the migrated runtime. Resume from the already committed genesis checkpoint, then execute the bounded local 0.4 Delta genesis publication/independent verify/ACK. Only after that ACK, make the approved seed page visible for its first submission, then verify and ACK that publication. Do not repeat the committed source acceptance or source/consumer registration, or replay any already committed effect or ACK. The 0.48 campaign did not submit the seed. Retain all prior failure receipts unchanged; no Ashlar 0.49 native migration or lifecycle result is claimed yet. | The [0.48 adoption packet](evidence/current-upstream-release-20261022/truss-0.0.48-adoption-preparation.json) preserves the revision-1 acceptance, registrar-only refusal and separate ACL-scoped source/view recovery; it did not execute graph, feed, publication, destination or ACK. The [0.49 adoption packet](evidence/current-upstream-release-20261022/truss-0.0.49-adoption-preparation.json) records the published wheel, manifest, migration recipe and Mac exact-payload qualification separately from the hosted Linux qualification. Code/profile controls do not execute migration or feed operations. A 0.49 migration/consumer resume and each publication/ACK remain native gates requiring bounded resource review and an explicit execution slot. Local source authority does not grant production authority. Do not relabel Truss 0.0.22, 0.0.44 or 0.0.48 receipts.
| R04 | After R03 establishes actual accepted source and complete feed custody for the selected Truss version, freeze the exact canonical bytes and digest of a fresh versioned current-core 0.10 eight-slot A/B semantic schedule, including independent source identities, epochs, installations and ACK scopes, before the first native publication. Resolve each exact request from verified predecessor results and durably bind its bytes and digest to the schedule digest before that slot’s source mutation. Do not begin a slot until its predecessor’s graph publication, independent verification and protected ACK complete. Execute the schedule through local publication, retained old reads, replay/restart, source-specific durable ACK and evolution refusal. The completed 0.44 single-source lifecycle is a narrower baseline; the old `native-r2` 3/8 original UMF 0.8 run stays historical and incomplete. In an independent lane, complete P06/P07 held Fabric commerce upload, load, R1/R2 activation and interrupted-replacement witness. The [fresh Fabric functional attempt plan](evidence/commerce-fabric-fresh-functional-plan-20261023/README.md) is a bounded offline candidate, not launch authorization or execution evidence. | Native evidence for every frozen current request on the selected version, complete publication vectors and source-specific progress/ACK, independent identities despite overlapping local IDs, exact values and no mixed Fabric activation. The historical A ambiguous OneLake upload and native 503 remain unresolved until reconciled. Fabric execution requires dedicated capacity and approved resource/cost bounds. |
| R05 | Size the owner-selected 1B-node/assumed 5B-edge workload from one actual canonical-carrier sample before a full run. Show node/edge bytes, 1M-node write time, an 80%-of-measured-throughput planning curve and its limits; if the full-run estimate exceeds 15 minutes, review it with the owner before execution. The desired test is one full materialization, two point lookups, two scans and actual graph-engine export/execution, runnable locally; keep the small real Truss/managed UC publication and ACK campaign separate. | A selected full run occurs only after sample-based admission, with actual physical bytes, row and endpoint accounting, an engine result and workload time; provisioning is reported separately. Compact algorithmic representations and wide canonical carriers are separately named and cannot substitute for each other. No shared default cluster, redundant stress matrix or predictive-optimization disablement. Existing approved $100 total ceiling, preferably $10, governs Databricks activity. |
| R06 | After R01–R05, reconcile every P01–P10 support claim, build wheel and sdist, install outside the checkout, run only changed-behavior and required final checks, obtain an independent review and prepare the release handoff. Recheck latest upstream versions at the candidate cut. | Evidence matrix qualifies exact versions/subsets/authority; package metadata, dual-license files and required resources are present; reviewed main equals origin and required checks pass on the selected revision. |

The proposed managed R04 campaign uses eight sequential, one-slot dedicated
serverless Jobs tasks through the existing managed trial entrypoint. After each
terminal job, an independent dedicated SQL read must verify that slot's exact
durable publication/vector and destination receipt; only then may the matching
source submit and read back its protected ACK before the next slot. This follows
CONTRACT-001's ACK-gated schedule. Combining A1/B1 in one task while deferring A's
ACK would require a new source/receipt orchestration boundary and is not this
campaign. Run the selected Linux Weft grouped, negative-HAVING, first-page and
continuation cases once on an admitted A2 publication; other slots need their
own vector, source-specific ACK and retained-read checks, not repeated query
matrices.

Before creating the fresh `client_dev` schema or starting any compute, freeze
actual accepted A and independently installed B source identities, all eight
request bytes and expected states, selected runtime/principal, one owned schema,
per-task timeout, dedicated SQL endpoint and complete Jobs/SQL/cleanup cost
estimate. Propose one bounded A1 pilot with a five-minute task maximum,
concurrency one, no retries and a separate finite caller wall deadline that
includes startup, cancellation and cleanup; adjust these proposed bounds before
launch if the selected provider's task timing requires it. Admit the actual
serverless resource shape, Jobs and SQL meters, applicable rate, caller wall
deadline and cleanup reserve before launch. At an assumed 120 Jobs DBU/hour,
eight five-minute maxima imply 80 Jobs DBU or $37.60 at the observed
$0.47/DBU retail rate; this reference envelope omits SQL, variable driver and
startup billing and is neither an expected bill nor an invoice cap. Favor the
$10 total preference where the admitted estimate permits it, under the approved
$100 hard ceiling. After A1's terminal receipt and ACK, use its actual elapsed
time and fresh usage when available to project seven remaining slots plus SQL
and owned cleanup. If billing telemetry is delayed, use the admitted resource
shape, rate and caller-duration upper bound with the full cleanup reserve.
Stop for review when neither measurement nor a reliable conservative bound can
cover the remaining campaign, or when projected total spend exceeds the $100
ceiling; check the envelope before each later slot. No pilot
starts until the two sources and complete schedule are frozen. The historical
unscheduled layout job does not execute this flow.

R05 has two distinct proofs. The small managed UC campaign qualifies actual
selected-version Truss 0.0.49 acceptance, incremental publication, source ACK and current Weft
reads using the affected-key design in [TD-006](../02-design/technical-designs/TD-006-managed-current-incremental-publication.md).
The full-size algorithmic check uses a canonical `.4` synthetic Delta fixture
with full Delta semantics and an independently checked deterministic graph
shape. Its generator and graph-engine adapter must be portable enough to run
locally. It must write the full 1B-node/5B-edge fixture,
measure actual bytes and independently account for IDs, endpoints and counts;
a lazy range, arithmetic-only result or metadata-only export is not a pass.
The proposed [pinned-Delta GraphFrames action](../02-design/technical-designs/TD-008-pinned-delta-graphframes-scale-action.md)
uses Spark DataFrames for one actual bounded engine operation, while the existing
10,000-row JSON release and GraphFrames export retain their historical limits.
The local synthetic fixture retains full `.4` Delta semantics, but does not
establish managed Unity Catalog custody or production authority. Both proofs
retain their own authority. The preserved
`ashlar-delta/0.3` carriers use SHA-256 hex `lookup_hash`. The separately
versioned `ashlar-delta/0.4-xxhash64` physical profile in
[TD-007](../02-design/technical-designs/TD-007-native-xxhash64-lookup-layout.md)
implements Spark's signed 64-bit `xxhash64` `BIGINT` pruning field while retaining
exact native identity predicates. Its [same-fixture local sample](evidence/current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-sample-20261021.json)
reduced active Parquet bytes by 68.85% at 1M nodes/5M edges; observed scan
plans do not prove file pruning. This does not qualify full-scale or managed
Unity Catalog behavior, and no old SHA-256 receipt is relabeled.
The next local functional gate is an explicit fresh `0.4-xxhash64` Truss
publication: ACK the catalog-only genesis page after durable vector and schema
validation, then publish and ACK its graph-change page after native hash and
row-bag validation under the same profile and retained source receipt. Keep
`0.3` as the local default and refuse mixed-profile continuation. This is local
development evidence only.

The [Truss 0.0.44 lifecycle receipt](evidence/truss-current-native-lifecycle-0.10-20261021/receipt.json)
now records one real seven-change page, local Delta publication, a matched protected
destination receipt and an advanced public checkpoint established by read-only
reconciliation. It does not qualify two-source R04 or Truss 0.0.45. The
[0.45 source comparison](evidence/truss-current-native-lifecycle-0.10-20261021/truss-0.0.45-source-comparison.json)
and isolated [public artifact probe](evidence/truss-current-native-lifecycle-0.10-20261021/truss-0.0.45-public-artifact-probe.json)
record the installed official 0.45 wheel, runtime manifest and effect-free public API checks.
The Ashlar 0.45 current-catalog and graph-admission profiles are separately versioned;
the 0.44 path remains intact. The release pins native support to `linux-x86_64`,
so this evidence does not qualify native operation on other platforms. The
source comparison found unchanged signatures for the used public catalog preparation,
accepted-document and receipt-reconciliation calls, while packaged runtime SQL
and catalog behavior changed. Keep the new 0.45-only Ashlar profile bound to its
installed release identity, retain the 0.44 branch, verify named records/fields
with the fresh accepted source, then run focused admission/refusal and one bounded
fresh-source publication/receipt check. A package replacement requires a fresh
Truss process; an unchanged signature or feed SQL digest is preparation evidence,
not a runtime pass. Select Truss `[local,native]`; its optional `[query]` Weft
dependency must be reconciled with Ashlar's separately selected Weft candidate
before enabling that extra. Keep the frozen R04 schedule unchanged; this
single-source seed does not admit any R04 slot or establish feed/ACK custody.

The retained 0.45 source-preparation run has now committed and publicly read
back one atomic seven-operation seed group, including exact committed-receipt
replay. Its worker failed afterward while freezing the next request; offline
recovery froze the five-operation update/delete request without applying it.
This local macOS arm64 run does not qualify the release's listed Linux native
platform, and no feed, publication, destination, ACK, or query was exercised.
See the [attempt 03-r2 evidence](evidence/truss-current-native-lifecycle-0.10-20261021/truss-0.0.45-seed-and-offline-freeze-attempt-03-r2.json).

For the next R04 candidate, Truss 0.0.49 is the selected source/provider
profile. The already accepted 0.0.48 source remains its retained migration
predecessor; complete and verify the exact latest-only migration before any
0.49 feed or publication work. Then A and B each need a fresh selected-version
accepted UMF 0.10 installation, registration, feed, epoch, namespace, ordinary
consumer and protected destination ACK scope. The intended interleave is A1,
B1, A2, B2, A3, B3, A4, B4: one complete source transaction per slot, with
create, update, delete and additive-candidate stages for each source. Preserve
an overlapping local entity ID across A/B to prove source-qualified identity.
This is a sequence shape, not the frozen request schedule or an assertion that
0.49 has accepted those mutations. After actual public catalog acceptance,
freeze the exact canonical bytes and digest of the full semantic schedule, source
assignments, model/revision pins, expected values and both source/ACK registrations
before the first native graph publication. Resolve each slot's original mutation-
request bytes and digest from verified predecessor results and bind them to the
schedule digest before that slot's source mutation. Do not start a slot until the
preceding slot's graph publication, independent verification and protected ACK
have completed. At each ACK-gated feed step, bind the actual writer position,
event-byte inventory and source checkpoint to its already frozen request, refusing
a changed or missing page before that slot's effects. The owner approved staged
feed-page binding on 2026-10-21 and R04 request-freeze timing on 2026-10-23. These
decisions distinguish the frozen semantic schedule and per-slot original request
bytes from feed pages bound at their ACK-gated slots. The prior Truss
0.0.45 `read_feed_in_transaction(tx, consumer_id, limits)` has no future-page
cursor argument and requires the registered checkpoint; recheck the exact public
0.0.49 feed signature and custody before freezing that version’s requests. The
2026-10-21 feed-page decision and 2026-10-23 request-freeze decision approve
only those respective timing rules; neither establishes R04 execution.
Read back each durable four-data-role manifest vector and both
progress entries; ACK only the publishing source after independent receipt
verification. Retain older
publication reads, exact no-new-commit replay, fresh-process journal restart,
uncertain-operation/missing-journal refusal and breaking-evolution refusal.
The 0.44 single-source installation and old `native-r2` prefix supply no slots.
Version CONTRACT-001/003/005 for the selected 0.48 source before claiming this
new profile. The R03 fresh source may supply R04 or the bounded UC trial only
where the exact source identity and separate consumer/destination ACK custody
are independently retained; never share a cursor or treat one destination's
ACK as another's.

R05 starts with one separately defined, small functional trial in the owner-selected
`client_dev` Unity Catalog catalog. The selected Truss provider profile for new
work is `0.0.49`; the already accepted `0.0.48` source remains its retained
migration predecessor until the exact migration is completed and verified. No
0.49 native lifecycle is claimed. Create a new per-run temporary schema, use only
that trial's owned objects, and delete them after retaining the results; verify their
absence. Owner authorization prefers at most $10 and sets a hard $100 ceiling for
the total Databricks trial. Before launch, identify the actual runtime, dedicated
serverless task, principal, selected development source authority and its
production-authority gap, catalog/schema permissions,
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
does not qualify 1B nodes/assumed 5B edges. For the separate full-size check,
freeze one deterministic canonical `.4` graph shape and independent
row/endpoint oracle before execution. The historical [1M/5M canonical-carrier sample](evidence/current-upstream-release-20261021/local-current/canonical-carrier-sample-20261021.json)
wrote 1M nodes in 4.872 seconds and 5M edges in 4.205 seconds. The later
[18-core `.4` calibration](evidence/current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-18core-calibration-20261021.json)
measured 3,062,253,112 active Parquet bytes at 45M nodes/225M edges and projects
about 68.05 decimal GB for 1B/5B by row-linear arithmetic. Its 80%-throughput
write-only projection is about 18.05 minutes, still over the 15-minute review
threshold and explicitly held. This single synthetic sample is not a production
capacity forecast across payloads, hardware, parallelism or table geometry; the
full test remains held. See the dated [R05 storage and throughput reassessment](evidence/r05-storage-throughput-reassessment-20261023/README.md)
for measured contributors and exclusions. The owner's 2026-10-23 release
decision is to retain full Delta semantics and use this measured approximately
68.05 GB active-Parquet envelope for the synthetic target; no compact
representation investigation is required for this release. This records the
release tradeoff only: it does not qualify production capacity or authorize the
held full run. Once admitted,
physically generate all rows once, then perform two real point lookups and two
real scans on the stored representation and admit it to an actual graph engine
that executes a bounded result. Record provisioning separately from measured
materialization, reads and export. Preserve one original attempt and its exact
terminal result if it misses the desired few-minute workload time. The local implementation and selected
dedicated Databricks integration use comparable fixture identities and checks,
but each qualifies only its actual engine, encoding and runtime. The cloud run
stays within the existing $100 hard ceiling and $10 preference, reserves
cleanup, uses no automatic retries and reports actual DBUs and bytes. The
synthetic full-size check does not prove a complete Truss ingestion cycle,
managed Unity Catalog custody or production capacity; those remain separate
support claims.
The [current public retail price observation](evidence/current-upstream-release-20261021/managed-current-retail-pricing.json)
records $0.47 per DBU for the selected Central US premium automated serverless
meter; it is an estimate input, not an account invoice or spending limit.

R02 and R03 can proceed independently once R01 pins are fixed; R04 source and engine lanes are independent. Reuse valid receipts and run a focused check when a changed pin or demonstrated gap affects a claim. A missing external prerequisite remains an open row, never a simulated pass. Development measurements describe their local environment and do not replace R05. A scoped prerelease may carry only demonstrated claims; full P11 completion requires every applicable row and any audit findings to close.

The R04 Fabric lane remains pending. A [read-only reconciliation](evidence/current-upstream-release-20261021/fabric-held-readonly-reconciliation.json)
on 2026-10-21 observed the same workspace capacity assignment and HTTP 404
`PathNotFound` for the exact first node directory of both the lost-response A
attempt and the capacity-refused B attempt. This establishes present absence at
those paths only; it cannot reconstruct the lost PUT response or qualify upload,
graph creation, activation or interrupted replacement. The separate [fresh
functional attempt plan](evidence/commerce-fabric-fresh-functional-plan-20261023/README.md)
names new candidate identities and read-after-ambiguous-write stop rules, but
retains explicit R2 source/ACK, runtime-bound, and cost-admission blockers. No
write or compute was requested by either the reconciliation or this plan.

### Draft alternative for managed A1/R04 compute (TD-009)

The [TD-009 proposal](../02-design/technical-designs/TD-009-managed-spark-receipt-verification.md)
replaces neither the recorded serverless Jobs/SQL campaign nor its launch gates
until reviewed and adopted. It proposes a publisher followed by a fresh independent
Spark verifier task/session on one dedicated single-node Jobs cluster, then protected
ACK by the original external source holder before the next global slot. The narrow
proposed CONTRACT-001/003/004 amendments govern this candidate; the existing SQL
reader remains serverless current-channel only. An offline read-only implementation
is the first bounded slice. Live external read/pin renewal through ACK, exact
UC-capable runtime/access-mode admission and actual Jobs/VM/startup/idle/cleanup
rates remain prerequisites. Prefer total cost below $10 under the hard $100 trial
ceiling; no estimate or single-node selection guarantees that bound. No compute,
source mutation, ACK or new qualification follows from this proposal.

The [fixed-Jobs retail observation](evidence/current-upstream-release-20261023/managed-fixed-jobs-retail-observation.json)
verifies Central US/Premium, $0.255 per Linux D4ds_v5 VM-hour and $0.30 per
Premium Jobs DBU-hour. The actual node DBU/hour meter remains open. At an assumed
maximum 2 DBU/hour and 35 minutes per complete slot, eight slots cost $3.99 in
compute; assumed campaign ancillary/cleanup reserves of $5 or $20 give $8.99 or
$23.99. These are conditional planning estimates, not actual usage, admitted
budget or launch approval. Resolve effective predictive optimization, residual
meters, live ACK time and terminal cleanup before admitting either envelope.
