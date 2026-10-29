# Arras AS-0 local boundary and no-op coverage candidate — 2026-10-29

This record covers local contract and code inspection only. No Truss migration,
PostgreSQL source run, native Delta publication, ACK, Databricks job, scale trial,
release asset or remote push was performed. The selected Ashlar code remains a
candidate until one bounded original-source/native continuation and restart
receipt proves its durable effects.
The [exact changed-file manifest](arras-as0-local-20261029-manifest.json) and
[continuation record](arras-as0-local-20261029-continuation.md) bind this
candidate's selected files and remaining gates.

## Source and installed cohort

Ashlar currently pins Weft 0.4.53, UMF core 0.10.1/access 0.1.0.dev7 and the
`truss-toolkit` 0.0.50 wheel by exact URL/hash. Ashlar's current-catalog and
`.4-xxhash64` admission gates accept Truss 0.0.48–0.0.50 with version-specific
runtime provenance. None of those gates accepts 0.0.51. The public
[`v0.0.50…v0.0.51` comparison](https://github.com/DocumentDrivenDX/truss/compare/v0.0.50...v0.0.51)
shows a `truss-toolkit` → `umf-truss` distribution rename and a changed migration
manifest/added migration artifact, with no changed Python execution module under
`packages/python/src/truss` in that tag delta. This is a source-diff observation,
not installed-wheel or migration qualification. A reviewed .51 wheel hash,
installed runtime provenance, dependency resolution and focused current-source
checks are prerequisites to selecting .51; the allowlists were not widened.

## AS-0 dispositions

1. **Opaque receipt comparison:** Ashlar has complete publication pins and
   source-progress custody, but no public source operation that validates a
   person's Truss token against a supplied immutable source-retained
   destination ACK receipt. A cursor alone is unsigned caller data, even when
   its registration and `nextXid` are well-formed. The proposed source
   operation must match all receipt bytes and bindings to Truss's retained
   receipt, then compare the authoritative writer within Truss.
   Truss `reached_in_transaction(..., consumer_id=...)` observes the source
   consumer checkpoint, which can be ahead of a held Ashlar publication. The
   proposed four-way destination result is specified in CONTRACT-002; no
   runtime `included`/`known_behind` claim is made. The source API is an
   upstream dependency. Absence, epoch mismatch, authorization failure and
   missing proof remain unknown or denied without leaking source progress.
2. **Complete no-op coverage:** Original `empty_interval` bytes now form a
   progress-only source batch when its next cursor advances and its transaction
   member lists are empty. The local continuation uses the existing durable
   source-plan, manifest, full-vector and publication-before-ACK path; unchanged
   cursors remain polls. Exact archives and restart admission now preserve a
   null writer and safe frontier. The predecessor's schema revisions carry
   forward; no new catalog semantics are inferred from an empty page.
   The selected [Truss 0.0.50 feed source](https://github.com/DocumentDrivenDX/truss/blob/v0.0.50/packages/python/src/truss/_catalog_runtime/feed.sql)
   constructs this page with `writer = NULL`, an empty member set and a next
   cursor at the captured committed frontier.
3. **Arras authored reads:** The current pinned singleton bridge has an owning
   Ashlar operation. The concrete existing local `entity-page` path is
   `configured_typed_graph_sdk_request` → Weft
   `compile_engine_page_json` → `EngineRead.from_original` →
   `AshlarTarget.page`. Its Backend02 `weft-sql/0.2.0` builder admits required
   facetless STRING keys/properties and STRING parameters over an original
   configured UMF 0.10 graph and local Spark aliases. That is a `.3` local
   candidate, not a person-scoped managed `.4-xxhash64` execution proof.
   `managed_current_sdk_pages_request` and the `.4` target allowlist are
   separate candidates. Arras has located the bounded Backend02 `.4` page
   composition from its accepted source bundle and `weftBindingInputs` through
   the original compiler result, typed `EngineRead` and target. Ashlar still
   lacks a production person-authenticated managed read-session opener with
   mandatory policy and custody; the campaign owner requires fixture expected
   rows. Backend03 grouped queries and
   general SQL remain separate, unsupported work; Arras must not parse or
   rewrite Weft SQL.

## Focused local checks

`PYTHONPATH=src:tests python3 -m unittest` on the ten affected Truss feed,
archive, graph admission, continuation, resume and publication test modules:
41 tests passed in 0.316 seconds. New cases cover exact no-op batch custody,
member-list refusal, archive round-trip with null writer, dedicated no-effect
admission, advancing progress-only continuation and unchanged-cursor polling.
`git diff --check` passed. These checks use fakes for native/source effects and
do not establish an installed Truss 0.0.50 lifecycle or production runtime.

## Remaining release gates

The next local gate is one bounded original .50 source and `.4` Delta no-op
continuation with interruption/restart and exact retained destination receipt,
using a separately admitted machine/source budget. Receipt inclusion then needs
the Truss source-retained-receipt comparison API and one person-scoped
publication-pinned before/after case. Arras's bounded Backend02 mapping needs
the separate managed person-scoped opener and its execution evidence. Selecting
Truss .51 requires its own reviewed
installed cohort. Managed Unity Catalog, full lifecycle and production-scale
claims remain unqualified under the current project plan.
