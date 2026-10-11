---
ddx:
  id: TD-004
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
  - id: FEAT-002
    kind: informed_by
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-005
    kind: informed_by
  - id: CONTRACT-006
    kind: informed_by
---

# Installed Truss catalog preparation

## Purpose and approach

Provide the installed preparation invocation governed by CONTRACT-005. An
operator submits complete ordered UMF sources and explicit Truss configuration
through the selected public Truss package, retaining its original preparation
outputs and limitations. This supplies a reproducible schema-preparation handoff;
it grants no accepted identity, schema head, source authority, feed, ACK,
readiness or publication. Full P10 acceptance/feed requirements remain separate.

Keep Ashlar's portable core independent of Truss and its host runtime. A selected
Truss Python host at least 3.11 and release-matching Bun execute in an isolated
child; the portable parent retains its existing supported Python profile. Pin
package distribution/version, immutable source/build provenance, installed
resource namespace/origins and runtime bytes explicitly. A pin is provenance,
not producer authentication. Missing selected package/runtime refuses without a
checkout, download, sibling-source or alternate semantic implementation fallback.

## Owning components and public composition

`truss_preparation_configuration.py` owns the exact typed configuration and
bounded file/package/runtime custody described in CONTRACT-005.
`truss_preparation.py` owns child supervision, provisional result custody and
lossless exclusive output release. The installed CLI dispatches to that host
owner; core modules do not import Truss. The packaged child entry is selected
through a closed internal invocation, not a public callback/extension surface.
Apply existing module-boundary rules and run `tools/check_module_boundaries.py`.

The child imports `CatalogDocument` and `prepare_acceptance` from the selected
installed public `truss` exports. Build each exact document from unchanged bytes,
explicit ID/revision and artifact identity, preserving order. Pass the unchanged
configuration bytes and selected Bun path to the public call. The public package
owns original UMF interpretation, declared-version admission, transformations,
unknown-content retention and preparation refusal. Ashlar neither imports private
Truss producers nor interprets its owner observations as accepted catalog facts.

Capture all result byte views and complete provenance under CONTRACT-005.
Persist original documents and observations alongside input/declarations/archive/
report evidence without flattening, deleting unavailable fields, synthesizing
empty reports or merging distinct owner results. Package-generated observation
bytes may have the owner's encoding; never describe those as unchanged source
bytes. Source whitespace, Unicode, declared versions and unknown extension bytes
remain in their separate original source carriers. A later owner-supported
version transformation is a distinct retained observation, not a rewritten input.

The result directory contains complete immutable byte files and a bounded index
of their roles, sizes and digests, with original ordering and provenance. A compact
summary may identify preparation outcome and retained limitations, but all fields
remain available in the original views. The index does not certify producer
trust, installation or semantic enforcement. There is no schema/store migration.

## Bounds, lifecycle and diagnostics

The typed limits and ceilings are owned by CONTRACT-005. Before invoking Truss,
renew complete original file/config/package/runtime identities and admit aggregate
input/request representation capacity. The owner has fixed internal bounds;
Ashlar must not claim its external limits control all internal Truss allocations.
After the public result returns, measure every returned byte view before child
serialization, including duplication and base64/JSON expansion; reject aggregate
overflow without truncated successful output. A streaming bounded child transport
prevents a whole encoded result from bypassing capture capacity.

The parent owns the selected finite deadline, bounded streams and process cleanup.
Use the existing primary-safe capture/finish discipline. An isolated installed
child must prove selected interpreter/package/runtime origins before the call;
ambient `PYTHONPATH`, environment defaults and raw secret-bearing settings do not
supply selection. Verify input and output directory identity independently at
closing on every acquired interval exit, including failed opening, body error,
timeout and cancellation. Reap the child and close/fsync all owned handles before
releasing the receipt. Output refusal does not turn partial preparations into
accepted artifacts; prior source/store state is unchanged.

Use CONTRACT-006 bounded diagnostics with closed preparation phases and safe
categories. Never emit document/configuration contents, raw exception text,
credentials, runtime environment values or unbounded owner diagnostics into
telemetry. Complete owner refusal diagnostics remain in private bounded evidence
artifacts, distinct from diagnostic summaries. Preserve primary exception identity
when secondary capture or closing checks fail; attempt independent cleanup.

## Assurance properties and controls

| Property | Required observation |
| --- | --- |
| PREP-F1 original correspondence | Ordered document IDs/revisions/artifact identities and all original source/configuration bytes survive public call and release. |
| PREP-F2 complete preparation custody | Every raw view, provenance field, unavailable observation and unknown content survives; changed or omitted views cannot yield success. |
| PREP-F3 effect exclusion | No accepted ID/head, DB/source/ACK/feed/publication port is invoked or granted by prepared status. |
| PREP-F4 finite release | Ashlar input/transport/release representation limits hold before Ashlar buffering/serialization; upstream owner internal allocations remain separately bounded. Overflow releases no successful prefix. |
| PREP-L1 owned completion | Complete child transport, independent closing admission and cleanup precede receipt; errors/cancellation retain primary identity and withheld success. |

The consumer conformance plan owns the small positive/adversarial corpus. Run the
actual public installed-package fixture with its original explicitly declared UMF
version/configuration, plus unsupported-version and unknown-content cases.
Expected preparation availability follows that owner's response; an unavailable
operation must remain unavailable. Test aggregate multi-view overflow, changed
inputs/package/runtime origins, malformed config/output paths and cancellation
with independently failing cleanup. Inert controls prove host behavior; a fresh
installed public-package run separately qualifies the selected tuple. Neither
kind substitutes for accepted Truss schema/mutation/feed/ACK execution.

## Compatibility and unresolved authority

The command is additive and does not change existing raw intake, source adapters,
accepted binding plans or publication behavior. Removing its released preparation
artifacts has no database rollback semantics. Actual distributed wheel origin,
producer trust and every native acceptance/source profile require independently
selected evidence before corresponding claims; no preparation digest settles them.
