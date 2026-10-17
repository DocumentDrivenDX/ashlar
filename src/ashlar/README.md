# Ashlar toolkit components

Ashlar's Python3.9+ standard-library core supplies admitted schema intake,
whole-entity graph application, immutable publication resolution and guarded
query orchestration. Authenticated database transports and deployment tools
remain separate from the portable library. Start with the
[typed Truss quickstart](../../examples/truss-native/QUICKSTART.md) for ingest,
queries and graph export, or the [portable example](../../examples/end-to-end/README.md)
for source inspection and native commands. The
[immutable PuppyGraph workflow](../../examples/puppy-immutable-export/README.md)
queries retained release/custody files through bounded local Cypher or Gremlin.
For the complete original archaeology/ecology query gallery, use the
[original 17 PuppyGraph workflow](../../examples/puppy-original-scenarios/README.md).
With Python 3.11 or later and the documented SDK extra, select the pinned UMF
source, original input files and independently selected release/custody pairs:

```sh
ashlar query-original-pack-puppygraph --configuration /absolute/private/scenarios.json
```

Choose Cypher or Gremlin explicitly in the configuration. The command runs all
17 authored scenarios and exposes its report after source and runtime closing.
For a small SQL-only local check, the
[DuckDB export guide](../../examples/duckdb-immutable-export/README.md) covers
directed paths and counts from the same immutable files without a running source.

## Install and inspect a source

From the repository root:

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
ashlar inspect-configured-source examples/end-to-end/configured-source.json
```

Inspection preserves complete original transaction bytes, digests and text
cursors. It grants no source acknowledgement or native schema acceptance.
Configured-source inspection resolves explicitly pinned model, interpretation,
binding and data files; it does not invent catalog IDs.

For a pinned local compiler installation and original-request compilation, use
the [Weft installation guide](../../examples/end-to-end/WEFT-INSTALLATION.md).

## Component boundaries

| Components | Responsibility | Required external evidence |
| --- | --- | --- |
| `schema`, `binding`, `semantic_policy`, `schema_policies`, `schema_registry` | Retain UMF source/report custody and apply explicitly admitted bindings/revisions | Original UMF producer, supported interpretation and target admission |
| `source`, `csv_source`, `whole_entity`, `apply`, `recovery`, `outbox` | Preserve committed groups, typed identities, history and whole-entity changes | Source/epoch authority, complete interval and retention |
| `staging`, `attempt_store`, `manifest`, `publisher`, `stored_publisher` | Durable phase/effect/descriptor custody and original-handle recovery | Qualified writer fencing, authenticated transport and validation/ACK driver |
| `publication`, `native`, `singleton`, `pins`, `authority`, `protocol`, `retention_policy` | Resolve exact immutable vectors and gate reads | UUID/version/schema/file/protocol custody, current effective permission and finite readability |
| `weft_binding`, `weft_query`, `weft_decode` | Publication-bound compilation and guarded buffered result release | Original compiler/model/binding/profile evidence and every emitted integrity obligation |
| `weft_installation`, `weft_distribution` | Verify indexed compiler bytes and compose explicit local CLI installation/compilation | Independently admitted index, exact package evidence and actual qualified host observations |
| `truss_input`, `truss_feed`, `report_parts`, `profile_custody` | Preserve selected Truss artifacts and feed semantics | Real accepted catalog/report/head, source registrations and complete native feed/ACK |

No provider policy has a trusting default. A declared principal, stored validation
flag, supplied catalog ID or phase label is insufficient authority. Unknown model
meaning must be retained and reported or execution refused, never silently narrowed.
Truss property journals require their own semantic adapter; they must not be
relabeled as whole-entity events.

## Recover an uncertain protected outbox ACK

A `RegisteredOutboxAcks` owner can reconcile an `AckOutcomeUncertain` directly:

```python
try:
    receipt = acks.acknowledge(request_bytes, manifest_bytes, vector, **ports)
except AckOutcomeUncertain as uncertain:
    receipt = acks.reconcile_uncertain(uncertain, vector, **ports)
```

`ports` supplies the original context, live pins, publication backend and supported
profiles/revisions. Reconciliation verifies the complete registered source scope
and uses the exception's unchanged request and manifest bytes. It opens a fresh
ordinary connection and observes the protected receipt; it never submits another
ACK. A missing or conflicting receipt refuses. Keep the original exception and
publication custody for further recovery. The native publisher uses this path
automatically for registered outbox sessions.

## Query original commerce on an existing Truss publication

Use the saved Truss reader configuration with the installed public Weft 0.4.2
implicit-arithmetic realization and the unchanged original commerce pack:

```sh
python -m ashlar query-truss-commerce \
  --configuration /absolute/truss-reader.json --output /absolute/fresh-query.json \
  --weft-index /absolute/weft/index.json --weft-installation /absolute/weft/installation \
  --pack /absolute/commerce/pack.json --revision 2 --document-id urn:umf:domain:commerce \
  --case settlement --maximum-rows 1000
```

Choose `partial-return`, `fulfillment`, `settlement` or `refund`. Set the revision
and document ID to the actual accepted catalog selections; the example values
are not defaults. Preserve the reader's original publication intent, runtime and
explicit historical acceptance archives where needed. Each record must have one
actual physical revision; records may differ from one another. The command keeps
original SQL and positional columns, checks complete source populations before
capacity checks, and compares the result with an independent current-source
calculation. It reads an existing publication and leaves source ACK unchanged.
Select the existing local runtime and a fresh output; supervise the invocation
with a finite bound as described in the typed Truss quickstart. For a historical
read, select that publication's original intent and accepted logical revision;
keep explicit historical acceptance archives available to the reader. The live
source checkpoint can remain ahead of the selected publication. The
[original three-case evidence](../../docs/helix/04-build/evidence/truss22-commerce-original-three-queries-20261014/README.md)
and [evolved settlement evidence](../../docs/helix/04-build/evidence/truss22-commerce-indexed-settlement-20261014-c/README.md)
qualify the recorded local Truss/Delta and indexed compiler selections. Different
runtime/compiler bytes, managed Unity Catalog and production authority need
separate qualification.

## Diagnose retained medical recovery

For an existing original-medical installation and its pinned request, use the
same recovery configuration with a fresh private output and `native_seconds`
at most 60:

```sh
python -m ashlar preflight-original-medical --configuration /absolute/recovery.json
```

The command performs complete recovery preflight and closing without publishing
or advancing ACKs. Bounded `preflight-stage-NNN.json` records retain entered and
completed stages, including prior/current captures and committed receipt checks.
An unfinished stage is an observation to investigate; preflight success does not
prove publication completion. The [medical contract](../../docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md)
defines the exact configuration, authority and failure requirements.

## Query an existing original medical publication

Use the installed toolkit's supervised command with an existing original medical
read configuration and independently indexed compiler installation:

```sh
python -m ashlar query-original-medical --configuration /absolute/medical-query.json
```

The query configuration is a closed JSON object. Set `read` to the **complete
original medical read configuration object**, including its explicit runtime,
source/publication authority, bounds and fresh private output directory; a filename
or partial override is insufficient. Select the existing compiler with canonical
absolute paths:

```json
{
  "profile": "ashlar-original-medical-query-batch/0.1",
  "read": {},
  "compiler": {
    "index": "/absolute/weft/index.json",
    "installation": "/absolute/weft/installation"
  },
  "cases": ["singleton", "two-hop"]
}
```

Replace the illustrative empty `read` object before running. The
[consumer contract](../../docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md#installed-original-medical-query-command)
defines the required configuration and authority. Cases must be explicit, unique
and nonempty. `singleton` returns the full original encounter row; `two-hop`
checks five relationship-pair queries covering 20 original two-hop occurrences.
Select `filtered-limit` for the
ordered encounter list with `LIMIT 2`, or `all-objects` for all eight original
record projections. The original encounter population is one, so its limit does
not demonstrate truncation. Compiler and runtime support remain specific to the
recorded local profile; selecting a case alone does not qualify its execution.

The command reads the selected retained publication without advancing source
ACKs. Treat only successful command termination with its bounded JSON result as
completion. Private query evidence may remain after a later closing failure;
those files alone do not prove the command succeeded.

## Supported development evidence

The configured JSONL example has actual native
[publication/update/delete/history](../../docs/helix/04-build/evidence/native-configured-publication-complete-20261008.json),
[singleton](../../docs/helix/04-build/evidence/native-configured-singleton-20261008.json),
[guarded Weft query](../../docs/helix/04-build/evidence/native-configured-guarded-weft-query-20261008.json)
and [committed replay](../../docs/helix/04-build/evidence/native-configured-replay-20261008.json)
proof. The [typed Truss quickstart](../../examples/truss-native/QUICKSTART.md)
and [commerce workflow](../../examples/original-commerce-local/README.md) describe
additional typed, relationship and numeric query paths with their qualified
compiler profiles. Support depends on the selected source/model, original compiler
artifacts and runtime evidence. Source fixture IDs are not Truss-issued acceptance
IDs, and a local feed checkpoint does not establish a protected production ACK.

Readability depends on the observed predictive-optimization and retention
configuration. The reader refuses unavailable or expired snapshots instead of
falling back to current heads; it does not disable predictive optimization.
Each support claim must name its source/model profile, native versions, subset
and evidence. Local simulated tests prove component behavior only.

See the [execution plan](../../docs/helix/04-build/end-to-end-plan.md) for active
work and the [contracts](../../docs/helix/02-design/contracts/) for required
interfaces. The [pre-cleanup component documentation](../../docs/helix/04-build/evidence/documentation-history-20261009/src/ashlar/README.original.txt)
preserves historical implementation notes verbatim.
