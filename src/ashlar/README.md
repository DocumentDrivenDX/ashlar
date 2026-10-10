# Ashlar toolkit components

Ashlar's Python3.9+ standard-library core supplies admitted schema intake,
whole-entity graph application, immutable publication resolution and guarded
query orchestration. Authenticated database transports and deployment tools
remain separate from the portable library. Start with the
[runnable example](../../examples/end-to-end/README.md) for setup and native commands.

## Install and inspect a source

From the repository root:

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
ashlar inspect-configured-source examples/end-to-end/configured-source.json
PYTHONPATH=src:tools python3 -B -m unittest discover -s tests
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

## Supported development evidence

The configured JSONL example has actual native
[publication/update/delete/history](../../docs/helix/04-build/evidence/native-configured-publication-complete-20261008.json),
[singleton](../../docs/helix/04-build/evidence/native-configured-singleton-20261008.json),
[guarded Weft query](../../docs/helix/04-build/evidence/native-configured-guarded-weft-query-20261008.json)
and [committed replay](../../docs/helix/04-build/evidence/native-configured-replay-20261008.json)
proof. Weft's qualified result subset is required strings and optional string
presence; numeric, relationship, join, traversal and aggregate support require
additional binding, decoder and native evidence. Source fixture IDs are not
Truss-issued acceptance IDs. Committed replay proof does not establish
missing-journal recovery or remote Truss ACK.

Readability depends on the observed predictive-optimization and retention
configuration. The reader refuses unavailable or expired snapshots instead of
falling back to current heads; it does not disable predictive optimization.
Each support claim must name its source/model profile, native versions, subset
and evidence. Local simulated tests prove component behavior only.

See the [execution plan](../../docs/helix/04-build/end-to-end-plan.md) for active
work and the [contracts](../../docs/helix/02-design/contracts/) for required
interfaces. The [pre-cleanup component documentation](../../docs/helix/04-build/evidence/documentation-history-20261009/src/ashlar/README.original.txt)
preserves historical implementation notes verbatim.
