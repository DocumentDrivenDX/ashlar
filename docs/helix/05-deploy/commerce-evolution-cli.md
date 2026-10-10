---
ddx:
  id: ashlar.commerce-evolution-cli
  type: runbook
  activity: deploy
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-001
    kind: informed_by
  - id: ADR-001
    kind: informed_by
---

# Commerce evolution commands

Run the original alternating eight-publication schedule against an already authorized, empty installation:

```sh
ashlar publish-commerce-evolution --configuration /absolute/fresh.json \
  --provider /absolute/operator.py --provider-sha256 <trusted-provider-sha256>
```

Resume the retained original run in a fresh process:

```sh
ashlar resume-commerce-evolution --configuration /absolute/resume.json \
  --provider /absolute/operator.py --provider-sha256 <trusted-provider-sha256>
```

Configuration uses profile `ashlar-commerce-evolution-invocation/0.1`, exact `mode` (`fresh` or `resume`), absolute `ledger_path` and unused `receipt_path`, and `producer`. Producer fields are absolute `source`, `bun`, `git` paths and integer `timeout_seconds`, `maximum_output_bytes`, `maximum_receipt_bytes` bounds. Supply explicit selected public UMF producer resources.

Fresh configuration includes `request`: two distinct `source_order` identifiers, `stream`, `predecessor`, two four-element `clocks` arrays, eight distinct `publication_ids`, and eight canonical microsecond-decimal `recorded_at` strings. Clocks use UTC `YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00`. Original values determine the schedule and remain fixed during recovery. Resume contains `expected_sha256` instead of `request`: the independently retained original run digest. Resume neither creates an installation nor substitutes operations for an uncertain original submission.

The provider is explicitly trusted Python code executed from admitted file bytes. Its SHA-256 pin proves operator-selected code correspondence and grants no source, publication or recovery authority. The extension owns its imports and ordinary supplied session credentials. It must avoid printing credentials, raw source payloads or unbounded diagnostics.

Use [`examples/existing-evolution-operator.py`](../../../examples/existing-evolution-operator.py) as the trusted provider for an existing local Spark/Delta installation. It composes `ExistingEvolutionOperator`, the owned POSIX writer lease, ordinary registered source and protected ACK sessions, and `NativeEvolutionRunPolicy`. Run it in a fresh dedicated process with your selected installed Spark, Delta jars, and psycopg runtime. It uses existing Delta paths; it does not recreate catalog tables or source roles.

Supply these operator inputs explicitly; DSNs authenticate the configured ordinary roles directly:

| Environment input | Required value |
| --- | --- |
| `ASHLAR_INSTALLATION_ID`, `ASHLAR_NATIVE_JOURNAL` | Existing installation identity and absolute original native SQLite journal |
| `ASHLAR_ORIGINAL_RUN_RESERVATION` | Absolute durable original-run file in the journal's existing private parent; absent for fresh, required for resume |
| `ASHLAR_EXISTING_OPERATION_CAPACITY` | `uncapped` or `8MiB`, matching the original installation reservation exactly |
| `ASHLAR_DELTA_JARS` | Explicit existing local Delta jar paths, comma-separated; no package download |
| `ASHLAR_<ROLE>_TABLE`, `_PATH`, `_UUID` | Existing three-part table identity, absolute Delta path and native UUID for each of `ATTEMPTS`, `MANIFEST`, `OBJECT_CURRENT`, `EDGE_CURRENT`, `TOMBSTONE`, `WHOLE_SOURCE_HISTORY` |
| `ASHLAR_A_*`, `ASHLAR_B_*` | For each source: `SERVICE_SCHEMA`, `SCOPE_ID`, `ACK_INSTALLATION_ID`, `CONSUMER`, `FEED`, `EPOCH`, `SOURCE_SCHEMA`, `SOURCE_SIGNATURE_SHA256`, `SOURCE_ROLE`, `ACK_ROLE`, `DATABASE`, `SOURCE_DSN`, `ACK_DSN` |

The invocation ledger must share the existing native journal's private parent. Preserve its original run reservation and all journal files across processes. Fresh requires an already authorized empty installation, no previous run reservation and no ledger. Resume requires the independently selected original digest, complete retained reservation and ledger, and current native/source/ACK lineage. Missing custody refuses; selecting fresh cannot repair it. The local lease excludes cooperating operators; native closing readback remains mandatory for other writers. The native registry uses query-only SQL on an existing WAL connection, which may maintain SQLite sidecars. The run ledger uses its original DELETE-journal read-only inspection profile.

Custom providers implement `open_evolution(invocation)` yielding the matching configuration and preserve the same authority and ownership boundaries. A code pin proves correspondence rather than permission. Installation initialization, role creation, grants and credential discovery belong outside these commands.

The command validates closed bounded configuration before loading code, renews configuration/provider byte custody after execution and cleanup, and writes an exclusive receipt containing only the original run digest and eight publication identifiers. Closing admission or receipt failure can follow durable publication: recover using retained original custody rather than treating reporting failure as rollback. The receipt is a report, not an authority token.
