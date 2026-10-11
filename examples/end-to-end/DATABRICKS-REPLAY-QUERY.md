# Replay and query an existing Databricks publication

This guide exposes the existing configured-JSONL development path. It does not create compute, install a schema, ingest a new batch or repair missing journals. Use an explicitly authorized existing dedicated Ashlar SQL endpoint; shared/default compute and new paid capacity are excluded. No Databricks command was run to verify this documentation. P09's fresh documented-command execution gate remains open.

## Select the original installation

Copy [selection.template.sh](../databricks-replay-query/selection.template.sh) to an operator-owned file and replace every value. Run from the Ashlar checkout with a separately qualified host Python/Databricks SDK environment. The profile names an explicitly authenticated operator configuration; keep credentials out of this file and retained output. The profile and warehouse must identify the same authorized dedicated endpoint used by the original installation.

Preserve the original authenticated authority and warehouse identity as well as the original bytes. Durable SQL requests and effect plans bind that identity into their exact hashes. An historical journal created against another or shared endpoint cannot be migrated by changing `--profile` or `--warehouse`; this guide does not authorize replay there, endpoint substitution or regenerated plans. If the original endpoint is no longer admissible under the dedicated-compute constraint, retain the journal and report that dependency explicitly.

`ASHLAR_INSTALLATION` is the retained `tools/setup_native.py` summary for the actual catalog/schema/table UUIDs. Preserve that setup's original durable SQL journal separately. `ASHLAR_INTAKE_PROOF` is the retained `tools/register_umf.py` summary for the exact original model revision and public UMF validation. Preserve its original intake journal too. Those setup/intake outputs are prerequisites, not replaceable examples or a grant to recreate resources. Neither is produced by the commands below.

`ASHLAR_PUBLICATION_JOURNAL` is the original `tools/run_native_source_stream.py` SQLite journal, including required WAL state, native attempt handles, submission/checkpoint bytes and completed descriptors. Keep the same original source configuration, source bytes, model bytes, explicit type/property/identity bindings, feed/epoch and UMF checkout. Select the last completed ordinal explicitly (1–4). The committed [configured-source example](configured-source.json) illustrates the configuration shape; substituting it for another run's original configuration cannot recover that run. Do not overwrite, reset, recreate or hand-edit an original journal.

The Weft query additionally selects a clean admitted source checkout and its verified qualified wheel installation. The historical configured String/presence profile used Weft `2744531735c2a771fbe7ed24a7f67e3afc851b25`; another revision/profile requires its own admission. It is separate from the local indexed Weft 0.4.2 commerce workflow. Current source/model permissions, UUIDs, complete publication vector, native runtime/profile, finite retention and pins must still be admitted; historical receipts do not renew them.

## Run separate replay and query processes

Source your completed operator selection file explicitly. Reject unfilled placeholders before invoking the owner:

```sh
. /absolute/path/to/operator-selection.sh
for selected in "$ASHLAR_PYTHON" "$ASHLAR_DATABRICKS_PROFILE" "$ASHLAR_DATABRICKS_WAREHOUSE" "$ASHLAR_INSTALLATION" "$ASHLAR_INTAKE_PROOF" "$ASHLAR_PUBLICATION_JOURNAL" "$ASHLAR_SOURCE_CONFIG" "$ASHLAR_LAST_ORDINAL" "$ASHLAR_UMF_SOURCE" "$ASHLAR_WEFT_SOURCE" "$ASHLAR_WEFT_PYTHON" "$ASHLAR_REPLAY_OUTPUT" "$ASHLAR_QUERY_OUTPUT"; do
  case "$selected" in ''|*REPLACE_WITH*) printf '%s\n' 'Explicit operator selection required' >&2; exit 1;; esac
done
```

Replay only the last committed original batch. Wait for terminal closing before running the separate query; retain all original outputs, including failures.

```sh
"$ASHLAR_PYTHON" tools/run_native_source_stream.py \
  --profile "$ASHLAR_DATABRICKS_PROFILE" --warehouse "$ASHLAR_DATABRICKS_WAREHOUSE" \
  --source configured-jsonl --source-config "$ASHLAR_SOURCE_CONFIG" \
  --limit "$ASHLAR_LAST_ORDINAL" --replay-last \
  --installation "$ASHLAR_INSTALLATION" --intake-proof "$ASHLAR_INTAKE_PROOF" \
  --journal "$ASHLAR_PUBLICATION_JOURNAL" --umf-source "$ASHLAR_UMF_SOURCE" \
  --output "$ASHLAR_REPLAY_OUTPUT"
```

The committed replay backend must observe the same original committed request and descriptor. It cannot prepare/apply/recommit a missing attempt. Current admission precedes idempotent local checkpoint acknowledgement; no new Delta submission is allowed. This is same-host development progress, not a real Truss feed or remote ACK.

```sh
"$ASHLAR_PYTHON" tools/run_native_source_stream.py \
  --profile "$ASHLAR_DATABRICKS_PROFILE" --warehouse "$ASHLAR_DATABRICKS_WAREHOUSE" \
  --source configured-jsonl --source-config "$ASHLAR_SOURCE_CONFIG" \
  --limit "$ASHLAR_LAST_ORDINAL" --query-only \
  --weft-sql 'SELECT i.label, i.caption FROM Item i' \
  --weft-source "$ASHLAR_WEFT_SOURCE" --weft-python "$ASHLAR_WEFT_PYTHON" \
  --installation "$ASHLAR_INSTALLATION" --intake-proof "$ASHLAR_INTAKE_PROOF" \
  --journal "$ASHLAR_PUBLICATION_JOURNAL" --umf-source "$ASHLAR_UMF_SOURCE" \
  --output "$ASHLAR_QUERY_OUTPUT"
```

Query-only reads the last retained publication and withholds results on compiler obligations, source/native drift, permissions, retention, pin or closing refusal. It must not select current heads, advance source progress or substitute a broader compiler profile. Both outputs must be distinct fresh directories. A failure is retained for review, not automatically retried.

## Scope and evidence

Historical [committed replay evidence](../../docs/helix/04-build/evidence/native-configured-replay-20261008.json) retains 58 unchanged original submissions, three unchanged checkpoint rows, the same descriptor and fresh native observations. Historical [guarded query evidence](../../docs/helix/04-build/evidence/native-configured-guarded-weft-query-20261008.json) retains the configured String/presence result `updated` and the value `雪`, native guards and closing custody. These qualify the recorded 2026-10-08 development tuple, not present-day endpoint access, retained-version availability, general query support or production/Truss authority.

The commands here are checked only against the current owner's parser without importing the SDK or contacting Databricks. A separately authorized bounded run must retain the exact selected command/source/dependency tuple, raw statements/results/permission and retention observations, original journal correspondence and terminal closing proof before the [P09 gate](../../docs/helix/04-build/toolkit-delivery-plan.md) can be marked complete. Preserve the full replay/query obligation; local Spark evidence does not substitute for Databricks execution. Governing boundaries: [publication](../../docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md), [consumer reads](../../docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md) and [resolver](../../docs/helix/02-design/contracts/CONTRACT-004-publication-resolver.md).
