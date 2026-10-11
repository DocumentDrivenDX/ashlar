---
ddx:
  id: ashlar.commerce-implicit-arithmetic-cli
  type: runbook
  activity: deploy
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-004
    kind: informed_by
---

# Original commerce query command

## Service summary

`query-commerce-implicit-arithmetic` executes the four unchanged commerce pack queries through the explicitly selected Weft 0.4.2 compiler and a held original publication. The operator owns the existing local installation, supplied ordinary PostgreSQL sessions and explicit runtime paths. Final results require complete source, ACK, publication, integrity and closing checks.

## Operator entry points

Use `ashlar query-commerce-implicit-arithmetic --help` for the complete required arguments. Supply absolute `--index`, `--installation`, `--publication`, `--output`, `--jars`, `--model`, `--graph` and `--pack` paths. Output must be unused. Select the independently indexed implicit-arithmetic package; a count-star installation cannot substitute for it.

Supply the explicit producer settings `--umf-source`, `--bun`, `--git`, `--producer-timeout-seconds`, `--producer-maximum-output-bytes` and `--producer-maximum-receipt-bytes`. Select finite `--maximum-artifact-bytes`, `--maximum-rows`, `--maximum-cell-bytes` and `--maximum-total-cell-bytes` bounds appropriate to the complete four-query result.

The nonsecret local profile settings `--postgres-container`, `--postgres-host`, `--postgres-port` and `--postgres-database` must describe the original installation. Session authentication comes from the explicitly supplied provider, rather than container credential discovery.

Pass `--session-provider /absolute/operator.py --session-provider-sha256 <trusted-sha256>`. The provider exports:

```python
def ordinary_query_connection_factory(config):
    from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
    return OrdinaryQueryConnectionFactory(open_ordinary_connection)
```

`open_ordinary_connection(role)` returns a fresh DB-API connection authenticated directly as that requested ordinary role. The reader owns rollback and closure. The provider owns authentication inputs and imports; it must avoid printing credentials or source payloads. Provider Python code is trusted operator code, not a sandbox. The SHA identifies captured code bytes and grants no source, ACK or publication authority.

## Dependencies and failure boundaries

Use the qualified local Spark/Delta runtime, existing publication journal, original source/model/graph and the current ordinary protected ACK service. Fresh public producer correspondence must match the publication. The command does not advance source progress. A prior report or a compiler hash cannot establish current native authority.

## Alert triage

On `ashlar-host: refused`, first check absolute paths, the exact provider pin, original pack bytes and finite bounds. For ACK or native drift, preserve the original publication and ask its owner to establish current role/source/retention custody. Do not replace original bindings, compiler SQL or checkpoint values to force a pass.

## Common incident procedures

If the process fails or is cancelled, retain its bounded diagnostics and output directory. Treat intermediate case results as provisional. A successful report requires reader closure, Spark shutdown and closing source/JAR/installation/invocation checks. A report from another run does not establish that this attempt passed.

## Rollback and recovery

This is a query-only command. After a failed attempt, preserve its evidence and use a fresh output directory for a new invocation once the underlying input or authority failure is resolved. Reuse the same immutable publication only while its versions and retention remain readable. Never repair or rewrite native data as a query recovery action.

## Escalation and communications

The operator resolves provider authentication and runtime configuration; the source owner resolves ACK and source custody; the publication owner resolves native version or retention failures. Send others only a bounded, redacted description of the failed phase and retained receipt location.

## Diagnostic retrieval

An explicit `--diagnostics-config /absolute/diagnostics.json` selects the existing bounded diagnostic workflow. Inspect the selected run manifest and failed phase before widening retrieval. Capture loss or incomplete coverage cannot prove that a guard succeeded. Keep credentials and raw source values out of diagnostic messages.
