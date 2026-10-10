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

Implement `open_evolution(invocation)` as a context manager yielding the matching `FreshCommerceEvolutionConfig` or `ResumeCommerceEvolutionConfig`. Supply the existing `NativeDriver`, `RegisteredOutboxSources`, `RegisteredOutboxAcks`, `NativeEvolutionRunPolicy`, and independently owned current original reservation authority. Match the invocation's producer, ledger and original request or digest exactly. The provider owns runtime/session cleanup and closing authority renewal. Use public driver and ordinary supplied source/ACK session boundaries. Installation initialization, role creation, grants and credential discovery belong outside these commands.

The command validates closed bounded configuration before loading code, renews configuration/provider byte custody after execution and cleanup, and writes an exclusive receipt containing only the original run digest and eight publication identifiers. Closing admission or receipt failure can follow durable publication: recover using retained original custody rather than treating reporting failure as rollback. The receipt is a report, not an authority token.
