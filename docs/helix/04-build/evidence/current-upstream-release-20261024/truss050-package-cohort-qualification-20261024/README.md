---
ddx:
  id: ashlar.evidence.truss050-package-cohort-qualification-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-adoption-analysis-20261024
      kind: informed_by
---

# Truss 0.0.50 package and cohort qualification

Status: **effect-free package resolution and import-coexistence checks passed; a new .50 selected-source packet is viable at this qualification stage.** Ashlar's [`truss-local` pin](../../../../../../pyproject.toml) now selects the official .50 wheel by URL and SHA-256. The wheel and release pins were downloaded and SHA-256 verified against the GitHub release API. Wheel `RECORD` hashes and sizes pass. A read-only `uv pip compile` resolved 22 packages for Ashlar `[truss-local]` (Truss `[local,native]`) with the exact UMF core/access, Weft, and psycopg cohort. The wheel was imported by path in the existing read-only CPython 3.12.14 runtime carrying that tuple. No installation or write was made to the frozen R12 runtime. The exact import origins, selected public methods, dependency resolution, and metadata findings are in [`package-cohort-receipt.json`](package-cohort-receipt.json).

The imported `truss`, `truss.catalog_runtime`, `truss.feed`, and `truss.migrations` modules resolved directly from the verified .50 wheel. Public feed read/reread, receipt-ACK, ACK-reconciliation, and `RuntimeMigrations` surfaces import with the pinned tuple. This establishes import/API-surface coexistence only; it does not exercise method behavior or prove lifecycle correctness.

`release-pins.json` declares supported UMF version `0.10.0`, which refers to the UMF schema/model semantics. Ashlar's Python core distribution is 0.10.1 and its adoption receipt records unchanged 0.10.0 model semantics, so this declaration does not itself block the cohort. Wheel metadata's `umf_access==0.1.0.dev5` and `weft-sql==0.4.51` requirements are conditional on Truss's optional `[typed-sdk]` and `[query]` extras. Ashlar does not select those extras: its integration owns Weft directly and the selected Truss paths use the package's base/native surface. If either extra is selected in a later packet, its exact requirement conflicts with current access dev7 or Weft 0.4.53 and must be resolved before proceeding. The selected base wheel dependencies and the `native` psycopg extra align with the current tuple.

The source selected for the packet must still be rebound to the current Ashlar commit, with its own input hashes and gates. This receipt does not transfer R12 authority and does not authorize PostgreSQL, Spark, migration, source/feed/publication/ACK, or native actions. No sdist was needed because the verified wheel contains the package sources and metadata used by this check.
