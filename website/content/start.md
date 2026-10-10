---
title: "Use Ashlar"
description: "Install the toolkit, inspect a source, and follow a bounded publication and query workflow."
eyebrow: "02 / Start here"
composition: model-primary
nextPath: schema/
nextLabel: "See how the tables fit together"
---

## Install and inspect a source

Clone [the Ashlar repository](https://github.com/DocumentDrivenDX/ashlar) and run these commands from its root with Python 3.9 or later:

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
ashlar inspect-configured-source examples/end-to-end/configured-source.json
python3 tools/run_local_example.py
```

The source inspector retains complete committed transaction bytes. The configured example uses explicit model and identity bindings to apply bounded create, update and delete changes and check replay. Inspection does not acknowledge a source or publish warehouse data.

## Publish and query a small graph

Follow the [local setup and query guide](https://github.com/DocumentDrivenDX/ashlar/blob/main/examples/end-to-end/README.md) to prepare the original commerce graph, recompute its public UMF admission and publish it to local Delta tables. The guide names the separate writer and reader runtimes, exact compiler revision and required inputs.

Choose a fresh output directory, explicit source namespace and pinned model for each installation. Retain original input and journal bytes for recovery. Weft compiles queries against the selected immutable publication; Ashlar executes its unchanged SQL and integrity checks before releasing results. Reads must retain the full table-version vector and refuse drift or unavailable snapshots.

The local PostgreSQL sandbox supplies an experimental outbox source. Real Truss catalog acceptance and feed registration require their own integration. For Databricks, select a dedicated Ashlar endpoint and an explicit catalog/schema. Preserve predictive optimization; readability follows the observed retention configuration.

## Inspect the tables and mappings

The [schema browser](../schema/) shows the UMF model, physical fields, logical keys and relationships. The [Delta SQL package](https://github.com/DocumentDrivenDX/ashlar/tree/main/sql/ashlar-delta-v03) exposes baseline and optional layouts with explicit deployment placeholders.

Delta column nullability is only part of enforcement. The publisher validates typed identities, endpoint existence and relationship rules; execution adapters enforce read policy and publication custody. Review the package's responsibility matrix before deploying a layout or assuming a declared relationship is a Delta-enforced foreign key.

Use the [ecosystem examples](../ecosystem/) to choose an engine projection with its explicit value and identity mapping. Each engine and query profile needs its own qualification; a prepared export alone does not establish query support.
