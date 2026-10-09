# ashlar

A standard graph data model for Databricks.

Ashlar starts with a shared structure for graph nodes on Databricks. Its name
comes from precisely shaped stones that fit together into a larger structure.

**Status:** Development toolkit with Unity Catalog managed Delta, immutable
publication resolution, UMF-backed selected string-Record bindings and native
singleton reads. Private CSV/JSONL and schema-evolution publication/read workflows
have scoped native evidence; a Weft string/presence query also runs. Real Truss
acceptance/producer/feed and production source fencing remain unfinished.

Install the candidate toolkit locally (Python3.9+):

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
```

Build a candidate transaction from the original commerce domain-pack inputs:

```sh
ashlar commerce-source --ontology examples/domain-packs/commerce/upstream/ontology.json --graph examples/domain-packs/commerce/upstream/graph/fixture.json --source-system commerce-example --output commerce-candidate
```

The fresh output contains `source.jsonl` and explicit development bindings for
11 objects, 10 edges and 34 qualified fields. Exact original input hashes are
required. Public UMF validation and host publication remain separate steps.

Inspect an explicitly configured source with the installed command:

```sh
ashlar inspect-configured-source examples/end-to-end/configured-source.json
```

This checks pinned inputs, explicit bindings, transaction staging, local
create/update/delete/history and unchanged exact replay. It performs no database
I/O; actual upstream UMF logical-value checking remains the separate host preflight.

The distribution is `ashlar-graph-toolkit`; the import is `ashlar`. It has no
runtime dependencies. The installed CLI verifies source custody and emits complete
original transaction bytes; it does not publish or acknowledge data. Native SDKs
and credentials belong to the separate host tools.

Run the local example (Python standard library only):

```sh
python3 tools/run_local_example.py
```

It reads retained UMF intake and interpretation, validates a bounded JSONL source,
applies create/update/delete transactions and checks exact replay. The output
contains one live object, four history entries and one tombstone. IDs are explicit
fixture mappings; it does not establish Truss acceptance or Delta publication.

Start with the [project documentation](docs/helix/README.md) and
[product vision](docs/helix/00-discover/product-vision.md).

## Working with HELIX

This repository uses [HELIX](https://github.com/DocumentDrivenDX/helix).
Read `AGENTS.md` and `.helix.yml`, then invoke the installed `helix` skill.
The bootstrap used HELIX 0.14.1. Resolve its graph, templates, and prompts from
the installed plugin; the methodology catalog is not vendored here.

Continue with the [runnable setup/source/query workflow](examples/end-to-end/README.md).
The active goal still requires the real Truss catalog/producer/feed and completion
of the caller-configured native source path. Native reads retain explicit identity,
permissions, schema, finite-retention and pin checks. No scale benchmarks are scheduled.

## Naming

The owner selected Ashlar on 2026-10-03. Existing software uses include
[Ashlar-Vellum](https://ashlar.com/) and the unrelated
[ASHLAR imaging package](https://pypi.org/project/ashlar/).
Repository naming does not establish package, domain, or trademark availability.
See the [discovery input](docs/helix/00-discover/vision-input.md).
