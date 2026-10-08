# ashlar

A standard graph data model for Databricks.

Ashlar starts with a shared structure for graph nodes on Databricks. Its name
comes from precisely shaped stones that fit together into a larger structure.

**Status:** Candidate Unity Catalog Delta layout, publication resolver, durable
source/schema custody and UMF-backed selected string-record policy are implemented.
A small local workflow runs now. Protected Truss acceptance/mutations and the
composed native publication/read workflow remain unfinished.

Install the candidate toolkit locally (Python3.9+):

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
```

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

**Next action:** Wire an authenticated SQL executor and qualified policy/custody
provider into the [publication resolver](src/ashlar/README.md). The read-only native
backend rejects mismatched table identities and pinned schemas. No scale
benchmarks are scheduled.

## Naming

The owner selected Ashlar on 2026-10-03. Existing software uses include
[Ashlar-Vellum](https://ashlar.com/) and the unrelated
[ASHLAR imaging package](https://pypi.org/project/ashlar/).
Repository naming does not establish package, domain, or trademark availability.
See the [discovery input](docs/helix/00-discover/vision-input.md).
