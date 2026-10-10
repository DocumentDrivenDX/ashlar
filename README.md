# ashlar

A standard graph data model for Databricks.

Ashlar starts with a shared structure for graph nodes on Databricks. Its name
comes from precisely shaped stones that fit together into a larger structure.

**Implementation status:** Development toolkit with local Delta ingest,
protected PostgreSQL outbox ACK, publication-bound Weft queries, and immutable
GraphFrames/PuppyGraph projections. Publication-bound Weft execution covers all 17
original archaeology/ecology queries, including the original LEFT JOIN. See
the [query qualification table](examples/end-to-end/QUERY-GALLERY.md#qualified-original-query-subsets)
and [delivery scope](docs/helix/04-build/toolkit-delivery-plan.md) for exact subsets.
Real Truss integration, Fabric GQL and broader query support are unfinished.
Native profiles and production authority require their own qualification.

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

Use `ashlar archaeology-source` or `ashlar ecology-source` with the same arguments
and the matching original ontology and graph under `examples/domain-packs/`.
They produce 85 and 94 events respectively, retaining original field values,
nulls and relationships with explicit development bindings.

Build the original historical medical candidate with its explicit reviewed public receipt:

```sh
ashlar medical-source --ontology examples/domain-packs/medical/historical/archive/schemas/ontology.json --graph examples/domain-packs/medical/upstream/graph/fixture.json --public-admission docs/helix/04-build/evidence/medical-historical-admission-20261009/public-receipt.json --source-system medical-example --binding-profile ashlar-medical-development-bindings/0.2 --output medical-candidate
```

This requires exact original input and receipt bytes, emits 113 events and 48
development property bindings, and retains original Boolean tokens separately
from typed values. Native hosts must recompute public admission before runtime
acquisition; this command establishes no publication or ACK authority.

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
Native reads require explicit identity, permissions, schema, finite-retention
and publication pin checks. Production Truss source registration requires an
accepted catalog and a configured producer/feed.

## Naming

The owner selected Ashlar on 2026-10-03. Existing software uses include
[Ashlar-Vellum](https://ashlar.com/) and the unrelated
[ASHLAR imaging package](https://pypi.org/project/ashlar/).
Repository naming does not establish package, domain, or trademark availability.
See the [discovery input](docs/helix/00-discover/vision-input.md).
