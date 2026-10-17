# ashlar

A standard graph data model for Databricks.

Ashlar starts with a shared structure for graph nodes on Databricks. Its name
comes from precisely shaped stones that fit together into a larger structure.

**Implementation status:** Development toolkit with qualified local Delta ingest,
PostgreSQL outbox ACK, publication-bound Weft queries, immutable GraphFrames/PuppyGraph
projections, and local Truss typed graph/feed/evolution workflows. Fabric integration
and broader query support remain unfinished. See
the [query qualification table](examples/end-to-end/QUERY-GALLERY.md#qualified-original-query-subsets)
and [delivery scope](docs/helix/04-build/toolkit-delivery-plan.md) for exact subsets.
Use the [fresh original-commerce workflow](examples/original-commerce-local/README.md)
for explicit fixture setup, six-table initialization, publication and all four
unchanged Weft 0.4.2 queries. The [earlier indexed commerce guide](examples/end-to-end/COMMERCE-INDEXED-WEFT.md)
retains its separate supplier-relationship qualification.
Use the [public Truss examples](examples/truss-native/README.md) for schema
local setup, schema acceptance, graph creation/readback/replay, object/edge updates,
graph deletion, typed publication-bound queries, and same-version UMF 0.7/0.8
additive field evolution on the supported local Truss runtime. The guide identifies
the independently qualified model and runtime subsets.

Install the candidate toolkit locally (Python3.9+):

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
```

For local Delta ingest and Weft queries, follow the
[supply-chain workflow](examples/supply-chain-native/README.md).
`ashlar configure-supply-chain-native` generates the configuration consumed by
`ashlar run-supply-chain-native`.

Use [the Fabric GQL guide](examples/fabric-gql/README.md) to run
`ashlar query-fabric-gql` with an explicitly selected HTTP and graph-release
provider.
`ashlar prepare-fabric-graph-definition` generates the model mappings for
release-specific OneLake node and edge tables.
The guide includes a Spark notebook example for materializing a finite graph
release into those tables.
Existing commerce exports use the separately selected private custody-receipt
entry point described in the same guide.
`read_commerce_export` captures the release and receipt from an existing
configured commerce reader for that workflow.
Use `ashlar export-original-commerce` to save the pair from an existing
configured publication without writing a custom export provider.
For local development, `open_local_graph_export` admits the explicitly selected
retained pair for materialization without reconnecting to the original source.
Use `ashlar materialize-graph-export` to write that pair into fresh local Delta
node and edge tables with the existing Spark runtime.
Use `ashlar compare-graph-exports` to inspect changes between two saved exports
without starting an engine; the [Truss guide](examples/truss-native/README.md#compare-immutable-graph-exports)
shows the configuration and command.

Generate all eight runtime table proposals with UMF using the
[qualified DDL bundle command](docs/helix/02-design/models/ashlar-delta-runtime/README.md#generate-a-qualified-table-bundle).
It takes an explicit catalog and schema and produces a complete SQL bundle for review.

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

Prepare original UMF documents for later Truss catalog acceptance using the
[explicit package/runtime configuration](examples/truss-catalog-preparation/README.md):

```sh
ashlar prepare-truss-catalog --config /absolute/path/to/preparation.json
```

Successful preparation retains original document and configuration bytes and
complete producer result views. Catalog acceptance and feed registration require
their own configured owners.

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
I/O. Run `ashlar validate-configured-source` for fresh public UMF logical-value
checking before database setup; the [command guide](src/ashlar/README.md#read-an-existing-managed-publication)
lists all explicit producer and receipt limits.
Use the [configured local Delta workflow](examples/configured-local/README.md)
to initialize an isolated installation and publish or replay successive original
transactions with durable local checkpoints.

The distribution is `ashlar-graph-toolkit`; the import is `ashlar`. It has no
runtime dependencies. `commerce-source` emits complete original transaction
bytes; `inspect-source` verifies source custody. These source commands do not
publish or acknowledge data. The installed `publish-commerce` and `query-commerce` commands
use explicit host configuration; native SDKs and credentials belong to that host
composition.

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

`ashlar prepare-fabric-notebook` generates a private runnable notebook from your
selected export pair; see the [Fabric notebook guide](examples/fabric-gql/README.md#generate-a-private-fabric-notebook).

Run a fresh local Truss → Delta → Weft workflow with [the portable Truss example](examples/truss-native/README.md#one-local-end-to-end-command).

Run selected existing-publication graph reads with [the supply-chain common query command](examples/supply-chain-common-query.md).
