# Use Ashlar locally

Run commands from the Ashlar checkout. Select fresh output directories for new
installations; retain the original journals and inputs when recovering a run.
Use explicit source namespaces, model revisions and catalog bindings throughout.
The [query gallery](QUERY-GALLERY.md) gives exact commerce examples and the
semantic and execution boundaries needed for each query family.

## Install and inspect a source

```sh
python3 -m pip install .
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
ashlar inspect-configured-source examples/end-to-end/configured-source.json
python3 tools/run_local_example.py
```

The installed source inspector preserves complete committed transaction bytes.
The configured-source example checks explicit model/ID bindings, applies bounded
create/update/delete changes locally and verifies unchanged replay. Source custody
and local state inspection do not grant publication or source acknowledgement.

Adapt bounded UTF-8 CSV through the same installed custody boundary:

```sh
ashlar inspect-csv --feed csv-example --epoch immutable-example-1 --source-system local-example --schema-revision 3 --type-id 17 --properties-json '{"label":"23","caption":"24"}' < examples/end-to-end/string-source.csv
```

Preserve absent, null and value states separately. Feed/epoch and the original
outer cursor belong to the selected source; JSONL byte offsets cannot substitute
for a PostgreSQL outbox sequence or a Truss checkpoint.

## Install the local Weft compiler

Follow the [indexed installation guide](WEFT-INSTALLATION.md) to install the
qualified local candidate and compile an unchanged original request. Publication
execution still requires the resolved vector and all compiler host obligations.

For a complete local publication and indexed scalar/count/relationship workflow,
follow the [commerce setup and query guide](COMMERCE-INDEXED-WEFT.md).

## Prepare the original commerce graph

```sh
mkdir -p out
ashlar commerce-source --ontology examples/domain-packs/commerce/upstream/ontology.json --graph examples/domain-packs/commerce/upstream/graph/fixture.json --source-system commerce-example --output out/commerce-candidate
```

This creates `source.jsonl` and `bindings.json` in a fresh directory. The converter
requires the exact original model and graph bytes. The bindings map fully
qualified source identities to development type/property IDs, keeping original
lexical values and complete source rows. Treat those IDs as explicit fixture
configuration; accepted Truss catalog IDs require real catalog admission.

Recompute the original finite dataset through a clean pinned public UMF producer:

```sh
git clone https://github.com/DocumentDrivenDX/umf.git out/umf-commerce
git -C out/umf-commerce checkout --detach c7c95e1c4ea5b72541f47fa0350ca467ff02f395
(cd out/umf-commerce && bun install --frozen-lockfile)
bun tools/check_commerce_dataset.ts out/umf-commerce out/commerce-dataset.json
```

Keep the original public diagnostics and provenance. Individual Record context
warnings and complete supplied-dataset closure are separate results. A validator
receipt establishes its declared semantic subset; source authorization, native
writer custody and durable publication are additional host obligations.

## Publish and query a bounded local installation

Follow the [commerce setup and indexed query guide](COMMERCE-INDEXED-WEFT.md)
for the complete local workflow: explicit runtime and JAR selection, pinned public
UMF validation, private PostgreSQL readiness, fresh Delta publication, installed
compiler verification, and publication-bound queries. Run publication to terminal
cleanup before starting the separate query process. Both processes use one local
worker and a 512 MiB driver; no shared/default cloud endpoint is selected.

Queries hold the complete resolved publication vector, original source and
ordinary protected ACK through execution and closing checks. Preserve compiler
SQL and host obligations unchanged. Success requires the closing checks and
cleanup; it must not fall back to current table heads or promote an unsupported
scalar.

The local sandbox is an outbox substrate. Installing it does not install Truss
or establish catalog acceptance, production fencing or a Truss feed. Databricks
commands require an explicitly selected dedicated Ashlar endpoint; shared/default
compute is excluded. Keep predictive optimization configured and let readability
follow the observed retained snapshot configuration.

## Retained strict-compiler qualification

The earlier `tools/run_commerce_publication_weft.py` host retains its strict
compiler profile for historical qualification. Its admitted compiler source is
`f05f2df09e9c2494ac8c6d703dfe38413dbc4181`, built with
`ashlar-databricks-candidate`; newer binaries require explicit profile admission.
That profile's original count, decimal/String projection and product/supplier
join evidence remains separate from the indexed installation workflow above.
Use its retained source and receipts when reproducing that qualification; this
pin does not select the compiler for a new indexed query installation.

Use the [diagnostics guide](DIAGNOSTICS.md) to select bounded observation for
`query-commerce-paths` and inspect a closed local capture.

## Contracts and evidence

Use the [toolkit plan](../../docs/helix/04-build/toolkit-delivery-plan.md) for the
required end-to-end scope, and the [contracts](../../docs/helix/02-design/contracts/)
for source admission, tables and publication resolution. Engine projections must
preserve identity, independent edges, exact values and explicit capability losses.
Each engine needs its own actual query and release-lifecycle qualification.

The [original example notes](../../docs/helix/04-build/evidence/documentation-history-20261009/examples/end-to-end/README.original.txt)
retain previous execution history verbatim. Native receipts and their custody
hashes belong under build evidence rather than in this usage guide.
