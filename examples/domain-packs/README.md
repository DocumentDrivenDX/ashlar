# Pinned UMF domain-pack corpus

For the runnable archaeology/ecology publication and query workflow, start with
the [finite-pack quickstart](QUICKSTART.md). The inventory below establishes
original source custody; runtime support requires the separately qualified routes.

This is the source-custody starting point for Ashlar's independent domain work.
`corpus.json` defines the finite cases; their runtime qualification belongs in
execution evidence. Required cases remain required when an engine or credentials
are unavailable.

`inventory.json` describes all 24 canonical packs and all 595 files below
`spec/domain-packs` in UMF commit
`1f7b5f5d2a355c4b476e3a96b289b9048f03f567`, from
[DocumentDrivenDX/umf](https://github.com/DocumentDrivenDX/umf/tree/1f7b5f5d2a355c4b476e3a96b289b9048f03f567/spec/domain-packs).
Each file has its Git blob identifier, raw byte length and SHA-256. CSV counts,
source declarations and availability, schema declarations, relationship descriptors,
graph endpoint/key counts, original notices and declared digest correspondence
are observations of those objects. They are not a replacement UMF validator.
Opaque and future files remain in the inventory; no generator or source URL runs.

The `commerce/upstream/` directory is an exact, complete byte copy of the pinned
commerce pack, including its source metadata, ontology, graph candidate, CSV
fixtures, TableSpec schemas and original README. Ashlar commentary belongs outside
that directory. Those original files qualify themselves as authored synthetic
fixtures, not production samples or native-standard conformance. Other pack assets
are inventoried without being copied here; there is no unnecessary medical asset
bundle in Ashlar.

From the Ashlar root, regenerate and verify using any local UMF Git repository
that contains the exact commit (the working branch and dirty files are ignored):

```sh
python3 -B tools/domain_pack_inventory.py \
  --umf-repo /path/to/umf \
  --output examples/domain-packs/inventory.json
python3 -B tools/domain_pack_inventory.py \
  --umf-repo /path/to/umf \
  --output examples/domain-packs/inventory.json \
  --check --commerce-copy examples/domain-packs/commerce/upstream
PYTHONPATH=src:tools:tests python3 -B -m unittest test_domain_pack_inventory
```

Generation reads only pinned Git blobs. `--check` requires byte-identical generated
inventory and `--commerce-copy` requires exactly the original path set and bytes;
extra, missing, changed or symlinked copy files refuse. No checkout/reset occurs.
For a new copy, extract the pinned commerce path with `git archive` into a fresh
staging directory and preserve the complete pack subtree; verify it with the
command above before use. Do not rewrite or reserialize source JSON.

Execution order is commerce, supply-chain, archaeology, ecology, then bounded
medical original-byte/reference admission and allowed data qualification. The
inventory includes other synthetic packs, medical family packs and external
public-dataset profiles. All packs still require explicit classification; no
supported-engine claim follows from their inventory presence.

The ontology documents use UMF 0.8. Graph fixture values retain CSV lexical text,
not canonical UMF Values or native storage IDs. Their exact source-qualified tuple
keys need an explicit injective storage binding. Existing Ashlar UMF 0.7 evidence
cannot qualify these 0.8 ontologies. Use UMF-owned validation/interpretation and
retain unknown extension bytes before executable admission.

Graph candidates must retain the pack revision that created them. Legal and
medical candidates reference historical 1.0 packs; current pack metadata is 1.1.
Their original source-pack bytes are now retained and verified by UMF's
[graph provenance audit](../../docs/helix/04-build/evidence/umf-graph-provenance-20261009.json)
at commit `45473e71d5dfe9aa80abe3e346243b8efcbf1a37`. It distinguishes 14 current
candidates, two historical candidates and eight packs without candidate datasets.
The original pinned inventory remains unchanged: its mismatch observations refer
to current metadata, not missing historical provenance. Historical candidates
must not be repinned to current models or treated as current-model execution.
Medical admission must preserve source notices and avoid clinical inference.

The original commerce scenario expectations use template IDs, whereas the graph
fixture is seeded independent-component replay with tuple IDs. Independent query
oracles must account for this distinction. Additional graph controls and lifecycle
mutations belong to a separate explicit augmentation, preserving the originals.

## Explicit Boolean lexical admission

The common pack dataset tool accepts an optional Boolean profile:

```sh
bun tools/check_domain_pack_datasets.ts /path/to/clean/umf-e44cd15f \
  /path/to/pinned/pack-repo /absolute/fresh-datasets.json \
  --boolean-lexical-profile umf.csv-boolean-lexical/1.0.0
```

Use the exact producer revision `e44cd15f336dfb33db35acf20eee13dd120a1a28`
and pack revision selected by the tool. The public UMF validator and verifier
own Boolean conversion; the output retains each original token, field and source
context in a verified receipt. Omitted and explicit null values keep their
original states. Without this option, present Boolean lexical carriers refuse.
Original model/graph provenance checks still apply. This operation admits only
finite supplied dataset values; native source, storage and query execution
require their own qualification.

## Original commerce dataset admission

Use the public UMF finite-dataset operation to admit the original 0.8 ontology
and candidate lexical values without changing their declared domains:

```sh
bun tools/check_commerce_dataset.ts /path/to/clean/umf-c7c95e1c \
  /tmp/ashlar-commerce-dataset-new.json
python3 -m unittest discover -s tests -p test_commerce_public_dataset.py
```

The consumer requires exact UMF revision
`c7c95e1c4ea5b72541f47fa0350ca467ff02f395` and original model/graph hashes.
Keep Record receipts and their context warnings separate from the supplied
dataset's key and relationship results. This operation checks the explicitly
supplied finite dataset; source authority, native identities, publication, query
execution and acknowledgment require their own admission. The output preserves
the original source and public receipts, resolved endpoints and three separately
authored negative controls. The Python check verifies retained checked-in evidence;
regenerate with the Bun command to exercise the public API.

## Commerce source transaction

Create a complete candidate source transaction from the unchanged commerce graph:

```sh
PYTHONPATH=src:tools python3 tools/commerce_source_transaction.py \
  --source-system commerce-fixture --output /tmp/ashlar-commerce-source-new
```

Use a fresh development installation: every event explicitly creates its entity.
The fresh directory contains `source.jsonl` and `bindings.json`. The transaction
preserves all 11 objects, 10 edge occurrences, original endpoints and lexical
numeric values. The binding file assigns deterministic development IDs; they are
not accepted Truss catalog identities. Retained payloads preserve each original
row and the model/graph hashes. Run the public dataset admission above separately;
a complete source transaction still requires trusted source authority, schema
binding, writer fencing and publication admission before ingestion or ACK.

## Every pinned pack model

Run the public document validator over every original ontology in the inventory:

```sh
bun tools/check_domain_pack_models.ts /path/to/clean/umf-c7c95e1c \
  /path/to/umf-containing-1f7b5f5 /tmp/ashlar-pack-model-results-new.json
```

The consumer reads exact pinned Git objects, checks their inventory hashes, and
retains the original model bytes with each public result. Packs without a declared
UMF ontology get an explicit absence result. Document validation establishes
model semantics only; Record values, finite datasets, storage bindings, ingestion
and engine queries require separate admission and execution.

### Run a selected authored query

Use the existing native query configuration with `--case` to run only selected
original scenarios against an existing publication:

```sh
ashlar query-finite-pack --configuration /absolute/private/query.json \
  --case missing-media
```

Repeat `--case` for a small selection. Names must belong to the chosen pack, and
duplicates are refused before opening resources. The original pack, model and
SQL remain unchanged; selected queries retain the same publication/source
checks. The report records the selection. Omitting `--case` runs the full authored
corpus, as before. Archaeology includes `missing-media`; ecology includes
`network`. A selected result establishes only those cases.

### Export a published finite pack

Use an existing archaeology or ecology native query configuration to export its
publication without compiling or executing queries:

```sh
ashlar export-original-finite-pack \
  --configuration /absolute/private/query.json \
  --output /absolute/private/fresh-export
```

The command opens the existing publication and original finite-file source,
checks the complete graph against the source oracle under the full publication
vector, and closes its source, transport and Spark owners before exposing export
files. It writes `release.graph.json` and `custody.json` with separate SHA256
values. The custody uses an explicit finite-file profile; it does not claim a
PostgreSQL ACK. Retain and select both digests when using the files with
`materialize-graph-export`, `prepare-fabric-notebook` or an offline graph reader.
Export does not deploy a graph model or execute an engine query.

Fabric notebook preparation preserves the selected graph and custody bytes,
records their independent digests and named custody profile, and pins a toolkit
revision that can read that profile. Use release-specific OneLake node and edge
paths in the intended workspace/lakehouse. The generated notebook contains the
export data; keep it private until choosing its upload destination. Preparation
performs no upload, materialization or GQL query.

An export-only configuration avoids compiler settings:

```json
{
  "profile": "ashlar-original-finite-pack-export-configuration/0.1",
  "pack": "archaeology",
  "publication": "/absolute/private/existing-publication",
  "jars": "/absolute/path/to/existing-delta-jars",
  "model": "/absolute/path/to/original/ontology.json",
  "graph": "/absolute/path/to/original/graph/fixture.json",
  "pack_file": "/absolute/path/to/original/pack.json",
  "producer": {
    "source": "/absolute/path/to/selected-umf-source",
    "bun": "/absolute/path/to/bun",
    "git": "/absolute/path/to/git",
    "timeout_seconds": 5,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 33554432
  },
  "operation_capacity": {
    "profile": "ashlar-private-local-operation-capacity/0.1",
    "max_intent_bytes": 8388608
  }
}
```

Select the original source and operation capacity used by the existing
publication. The command neither initializes nor repairs that publication.
Existing query configurations remain accepted for compatibility.
