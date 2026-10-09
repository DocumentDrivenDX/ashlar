# Pinned UMF domain-pack corpus

This is the source-custody starting point for Ashlar's independent domain work.
The finite execution cases in `corpus.json` are **unexecuted**. Required cases
remain required when an engine or credentials are unavailable.

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
