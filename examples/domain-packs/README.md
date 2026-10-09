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

Two observed custody gaps remain: the legal and medical graph fixtures' declared
pack SHA-256 differs from their pinned `pack.json`; their ontology SHA-256 matches.
The inventory reports both mismatches. Neither source is silently repinned or
accepted for execution. Medical's required stage remains outstanding and must
resolve its binding provenance, preserve source notices, and avoid clinical
inference. Commerce's graph pack and ontology digests both match.

The original commerce scenario expectations use template IDs, whereas the graph
fixture is seeded independent-component replay with tuple IDs. Independent query
oracles must account for this distinction. Additional graph controls and lifecycle
mutations belong to a separate explicit augmentation, preserving the originals.
