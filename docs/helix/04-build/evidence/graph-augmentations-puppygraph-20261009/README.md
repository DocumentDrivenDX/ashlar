# Authored shared fixtures on native PuppyGraph

Both actual Cypher and Gremlin pass the shared source-profile fixture: all 26 complete original node Value/state carriers and six independent edge carriers, exact named native identities/incidences, 16 selected scalar token queries and five valid presence state/value/token queries, and the prescribed parallel/cycle/self-loop topology (A1 two one-hop occurrences, eight two-hop walks, four distinct destinations, A0 isolate). Expected bags are independently derived from original source edges; copied expected arrays alone do not qualify query behavior.

The separately authored core0.8 model and finite source are unchanged from the public c7 validated GraphFrames fixture. The adapter and each native checker recompute its original public dataset receipt byte-identically before use. Field columns derive from the exact ordered authored member declarations and preserve availability, original Value JSON and original lexical token. Native fields are explicitly STRING carriers; logical scalar type remains in the original public declarations/receipts. Exact Unicode, boolean spelling, integer tokens beyond 2^53, decimal18/scale2 tokens and signed-zero token are preserved. No native typed scalar promotion, canonical Ashlar publication, source ACK, production authority, Unity Catalog or ongoing source retention is claimed.

The new sealed `augmentations.duckdb` is independent of original Commerce/R1/R2 databases. The complete prior model plus AuthoredNode/AuthoredEdge addition was uploaded once to the existing cached dedicated PuppyGraph1.13.0 container (2 CPU/3GiB, loopback only). Exact before/request/after node/edge inventories and catalog-omission custody are retained. `/schemajson` omits catalogs; complete prior Commerce Cypher/Gremlin and R1/R2 native replays pass after addition, and all three sealed database hashes match. This is observed local schema addition, not atomic activation or rollback qualification. The container was stopped after checks.

Reproduction uses existing dependencies, a fresh preparation directory and explicit public producer/source paths:

```sh
export PYTHONPATH=src:tools:/private/tmp/ashlar-gremlin-python
/private/tmp/ashlar-db-client/bin/python tools/prepare_graph_augmentations_puppygraph.py \
 --model examples/graph-augmentations/model.umf.json \
 --source examples/graph-augmentations/source.json \
 --receipt docs/helix/04-build/evidence/graph-augmentations-graphframes-20261009/public-receipt.json \
 --umf /private/tmp/ashlar-umf-dataset-45473 \
 --output /private/tmp/ashlar-shared-puppy-FRESH
python3 -m unittest discover -s tests -p test_graph_augmentations_puppygraph.py
```

Each query checker (`check_graph_augmentations_cypher.py`, `check_graph_augmentations_gremlin.py`) requires those same model/source/receipt/UMF parameters, a fresh `--output`, and explicit `--bolt bolt://127.0.0.1:17887` or `--gremlin ws://127.0.0.1:18182/gremlin`. Credentials come from `ASHLAR_PUPPY_USER` and `ASHLAR_PUPPY_PASSWORD`. Activation requires the complete retained prior commerce merged request plus the new adapter model; it must not be replayed after Authored labels already exist. No original source-profile table/database was overwritten.

Actual host sealed database: `/private/tmp/ashlar-shared-puppy-prepared-20261009-b/augmentations.duckdb`. Actual native reports: `/private/tmp/ashlar-shared-puppy-{cypher,gremlin}-20261009-a.json`. No shared Databricks endpoint, source/Delta write or paid/cloud compute was used. Graph-native exact financial scenario arithmetic remains a separate unfinished task.
