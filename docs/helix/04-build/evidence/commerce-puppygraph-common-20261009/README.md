# Named original commerce PuppyGraph Cypher

Seven common commerce cases pass on the existing dedicated local PuppyGraph 1.13.0 ARM64 container (2 CPU, 3 GiB): full 11 node / 10 independent edge carriers, complete one-hop / two-hop bags, occurrence and distinct-destination groups, full-row singleton, and original fulfillments total2 → ordered limit1. Every native edge incident vertex and named native element identity is compared to its original immutable release identity. Exact props/retained JSON and null carriers remain text; no scalar promotion is claimed. Commerce Gremlin common cases, four authored scenarios and shared scalar/presence/topology fixtures remain outstanding.

Preparation consumes the explicit separately trusted commerce release/custody pair. Original source model/graph/development bindings are read from the admitted unchanged manifest and independently reconstruct the full canonical carrier oracle. `model.json` exposes one named CommerceNode/CommerceEdge pair; no fabricated R1/R2 commerce publications. The existing unrelated R1/R2 labels/catalog were retained by merging the complete prior model with the addition. Schema upload is not atomic graph activation. Native `/schemajson` omits catalogs, so before/after receipts prove node/edge inventory, while unchanged sealed databases and a subsequent complete 22-case R1/R2 Cypher/Gremlin replay prove their observed catalog/database correspondence. No pre-upload new replay occurred. The visibility/provenance guard added after upload is qualified as read-only replay, not a historical executed guard.

`predicate-observation.json` records that exact original-key parameter/literal predicates returned no rows despite matching stored keys. Successful singleton uses ordered original keys with native limit1; filtering uses the separately admitted original type ID. No general literal/parameter query capability is claimed.

Reproduction uses existing Python dependencies and the same dedicated local cached container only:

```sh
export PYTHONPATH=src:tools
/private/tmp/ashlar-db-client/bin/python tools/prepare_commerce_puppygraph.py \
 --release docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json \
 --custody docs/helix/04-build/evidence/commerce-graph-export-20261009/custody.json \
 --release-sha256 eeef228c5fa69720ac082dc3e19adbd63cebe17d1cdf877af250c8a9908aaed6 \
 --custody-sha256 667f5c8260fe6a37c1838495434c08aaa5ee1331aad507b8a65bac6167c731f8 \
 --output /private/tmp/ashlar-commerce-puppy-FRESH
/private/tmp/ashlar-db-client/bin/python -m unittest discover -s tests -p test_commerce_puppygraph.py
```

The query checker requires explicit `--release`, `--custody`, both trusted digests, `--bolt bolt://127.0.0.1:17887`, a fresh `--output`, and credentials in `ASHLAR_PUPPY_USER` / `ASHLAR_PUPPY_PASSWORD`. The activation helper requires complete prior/addition model paths and exactly the dedicated loopback HTTP endpoint, retains request/before/after receipts, and refuses label/catalog collisions or prior inventory mismatch. It is not rerun after the addition already exists. Native query and prior parity outputs are retained; the container was stopped after verification. No source/Delta writes, shared Databricks endpoint or cloud compute was used. Immutable consumer rematerialization does not imply continued native source retention or production authority.
