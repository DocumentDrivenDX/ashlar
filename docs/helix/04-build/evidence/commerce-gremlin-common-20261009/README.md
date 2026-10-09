# Original commerce common Gremlin queries

Seven common cases pass on the existing dedicated local PuppyGraph 1.13.0 / Gremlin Python 3.7.3 runtime: 11 complete original node carriers, 10 independent edge carriers with exact named native identity and incidence, complete one-hop and two-hop occurrence bags, original source/relationship occurrence counts and distinct destinations, exact full-original-key singleton equality, and native original fulfillments total2 → ordered limit1. Native GraphSON string IDs are decoded by the existing exact wrapper helper; scalar/presence JSON remains canonical text without promotion.

The original-key `.has(...)` predicate returned no rows despite matching stored keys. `key-probes.json` preserves this refusal and two actual successful native equality forms. The qualified singleton uses `.filter(__.values('original_key').is(originalKey))` with the exact original key binding. It is an actual native equality lookup; ordering is not substituted. Full singleton row parity is checked independently against the original model/graph/binding oracle. No general predicate equivalence is claimed.

The run reuses the previously admitted named immutable commerce export and unchanged dedicated graph schema, with explicit separately trusted release/custody digests. No schema, source or Delta writes occurred. Both commerce and prior release DuckDB hashes remained unchanged; the container was stopped afterward. Commerce scenarios and shared scalar/presence/topology fixtures remain separate unfinished required work. No Unity Catalog, scalar promotion, production authority or continued native source retention is qualified.

Reproduce against the already prepared dedicated local engine with existing dependencies:

```sh
export PYTHONPATH=src:tools:/private/tmp/ashlar-gremlin-python
/private/tmp/ashlar-db-client/bin/python tools/check_commerce_gremlin.py \
 --release docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json \
 --custody docs/helix/04-build/evidence/commerce-graph-export-20261009/custody.json \
 --release-sha256 eeef228c5fa69720ac082dc3e19adbd63cebe17d1cdf877af250c8a9908aaed6 \
 --custody-sha256 667f5c8260fe6a37c1838495434c08aaa5ee1331aad507b8a65bac6167c731f8 \
 --gremlin ws://127.0.0.1:18182/gremlin \
 --output /private/tmp/ashlar-commerce-gremlin-FRESH.json
/private/tmp/ashlar-db-client/bin/python -m unittest discover -s tests -p test_commerce_gremlin.py
```

Credentials are required via `ASHLAR_PUPPY_USER` / `ASHLAR_PUPPY_PASSWORD`. The retained actual run is `/private/tmp/ashlar-commerce-gremlin-20261009-d.json`. Earlier startup connection refusal, exact GraphSON wrapper refusal and unsupported `.has` singleton attempts produced no success report; the final qualified run passes every mandatory check. The small projection tests cover structure only; retained actual native results provide the runtime evidence.
