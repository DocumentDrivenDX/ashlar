# Original commerce common GraphFrames queries

Executed bounded local Spark 3.5.3 / Delta 3.2.1 / GraphFrames 0.12.3 against the separately trusted immutable commerce export: 11 complete node carriers, 10 complete edge carriers, 10 one-hop occurrences, 11 two-hop walks, and 10 nonempty source/relationship groups. Singleton and filtered ordered limit retain complete rows. The selected commerce type has one original record, so native total=1/returned=1 does not exercise truncation; a three-object unit fixture checks the independent oracle's observable limit. A subsequent read-only native regression on the existing Delta0 nodes selects the deterministic largest original type (fulfillments), orders its two complete original keys and applies limit1: native total2/returned1 with exact full-row parity. `observable-limit.json` retains the executed limit plan and independent original keys/rows. No scalar promotion is claimed: original JSON, lexical scalar values and presence remain exact canonical text carriers.

The report preserves original source/admission, full source snapshot vector, local Delta0 UUIDs, engine versions, jar hashes, original expected bags and actual native bags. The source oracle reads original model/graph lexical values and separately admitted development bindings; it does not derive expected values from exported rows or native query output. Private custody admits only rematerializing immutable export bytes, with no continued native source retention, Unity Catalog, production authority or native Databricks qualification. Commerce scenario queries and shared scalar/presence/topology fixtures remain outstanding.

Reproduce with existing local dependencies only, from the repository root:

```sh
export JAVA_HOME=/private/tmp/ashlar-db-client/lib/python3.9/site-packages/jdk4py/java-runtime
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/private/tmp/ashlar-spark311/bin/python
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONPATH=src:tools
"$PYSPARK_PYTHON" tools/run_commerce_release_graphframes.py \
  --release docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json \
  --custody docs/helix/04-build/evidence/commerce-graph-export-20261009/custody.json \
  --release-sha256 eeef228c5fa69720ac082dc3e19adbd63cebe17d1cdf877af250c8a9908aaed6 \
  --custody-sha256 667f5c8260fe6a37c1838495434c08aaa5ee1331aad507b8a65bac6167c731f8 \
  --output /private/tmp/ashlar-commerce-graphframes-FRESH \
  --jars /private/tmp/ashlar-scale-ivy/jars
python3 -m unittest discover -s tests -p test_commerce_release_graphframes.py
```

The retained actual output is `/private/tmp/ashlar-commerce-graphframes-complete-20261009-1701`. No shared cloud endpoint was used. The earlier already-existing `...graphframes-20261009-a` and `...-b` paths were refused before Spark startup; neither was changed.

The separate `tools/check_commerce_graphframes_limit.py` CLI repeats this read-only limit check with `--native-nodes /private/tmp/ashlar-commerce-graphframes-complete-20261009-1701/nodes`, the same release/custody digests and jars, and a fresh `--output` report. The previous four-scenario source-profile entrypoint `tools/run_commerce_graphframes.py` and its tests are preserved unchanged.
