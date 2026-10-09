# Original commerce four GraphFrames scenarios

Four authored scenarios pass as actual GraphFrames queries on the existing canonical commerce immutable export's local Delta0 tables: partial-return `[[6,2,6]]`, over-fulfillment `[["F2"]]`, settlement `[["PAY1"]]`, and refund `[["RF1"]]`. Expected bags come from independent original CSV integer/Decimal calculations and match the original pack's authored expected rows without executing its SQL. Original pack and all CSV template digests are bound to the original graph metadata. The explicit scenario-replay identity mapping retains seed42/component0 and returns authored template identifiers only after the native row qualifies.

Every consumed original numeric JSON token is extracted from the canonical property ID declared by the admitted development bindings and compared exactly before native calculations. Actual Python Decimal results of the native casts match exact original numeric values. Original facetless Integer remains logically unbounded: this fixture uses an explicit finite decimal38 representation and refuses unrepresentable tokens. Original authored decimal18/scale2 fields retain their lexical tokens (`12.5`, `125`, `25`), with an exact representable native decimal18,2 value. No binary float, source relabel, invented logical width or canonical scalar promotion is involved.

Unfiltered native subtract/add and quantity×unit-price intermediate results are each independently checked against exact original integer/Decimal calculations before the final scenario predicate. ANSI arithmetic and disabled precision loss remain enabled; full original carriers, prior actual UUID/version0 vector, all opening/closing native file hashes and closing UUIDs are checked. The report retains all intermediate and final observations. The final c run exercised these guards; the earlier b result preceded their addition and is not the qualified receipt.

This is bounded read-only Spark3.5.3/Delta3.2.1/GraphFrames0.12.3 local immutable-export evidence, not a general arbitrary-precision math profile, native Databricks/UC, production authority, future native source retention or Weft compiler qualification. No source/Delta writes occurred. Shared scalar/presence/topology fixtures and graph-native PuppyGraph scenario arithmetic remain unfinished.

Reproduce with the existing local runtime and same release/custody pair:

```sh
export JAVA_HOME=/private/tmp/ashlar-db-client/lib/python3.9/site-packages/jdk4py/java-runtime
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/private/tmp/ashlar-spark311/bin/python
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONPATH=src:tools
"$PYSPARK_PYTHON" tools/check_commerce_graphframes_scenarios.py \
 --release docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json \
 --custody docs/helix/04-build/evidence/commerce-graph-export-20261009/custody.json \
 --release-sha256 eeef228c5fa69720ac082dc3e19adbd63cebe17d1cdf877af250c8a9908aaed6 \
 --custody-sha256 667f5c8260fe6a37c1838495434c08aaa5ee1331aad507b8a65bac6167c731f8 \
 --native /private/tmp/ashlar-commerce-graphframes-complete-20261009-1701 \
 --jars /private/tmp/ashlar-scale-ivy/jars \
 --output /private/tmp/ashlar-commerce-graphframes-scenarios-FRESH.json
python3 -m unittest discover -s tests -p test_commerce_graphframes_scenarios.py
```

Actual final receipt: `/private/tmp/ashlar-commerce-graphframes-scenarios-20261009-c.json`. The Spark process exited and the slot is free.
