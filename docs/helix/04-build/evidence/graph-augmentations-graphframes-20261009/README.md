# Authored shared scalar, presence and topology GraphFrames fixtures

The separate authored core0.8 model/source passes actual public UMF c7c95e1 finite dataset admission: 26 records, 26 declared keys, six independent relationship occurrences, complete supplied scope, and invalid absent-required/null-excluded controls refused. Original individual Record context diagnostics remain unchanged. No domain pack declaration, version or fixture was modified.

Actual bounded Spark3.5.3/Delta3.2.1/GraphFrames0.12.3 validates every complete original Value/state row and independent edge, 16 exact native JSON scalar extractions (empty/Unicode/composed/decomposed strings, true/false, signed64 values around ±2^53, fixed decimal lexical tokens including `-0.00`), and all five valid presence states through native state/value extraction. Absent and explicit null remain distinct through availability even when both native value extractions are SQL NULL. Public UMF defines logical validity/canonicalization; every original token remains retained. Native scalar property promotion is not claimed.

The independently enumerated original source edges produce two one-hop occurrences and eight two-hop walks from A1, with four distinct destinations, preserving parallel edges, return cycle and self-loop. A0 remains isolated. Native complete walk bags match. The source is checked against the exact corpus digest/copied definitions and actual prescribed scalar/topology/presence values before runtime acquisition, and the original public receipt is recomputed byte-identically by the public producer.

Native tables use an explicit source-profile representation: text ID/case and exact original record JSON; they are not a canonical Ashlar publication, catalog registration, durable source ACK, Unity Catalog or production authority. No general unbounded arithmetic, typed property promotion or PuppyGraph shared-fixture qualification follows. Only 26 nodes/six edges were materialized locally.

Reproduction with existing local dependencies:

```sh
bun tools/check_graph_augmentations.ts /private/tmp/ashlar-umf-dataset-45473 \
 examples/graph-augmentations/model.umf.json examples/graph-augmentations/source.json \
 /private/tmp/ashlar-augment-public-FRESH.json
export JAVA_HOME=/private/tmp/ashlar-db-client/lib/python3.9/site-packages/jdk4py/java-runtime
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/private/tmp/ashlar-spark311/bin/python
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONPATH=src:tools
"$PYSPARK_PYTHON" tools/run_graph_augmentations_graphframes.py \
 --model examples/graph-augmentations/model.umf.json \
 --source examples/graph-augmentations/source.json \
 --receipt /private/tmp/ashlar-augment-public-FRESH.json \
 --umf /private/tmp/ashlar-umf-dataset-45473 \
 --output /private/tmp/ashlar-augment-native-FRESH \
 --jars /private/tmp/ashlar-scale-ivy/jars
python3 -m unittest discover -s tests -p test_graph_augmentations.py
```

Actual retained native output is `/private/tmp/ashlar-graph-augmentations-graphframes-20261009-a`; receipt is `/private/tmp/ashlar-graph-augmentations-public-20261009-a.json`. Native UUID/version0 and jar/runtime versions are retained in the report. The process exited; no shared endpoint or paid/cloud compute was used.
