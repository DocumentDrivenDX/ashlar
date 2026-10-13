# Configure and run the local supply-chain workflow

`ashlar configure-supply-chain-native` prints a valid configuration for the
supported five authored queries plus twelve two-hop queries. It does not start
Spark, install compilers, or create the publication. The run command creates a
fresh local publication, checks replay, executes the queries, and writes
`report.json` under the selected output directory.

Use an Ashlar installation in Python 3.11 with PySpark 4.0.1, Delta Spark 4.0.0,
and JDK 21. Select Bun 1.4.2 and a clean UMF checkout at
`fc78a6d48f08b3640748ccac0fc4a9ee06fc61e3`. Supply the original supply-chain
pack, ontology, and graph from `examples/domain-packs/supply-chain/upstream/`
in an Ashlar checkout. The JAR directory must contain exactly the supported
`delta-spark_2.13-4.0.0.jar` and `delta-storage-4.0.0.jar`; the run verifies their
pinned bytes.

The CountStar 041 and Paths candidate require their matching distribution
indexes and package archives. Install each through its public owner first:

```sh
python3.11 -m ashlar install-weft-count-star \
  --index "$COUNT_STAR_INDEX" --package "$COUNT_STAR_PACKAGE" \
  --output "$COUNT_STAR_INSTALLATION"
python3.11 -m ashlar install-weft-paths \
  --index "$PATHS_INDEX" --package "$PATHS_PACKAGE" \
  --output "$PATHS_INSTALLATION"
```

Set those variables to absolute paths for your selected artifacts. Installation
outputs must be fresh. Use the Paths index/package for the supported source
commit `530ae35`, not an unrelated or newer index. The installers validate the
selected distributions; configuration generation does not grant compiler or
source authority.

With `ASHLAR_CHECKOUT`, `UMF_CHECKOUT`, `DELTA_JARS`, `BUN`, and `GIT` set to your
absolute local paths, generate a configuration. `PUBLICATION` must be a fresh
path separate from every input and compiler installation. The generated JSON
uses the existing binary compiler evidence format and finite limits.

```sh
python3.11 -m ashlar configure-supply-chain-native \
  --output "$PUBLICATION" --jars "$DELTA_JARS" \
  --pack "$ASHLAR_CHECKOUT/examples/domain-packs/supply-chain/upstream/pack.json" \
  --model "$ASHLAR_CHECKOUT/examples/domain-packs/supply-chain/upstream/ontology.json" \
  --graph "$ASHLAR_CHECKOUT/examples/domain-packs/supply-chain/upstream/graph/fixture.json" \
  --producer-source "$UMF_CHECKOUT" --bun "$BUN" --git "$GIT" \
  --count-star-index "$COUNT_STAR_INDEX" \
  --count-star-installation "$COUNT_STAR_INSTALLATION" \
  --paths-index "$PATHS_INDEX" --paths-installation "$PATHS_INSTALLATION" \
  > supply-chain.json

export JAVA_HOME="/absolute/path/to/jdk-21"
export PYSPARK_PYTHON="/absolute/path/to/selected/python3.11"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
python3.11 -m ashlar run-supply-chain-native --configuration supply-chain.json
```

Use the same selected Python environment for every command. Paths must be
absolute and have no symlink components. Keep `supply-chain.json` outside the
selected inputs and publication directory. The native workflow uses local Spark
with one worker thread and 512 MiB driver memory. Run it under your finite
process supervisor; configuration generation does not supervise execution.
Success qualifies this local finite workflow; it does not qualify protected
ACK, cloud execution, or every common graph query.

### Select common query cases

For an already opened `OpenedSupplyChainCommonRead`, the public query API can
execute a small authored subset while retaining the complete source and graph
checks:

```python
report = query_supply_chain_common(
    opened, query_config, observe=store_original_protocol,
    selected_case_ids=("one-hop-0",),
)
```

Available IDs are `one-hop-0` through `one-hop-12`, `singleton`, `filtered-total`
and `filtered-limit`. Omit `selected_case_ids` to run all 16. Explicit selections
must be nonempty, unique and known; execution follows original declaration order.
The original requests, result oracles and integrity guards remain unchanged.
The observer stores provisional compiler artifacts; persist the report only
after both its storage and the actual outer reader close successfully. A selected
case result does not qualify unselected cases or replace native relationship
query evidence with a property join.
