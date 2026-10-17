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

For compiler-only work, `ashlar compile-supply-chain-count-star` accepts repeatable
`--case-id`. Allowed IDs are `split-excursion`, `excursion`, `replay`, `lineage`,
and `sensor`. Omit the option to compile all five; selections run in original
pack order. For example, use the command's explicit original input and installed
compiler paths to compile only replay:

```sh
ashlar compile-supply-chain-count-star \
  --pack /absolute/path/pack.json --model /absolute/path/ontology.json \
  --graph /absolute/path/fixture.json --metadata /absolute/path/compiler-metadata.json \
  --index /absolute/path/distributions/index.json \
  --installation /absolute/path/weft-installation --output /absolute/path/fresh-compile \
  --maximum-artifact-bytes 1048576 --case-id replay
```

Metadata retains the existing `bindings`, `manifest`, `registry`, and `aliases`
shape. This compiles and independently recompiles original SQL; it does not query
or authorize a native publication.


### Query the existing graph with GraphFrames

`query-supply-chain-graphframes` reads the publication selected by the original
native setup configuration. It opens the original public source and complete
publication vector, executes seven common operations in a separate GraphFrames
process, then closes every owner before writing `report.json`. It neither
publishes nor invokes Weft. The compiler installation fields in the original
configuration are not opened by this query.

Use the same Ashlar wheel in the canonical Spark 4.0.1/Delta 4.0.0 Python 3.11
installation and a separate Python 3.11 GraphFrames environment. Create that
environment with `python3.11 -m venv --copies "$GF_ENV"`; install the wheel,
PySpark 3.5.3, Delta Spark 3.2.1, GraphFrames Python 0.12.3 and its transitive
dependencies there. Include NumPy: the GraphFrame constructor imports
`pyspark.ml`. The recorded macOS ARM64 dependency selection uses NumPy 2.4.6.
Select JDK 17 and the four exact named JARs/hashes declared by
`ashlar_host.graphframes_release.JARS`; keep the canonical reader on JDK 21.

Inspect the actual GraphFrames environment without starting an engine. All
variables below must name your own existing absolute canonical paths. `GF_ENV`
is the copied virtual environment, `GF_SITE` its site-packages directory,
`PYTHON_STDLIB` its base Python standard-library directory, `GF_JAVA_HOME` the
selected JDK 17 home, and `GF_JARS` the directory containing those four JARs.
`ASHLAR_REVISION` selects the exact installed worker source. Keep the output
plan outside every inspected root.

```sh
"$GF_ENV/bin/python" -I -B -m ashlar inspect-graphframes-runtime \
  --worker-revision "$ASHLAR_REVISION" --worker-root "$GF_ENV" \
  --sdk-root "$GF_SITE" --transitive-root "$PYTHON_STDLIB" \
  --transitive-root "$GF_JAVA_HOME" --transitive-root "$GF_JARS" \
  --output /absolute/runtime-plan.json
```

Inspect and independently select the returned plan hash. Runtime inspection is
candidate correspondence, not source or publication admission. From the
canonical Spark 4 environment, run:

```sh
python3.11 -I -B -m ashlar query-supply-chain-graphframes \
  --configuration /absolute/supply-chain.json --java-home /absolute/jdk-21 \
  --runtime-plan /absolute/runtime-plan.json --runtime-plan-sha256 "$PLAN_SHA256" \
  --graphframes-java-home "$GF_JAVA_HOME" --graphframes-jars "$GF_JARS" \
  --output /absolute/fresh-graphframes-query
```

No provider file or custom runner is required. The output must be fresh and
separate from the publication, source, configuration and inspected runtime roots.
The command checks the actual selected worker interpreter, import origins,
versions and GraphFrame constructor in bounded probes at opening and closing;
complete selected namespace/file/JAR checks continue through the engine interval.
It retains bounded private probe transports, worker raw observations and the final
complete report. Run the overall command under a finite process supervisor as for
the native publication command; the isolated engine has a 300-second work ceiling,
30-second probe ceilings, 1 MiB engine streams and a 16 MiB evidence bound.

The operations cover full objects, full independent edges, one-hop, two-hop,
grouped occurrence/distinct-destination counts, full-row singleton and original
record-type filtered limit, with complete filtered membership and isolates as
controls. Canonical properties remain exact opaque STRING carriers. This profile
does not execute the five authored supply-chain scenarios in GraphFrames or grant
retained-export, protected ACK, Unity Catalog, cloud or production authority.
Private provisional SDK/probe files can remain after a refusal; only successful
command completion and the final closed report establish this command's result.
