# Publish and query a finite domain pack

Archaeology and ecology have installed native query evidence for their original
eight and nine scenarios. This workflow generates their existing configuration
shapes, publishes a fresh local Delta graph, and queries its retained publication.
Configuration generation itself starts no engine and grants no source authority.

Use Python 3.11 with installed `pyspark==4.0.1` and `delta-spark==4.0.0` packages.
Install Ashlar in that environment with `python -m pip install .`. Select Bun,
Git, JDK 21 and the Delta 4.0.0 JAR directory; JARs alone do not replace the Python
runtime packages.
The local native runner uses one Spark worker and 512 MiB. Select an independent
UMF producer checkout at `07357eadbda9cf8299b9d72d004cbb77f8091aee`; the actual
producer verifies that revision. Keep the original pack, ontology and graph bytes
under `examples/domain-packs/PACK/upstream/` unchanged. Select the qualified
CountStar compiler index and installation from the
[Weft installation guide](../end-to-end/WEFT-INSTALLATION.md).

Set the native runtime explicitly in your shell, replacing these paths:

```sh
export JAVA_HOME=/absolute/path/to/jdk21
export PYSPARK_PYTHON=/absolute/path/to/selected/python
export PYSPARK_DRIVER_PYTHON=/absolute/path/to/selected/python
```

Generate a publication configuration. All paths must be canonical and absolute;
publication and observation outputs must be separate fresh private locations
under existing parents. Run from the Ashlar repository root and replace the
repository prefix below with its actual absolute path:

```sh
ashlar configure-finite-pack \
  --pack ecology --mode publish \
  --profile ashlar-original-finite-pack-invocation/0.1 \
  --budget-profile small-local \
  --publication /absolute/private/ecology-publication \
  --output /absolute/private/ecology-publish-observations \
  --jars /absolute/path/to/delta-jars \
  --pack-file /absolute/path/to/ashlar/examples/domain-packs/ecology/upstream/pack.json \
  --model /absolute/path/to/ashlar/examples/domain-packs/ecology/upstream/ontology.json \
  --graph /absolute/path/to/ashlar/examples/domain-packs/ecology/upstream/graph/fixture.json \
  --producer-source /absolute/path/to/pinned-umf-producer \
  --bun /absolute/path/to/bun --git /absolute/path/to/git \
  > /absolute/private/ecology-publish.json

ashlar publish-finite-pack --configuration /absolute/private/ecology-publish.json
```

For queries, repeat the same original publication, JAR, pack, model, graph and
producer selections with `--mode query`, profile
`ashlar-original-finite-pack-invocation/0.2`, and a fresh observation `--output`.
Add `--index /absolute/path/to/index.json` and
`--installation /absolute/path/to/qualified-compiler-installation`, then save the
printed JSON to a separate private query file. Query profile 0.2 keeps the complete
public source receipt budget separate from the compiler artifact budget.

```sh
ashlar query-finite-pack --configuration /absolute/private/ecology-query.json \
  --case network
```

Omit `--case` to run all authored cases, or repeat it to select a small set.
For archaeology, substitute its three original upstream files and `--pack
archaeology`; `--case missing-media` is an available selected query. These commands
retain original SQL, compiler artifacts, source oracles and publication pins.
Keep partial output on failure and inspect the existing publication before retrying.

The closed `small-local` preset supplies developer-owned bounds: producer timeout
30 seconds, 1 MiB stdout and 32 MiB receipt capture; 8 MiB local operation intents;
query artifacts 1 MiB, 128 rows, 1 MiB per cell, and 16 MiB total cells and
observations. Query profile 0.2 allows 2 MiB of public source bytes. Bounds are
refusal thresholds, not allocations or permission grants. Runtime paths, source
files and compiler selections have no defaults.

The generator also supports payments publication. For payments query settings,
select `--query-profile payments-count` or `payments-scenario` and the matching
compiler, then use the [dedicated payments commands](payments/README.md).
Generic `query-finite-pack` does not accept payments. Supply-chain keeps its
[separate configuration workflow](../supply-chain-native/README.md).

The [archaeology](../../docs/helix/04-build/evidence/finite-pack-installed-query-archaeology-20261010-c/README.md)
and [ecology](../../docs/helix/04-build/evidence/finite-pack-installed-query-ecology-20261010-c/README.md)
evidence qualify the recorded installed scenarios and local source profiles.
This guide does not establish Unity Catalog deployment, protected source ACK,
Fabric execution or a general graph adapter for every domain pack.

The [installed configuration check](../../docs/helix/04-build/evidence/finite-pack-configuration-installed-cli-20261013/result.json)
covers all seven supported setup routes and an unsupported-route refusal without
starting engines or admitting resources. Setup success does not establish native
publication or query execution for the supplied paths.
