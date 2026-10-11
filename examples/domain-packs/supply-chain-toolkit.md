# Publish and query the original supply-chain fixture

Run `ashlar run-supply-chain-native --configuration /work/ashlar/supply-chain.json`
to create a fresh private local Delta publication, verify exact replay, reopen
its committed phases and execute all five unchanged original queries:
split-excursion, excursion, replay, lineage and sensor. The command preserves
original model/graph bytes, carrier identities, presence and complete occurrence
bags. Its final report is released only after source, invocation, transport,
lease and Spark cleanup checks. Failed comparisons retain bounded compiler and
native observations and withhold the complete report.

Use an already provisioned Python 3.11 environment with Spark 4.0.1, Delta Spark
4.0.0, JDK 21 and Ashlar installed. The selected indexed Weft 0.4.1 COUNT star
installation requires Darwin arm64 with macOS 27.0.1. Keep its verified index
and installation as explicit paths. Supply the offline validator dependencies
listed in [the finite-pack guide](finite-toolkit.md), and an exact two-file JAR
directory containing `delta-spark_2.13-4.0.0.jar` (SHA-256
`538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e`)
and `delta-storage-4.0.0.jar` (SHA-256
`9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d`).
The command selects one local Spark worker with 512 MiB driver memory. It does
not install SDKs or choose a remote cluster.

The public UMF producer requires explicit Bun and Git binaries and a clean
checkout at `fc78a6d48f08b3640748ccac0fc4a9ee06fc61e3`. Select the original
files under `examples/domain-packs/supply-chain/upstream`. Every input must be
an absolute cooperating path without symlinks. The output must be fresh and
separate from the original inputs, producer and compiler installation.

```json
{
  "profile": "ashlar-original-supply-chain-native-invocation/0.1",
  "output": "/work/ashlar/supply-chain-publication",
  "jars": "/work/runtime/delta4-jars",
  "pack": "/work/ashlar/source/examples/domain-packs/supply-chain/upstream/pack.json",
  "model": "/work/ashlar/source/examples/domain-packs/supply-chain/upstream/ontology.json",
  "graph": "/work/ashlar/source/examples/domain-packs/supply-chain/upstream/graph/fixture.json",
  "producer": {
    "source": "/work/umf",
    "bun": "/work/runtime/bin/bun",
    "git": "/usr/bin/git",
    "timeout_seconds": 30,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 8388608
  },
  "compiler": {
    "index": "/work/weft/distributions/index.json",
    "installation": "/work/ashlar/weft-count-star"
  },
  "operation_capacity": null
}
```

Keep `operation_capacity` null for the selected default, or supply the exact
closed capacity configuration of the native profile. A saved configuration or
report grants no source, writer, retention or ACK authority. This finite-file
workflow acquires its own actual local source and publication holds; it supplies
no PostgreSQL protected ACK, Truss, Unity Catalog or remote-source admission.

The report uses explicit typed carriers for decoded exact integers, decimals
and original bytes. Decimal text retains scale and signed zero; decoded integer
keys are tagged text so JSON readers do not round them. Native schema/rows and
compiler request/response observations retain their original order and bytes.
No compiler SQL or observed native cells are repaired to match expected results.
