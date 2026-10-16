# Publish and query the original finite packs

Use `ashlar publish-finite-pack` to publish the unchanged archaeology or ecology
fixture into a private local Delta installation. Use `ashlar query-finite-pack`
to reopen its committed publication and execute every original scenario: eight
for archaeology or nine for ecology. The query command preserves the original
SQL, model documents, property/carrier identities and complete result bags.

Provision the native query environment with Python 3.11, JDK 21, Spark 4.0.1,
Delta Spark 4.0.0 and the exact two Delta JARs required by the selected native
profile. The selected indexed Weft COUNT star realization requires Darwin arm64
with macOS 27.0.1; this selection does not admit another host platform. The JAR
directory must contain exactly `delta-spark_2.13-4.0.0.jar` (SHA-256
`538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e`)
and `delta-storage-4.0.0.jar` (SHA-256
`9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d`).
Keep these selected runtime versions and original input bytes unchanged across
publication and query. Install Ashlar into that environment. Keep the indexed Weft COUNT star
installation and its index available as separate explicit paths; the query
command verifies that installed compiler instead of rebuilding it. The public
UMF dataset producer requires Bun, Git and a clean UMF checkout at
`07357eadbda9cf8299b9d72d004cbb77f8091aee`. These commands do not discover or
install runtimes, contact PostgreSQL or create protected ACK authority.

The query command also requires the offline JSON Schema validator in the same
Python environment. The selected validator environment uses `jsonschema==4.23.0`,
`referencing==0.35.1`, `jsonschema-specifications==2024.10.1`, `attrs==24.2.0` and
`rpds-py==0.21.0`. Supply these dependencies before running the query command;
Ashlar's base package has no runtime dependencies. The validator uses only the
installed compiler's pinned schema bytes and performs no network retrieval,
default insertion or value coercion.

Choose an absolute working directory with separate locations for the original
files, native publication and each fresh observation run. Keep the entire
original `upstream` pack directory unchanged. For example, create a publication
configuration with these fields, substituting your absolute paths:

```json
{
  "profile": "ashlar-original-finite-pack-invocation/0.1",
  "mode": "publish",
  "pack": "archaeology",
  "publication": "/work/ashlar/archaeology/publication",
  "jars": "/opt/ashlar/delta4-jars",
  "model": "/work/ashlar/source/archaeology/upstream/ontology.json",
  "graph": "/work/ashlar/source/archaeology/upstream/graph/fixture.json",
  "pack_file": "/work/ashlar/source/archaeology/upstream/pack.json",
  "producer": {
    "source": "/opt/ashlar/umf-original-producer",
    "bun": "/opt/bin/bun",
    "git": "/usr/bin/git",
    "timeout_seconds": 30,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 33554432
  },
  "operation_capacity": {
    "profile": "ashlar-private-local-operation-capacity/0.1",
    "max_intent_bytes": 8388608
  },
  "output": "/work/ashlar/archaeology/publish-observations"
}
```

```sh
ashlar publish-finite-pack --configuration /work/ashlar/archaeology/publish.json
```

Publication requires a fresh installation directory. It validates the original
whole dataset through the public producer, publishes its finite batch, checks
exact replay and reopens the committed phases. Current source identity, native
UUIDs, retained snapshots and the owned local lease remain mandatory. Keep the
selected operation capacity unchanged when reopening this installation.

For a query configuration, preserve the publication, inputs, producer and capacity
fields. Select profile `ashlar-original-finite-pack-invocation/0.2`, change `mode`
to `query`, select a fresh separate `output` directory and add the following
`query` object:

```json
{
  "index": "/opt/ashlar/weft/index.json",
  "installation": "/opt/ashlar/weft/count-star-installed",
  "maximum_artifact_bytes": 1048576,
  "maximum_public_source_bytes": 2097152,
  "maximum_rows": 10000,
  "maximum_cell_bytes": 1048576,
  "maximum_total_cell_bytes": 16777216,
  "maximum_observation_bytes": 67108864
}
```

The source budget bounds each complete verification request and receipt; it does
not enlarge the compiler-artifact budget or native capture limits. Profile `0.1`
retains its original shared artifact/source budget and rejects the new field.
Keep the complete original source receipt; reducing its content to fit a limit
would lose the correspondence evidence required by the query.

```sh
ashlar query-finite-pack --configuration /work/ashlar/archaeology/query.json
```

The native owner registers scoped external aliases for the existing Delta tables
and verifies their UUIDs and locations. The command compiles and recompiles every
original query, validates the complete admitted capabilities and obligations,
executes native guards and compares the complete captured results with an
independent original graph oracle. Unknown capabilities, invalid null/absence
carriers, source drift and exceeded capacities refuse the whole result.

`observations.jsonl` retains bounded original compiler bytes and native schema/
rows before decoding or comparison. A failed comparison or closing check may
leave observations, but withholds the final `report.json`. The final report preserves selected decoded scalars with explicit integer and
Decimal tags, including integer key components beyond JavaScript’s safe integer
range. Path/key carriers retain original byte hex and ordered
occurrences. Unknown host objects refuse instead of being converted to text. It is
written only after source, publication, producer, transport, Spark and invocation
cleanup succeed. A receipt or report establishes correspondence; it never grants
source, writer, retention or protected ACK authority.

For ecology, select `pack: "ecology"`, its original model/graph/pack files and
its own publication and observation directories. Keep all nine original scenarios,
including empty results and explicit present-null values. Capture limits bound
retained output, not JVM/executor memory; native execution requires the selected
runtime and complete publication gates to succeed.

## Export a retained finite-pack publication

Generate the export-only configuration from the same original pack, model, graph,
producer and publication selections. `configure-finite-pack-export` supports
`archaeology`, `ecology` and `payments`; every flag below is required. It prints
`ashlar-original-finite-pack-export-configuration/0.1` JSON without reading file contents,
creating directories or starting engines. The required fresh export `--output`
is checked for lexical separation and is not included in the emitted JSON.
Keep the configuration file separate from that export target:

```sh
ashlar configure-finite-pack-export \
  --pack ecology --budget-profile small-local \
  --publication /absolute/private/ecology-publication \
  --output /absolute/private/ecology-export \
  --jars /absolute/path/to/delta-jars \
  --pack-file /absolute/path/to/ashlar/examples/domain-packs/ecology/upstream/pack.json \
  --model /absolute/path/to/ashlar/examples/domain-packs/ecology/upstream/ontology.json \
  --graph /absolute/path/to/ashlar/examples/domain-packs/ecology/upstream/graph/fixture.json \
  --producer-source /absolute/path/to/pinned-umf-producer \
  --bun /absolute/path/to/bun --git /absolute/path/to/git \
  > /absolute/private/ecology-export.json

ashlar export-original-finite-pack \
  --configuration /absolute/private/ecology-export.json \
  --output /absolute/private/ecology-export
```

The export command reopens the existing publication and checks its original source
and native boundaries before writing the retained release and custody artifacts.
It does not publish a new batch. Use the selected artifacts with the existing
[GraphFrames retained-export guide](graphframes-toolkit.md),
[PuppyGraph original-scenario guide](../puppy-original-scenarios/README.md) or
[Fabric graph materialization and GQL guide](../fabric-gql/README.md), preserving
each guide's explicit runtime, admission and deployment requirements. Configuration
generation does not establish a new native export or engine-query pass.
