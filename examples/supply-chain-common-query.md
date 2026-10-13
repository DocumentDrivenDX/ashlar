# Query an existing supply-chain publication

`query-supply-chain-common` runs selected original cases without creating tables
or publishing again. Use the same original model, graph, pack, producer and
operation capacity as the existing installation. The selected Python environment
must already contain its compatible Spark/Delta dependencies.

Generate the configuration from the original native invocation without running
the producer or engine:

```sh
ashlar configure-supply-chain-common \
  --configuration /absolute/private/original-native.json \
  --python /absolute/path/to/current-python --java-home /absolute/path/to/jdk21 \
  --output /absolute/private/new-common.json
```

The original publication, producer settings, inputs, compiler and capacity are
retained. Python must be the current interpreter; the output is a fresh private
file. This prepares selections only; the query still admits the live owners.

```json
{
  "profile": "ashlar-supply-chain-common-query/0.1",
  "publication_directory": "/absolute/private/existing-publication",
  "python": "/absolute/path/to/current-python",
  "java_home": "/absolute/path/to/jdk21",
  "jars": "/absolute/path/to/existing-delta-jars-directory",
  "pack": "/absolute/path/to/original-pack.json",
  "model": "/absolute/path/to/original-model.json",
  "graph": "/absolute/path/to/original-graph.json",
  "producer": {
    "source": "/absolute/path/to/selected-original-umf",
    "bun": "/absolute/path/to/bun",
    "git": "/absolute/path/to/git",
    "timeout_seconds": 60,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 8388608
  },
  "compiler": {
    "index": "/absolute/path/to/public-041/distributions/index.json",
    "installation": "/absolute/path/to/public-041-installation"
  },
  "operation_capacity": {
    "profile": "ashlar-private-local-operation-capacity/0.1",
    "max_intent_bytes": 8388608
  }
}
```

```sh
ashlar query-supply-chain-common --configuration /absolute/private/common.json \
  --output /absolute/private/fresh-common-query --case singleton --case filtered-total
```

Available case IDs are `one-hop-0` through `one-hop-12`, `singleton`,
`filtered-total` and `filtered-limit`. Omit `--case` to run all sixteen; selected
cases retain authored order. Grouped-count is a separate pending family.

The command executes the original public producer validation once to obtain a
genuine same-process source invocation, then opens the existing native reader.
It uses the existing compiler and execution adapter unchanged. Protocol files
remain provisional until source, installation, Spark and query owners close;
only then is private `result.json` written. The report distinguishes selected
cases from the complete case inventory. Failures retain partial outputs and
never initialize or repair the existing publication. No downloads or cloud
compute are requested.
