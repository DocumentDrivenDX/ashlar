# Payments source

Convert the pinned original UMF 0.8 payments model and graph to a whole-entity
source transaction with explicit development bindings:

```sh
ashlar payments-source --ontology /absolute/path/ontology.json \
  --graph /absolute/path/graph/fixture.json --source-system selected-payments \
  --output /absolute/private/payments-candidate
```

Select the bundled [ontology](upstream/ontology.json) and
[graph](upstream/graph/fixture.json). The fresh output contains `source.jsonl`,
`bindings.json`, and byte-exact `ontology.json` and `graph.json`. All 13 objects
and 14 edges retain their original identities, composite keys, endpoints, currency
text and decimal tokens, including exponent notation. Original rows remain in
retained content.

Before publication, a host must hold and validate the original inputs through
`FinitePackDefinition("payments")` and the public UMF dataset boundary, then
establish its source and writer authority. Candidate bindings describe
correspondence; they grant no authority. The [public admission evidence](../../../docs/helix/04-build/evidence/payments-public-source-20261013/result.json)
records complete dataset validation and three invalid-dataset controls.

The bundled [pack](upstream/pack.json) retains its financial scenario SQL for
separate compiler and execution admission. Exact source preservation does not
establish settlement arithmetic or currency-conversion enforcement.

## Publish on local Delta

Use the original inputs with Python 3.11, Spark 4.0.1, Delta 4.0.0 and JDK 21.
Save an invocation configuration with these selections:

```json
{
  "profile": "ashlar-original-finite-pack-invocation/0.1",
  "mode": "publish",
  "pack": "payments",
  "publication": "/absolute/private/payments-publication",
  "jars": "/absolute/path/to/delta-jars",
  "model": "/absolute/path/ontology.json",
  "graph": "/absolute/path/graph/fixture.json",
  "pack_file": "/absolute/path/pack.json",
  "producer": {
    "source": "/absolute/path/to/clean-umf-07357ead",
    "bun": "/absolute/path/to/bun",
    "git": "/absolute/path/to/git",
    "timeout_seconds": 30,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 33554432
  },
  "operation_capacity": null,
  "output": "/absolute/private/payments-observations"
}
```

The producer must be the clean UMF revision
`07357eadbda9cf8299b9d72d004cbb77f8091aee`. Select the existing Delta Spark and
storage 4.0.0 JAR directory. Publication and observation outputs must be fresh.

```sh
JAVA_HOME=/absolute/path/to/jdk21 ashlar publish-finite-pack \
  --configuration /absolute/private/payments-publish.json
```

The returned `report` path selects the command report. The publication directory
also retains its full native report, original request, registry and public dataset
receipt. Publication checks the complete current/history rows, exact replay and
committed read-only reopen. The [native example](../../../docs/helix/04-build/evidence/payments-native-publication-20261013/result.json)
retains all 27 original source witnesses under private local ownership.

## Count a published record population

For `query-payments-count`, retain the same original publication and producer
selections. Change `profile` to `ashlar-original-finite-pack-invocation/0.2` and `mode` to
`query`, choose a fresh
observation output, and add:

```json
{
  "query": {
    "index": "/absolute/path/to/weft/distributions/index.json",
    "installation": "/absolute/private/weft-count-star-installation",
    "maximum_artifact_bytes": 1048576,
    "maximum_rows": 1,
    "maximum_cell_bytes": 10000,
    "maximum_total_cell_bytes": 100000,
    "maximum_observation_bytes": 1048576,
    "maximum_public_source_bytes": 1048576
  }
}
```

Select the authentic indexed Weft 0.4.1 count-star installation.

```sh
JAVA_HOME=/absolute/path/to/jdk21 ashlar query-payments-count \
  --configuration /absolute/private/payments-query.json --record ledger_entries
```

The observation directory retains original compiler protocols in
`observations.jsonl`. The report includes the complete native result, exact scalar
decoding and independent original count. This fixture has four
ledger entries. The [native count example](../../../docs/helix/04-build/evidence/payments-native-count-20261013/result.json)
queries the existing publication without reingestion. The dedicated command
admits a named base-record count; financial scenarios require separate admission.
