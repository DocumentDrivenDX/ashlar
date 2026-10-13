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

## Export and query the immutable graph

Select the existing publication with the same original producer, source and
runtime paths. Use profile `ashlar-original-finite-pack-export-configuration/0.1`
and omit invocation `mode`, `output` and `query`. Export reads the publication;
it does not require a financial SQL scenario.

```sh
ashlar export-original-finite-pack --configuration /absolute/private/payments-export.json \
  --output /absolute/private/payments-export > /absolute/private/payments-export-report.json
ashlar configure-graphframes-export \
  --export-report /absolute/private/payments-export-report.json \
  --export-report-sha256 ORIGINAL_REPORT_SHA256 \
  --java-home /absolute/path/to/jdk21 \
  --jar /absolute/path/to/graphframes-spark3.jar \
  --jar /absolute/path/to/graphframes-graphx-spark3.jar \
  --output /absolute/private/payments-graphframes.json
ashlar run-graphframes-export --configuration /absolute/private/payments-graphframes.json \
  --output /absolute/private/payments-topology.json --maximum-hops 2
```

Run the final command in the separate Spark 3.5 / GraphFrames 0.12.3 environment.
The immutable release and custody must remain paired with their original hashes.
The [small native example](../../../docs/helix/04-build/evidence/payments-immutable-graphframes-20261013/result.json)
checks complete node/edge carriers, one-hop and two-hop bags, degrees, path counts
and distinct destinations. Monetary values remain exact canonical strings;
financial property interpretation and authored financial SQL need separate admission.

## Query the original currency case

Use the query configuration above with an authentic indexed Weft 0.4.2
implicit-arithmetic installation, an observation directory that does not exist,
and room for the complete result bag (`maximum_rows`: 10 for this fixture).

```sh
JAVA_HOME=/absolute/path/to/jdk21 ashlar query-payments-scenario \
  --configuration /absolute/private/payments-scenario.json --case currency
```

This runs the original pack's `SELECT DISTINCT currency FROM ledger_entries`
unchanged. The command executes source and publication guards and compares the
full result with currencies derived independently from the original graph.
Keep compiler protocols and the completed report in the observation directory.
Other financial scenarios require separate compiler and native admission;
this case does not qualify settlement or arithmetic behavior.

The [native currency example](../../../docs/helix/04-build/evidence/payments-currency-native-20261013/result.json)
returns `USD` on the retained publication. The
[settlement guard refusal](../../../docs/helix/04-build/evidence/payments-settled-native-refusal-20261013/result.json)
and [original compiler cases](../../../docs/helix/04-build/evidence/payments-authored-compiler-20261013/result.json)
retain the separate numerical-carrier and JOIN ON limitations; neither qualifies
those financial scenarios as complete.
