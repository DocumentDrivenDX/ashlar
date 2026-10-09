# Original commerce immutable graph export

The bounded read-only export in `report.json` retains all11 original commerce
nodes,10 independent edge occurrences and every canonical carrier field. It
resolves the original complete four-table publication UUID/version vector and
compares full row bags with the independent original graph/model oracle. Original
public UMF0.8 supplied-dataset admission and individual Record diagnostics remain
in the unchanged manifest. Development type/property IDs are not Truss IDs.

`release.graph.json` is the original hash-bound immutable export. `custody.json`
is its separately admitted `ashlar-private-local-graph-custody/0.1` receipt. It
retains unchanged manifest strings, full native vector, graph roles, source
admission digest, original protected request/manifest/receipt bytes and fresh
opening/closing ordinary PostgreSQL session observations. All writer/pin/source
intervals, the outer native-file/journal reader and Spark shutdown completed
before either output artifact was exposed. All original native files remained
unchanged. No new publication or source acknowledgement was issued.

The original manifest has no retention-target inventory. The default loader
correctly refuses it. Explicit private-profile loading requires independently
trusted release **and** custody digests; it never adds retention to the manifest
or rescues a present malformed/mismatched target inventory. The profile admits
rematerialization of immutable export bytes only. It supplies no future canonical
retention, Unity Catalog, production source authority, scalar promotion, engine
activation or actual engine-query claim.

Runtime: existing Python3.9 DB client with psycopg3.2.13 and the pure Spark4.0.1/
Py4j bridge, Delta4.0.0 jars, JDK21, `local[1]`,512MiB, one shuffle/snapshot
partition. An initial attempt using Tablespec Python lacked psycopg and failed
without an output directory. The successful retained attempt is
`/private/tmp/ashlar-commerce-graph-export-20261009-b`. The provider's canonical
ACK-head admission was added after that process imported the module; retained
actual opening/closing head1 satisfies the added check under read-only receipt
revalidation. Native creation is not attributed to that later guard. The explicit canonical retained-ACK
head correspondence and CLI summary-output change likewise receive unit/read-
only replay coverage after the native run, without another source operation.

Reproduce a fresh bounded export using explicitly configured existing runtimes:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON=/private/tmp/ashlar-db-client/bin/python PYSPARK_DRIVER_PYTHON=/private/tmp/ashlar-db-client/bin/python PYTHONPATH=src:tools:/private/tmp/ashlar-spark4-python39-bridge /private/tmp/ashlar-db-client/bin/python tools/run_commerce_graph_release.py --publication /private/tmp/ashlar-canonical-commerce-publication-20261009-a --output FRESH_OUTPUT --jars /private/tmp/ashlar-delta4-jars
PYTHONPATH=src:tools:tests python3 -m unittest tests/test_private_graph_custody.py tests/test_graph_release_graphframes.py tests/test_commerce_graph_release.py
```

The17 scoped unit checks cover default-path retention refusal, explicit paired
custody, altered/missing/duplicate metadata, numeric boolean flags, source/ACK/
role/full-vector drift, retained original ACK under a later head, wrong ordinary
session, source/row/full-bag refusal, closing/suppression failures and cleanup of
interrupted persistence. Existing GraphFrames runtime/CLI can explicitly select
this custody profile; required named query bags and engine executions remain
subsequent independent work.
