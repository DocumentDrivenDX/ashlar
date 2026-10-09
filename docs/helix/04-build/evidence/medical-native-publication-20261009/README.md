# Historical medical local publication evidence

The bounded fresh `b` run consumed the original medical 1.0 archive/model and historical graph, actual public UMF `8e76c74` admission and explicit Boolean lexical receipts. It published all 51 objects, 62 edges and 113 history rows on existing local Spark 3.5 / Delta 3.2, checked full original source rows, observed protected PostgreSQL ACK 1, and verified exact replay did not change table history. The report was exposed only after transport close and Spark stop succeeded.

This private local fixture uses development bindings and the explicit `ashlar-private-local-operation-capacity/0.1` 8MiB operation-intent profile. Core phase/effect limits remain 4MiB. It does not establish Truss IDs, Unity Catalog, remote ingestion or production authority. Original model absence of an authored revision and owning historical pack revision remain explicit.

The original `a` attempt is retained unchanged at `/private/tmp/ashlar-medical-outbox-publication-20261009-a`; it applied current/history rows before the outer local operation intent exceeded the default 4MiB during `retain_applied`. It produced no successful report, publication manifest or ACK. This is interruption/provisional-state evidence, not a before-effects refusal. The fresh `b` installation is separate.

`native-files.json` binds the complete original local output. `frozen-source.json` identifies the reviewed source/capacity implementation. Early-exit byte budgeting applies to operation-intent serialization; producer `child_operation` still serializes upstream SQL/parameters for identity.

Reproduce in a fresh exclusive output directory using the already installed qualified runtime (no installs):

```sh
JAVA_HOME=/private/tmp/ashlar-db-client/lib/python3.9/site-packages/jdk4py/java-runtime \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/ashlar-db-client/bin/python \
PYSPARK_DRIVER_PYTHON=/private/tmp/ashlar-db-client/bin/python \
PYTHONPATH=src:tools:/private/tmp/ashlar-spark311/lib/python3.11/site-packages \
/private/tmp/ashlar-db-client/bin/python tools/run_medical_outbox_publication.py \
  --output /private/tmp/ashlar-medical-publication-reproduction \
  --jars /private/tmp/ashlar-scale-ivy/jars \
  --umf-source /private/tmp/ashlar-umf-medical-boolean-cea3
```

The UMF source must be the exact clean `8e76c74d14203225d1ef159c9132bb9e9b0cdffe` public pin. Local runtime paths are retained execution dependencies, not portable installation defaults.

The original `native-files.json` observation is unchanged. SQLite `operations.sqlite-shm` and `operations.sqlite-wal` are transient coordination sidecars; legitimate readonly inspection can change them. Stable parity covers the other 78 original files. `operations-snapshot.sqlite.gz` retains a consistent SQLite online backup taken through a readonly connection after producer close; `journal-snapshot.json` binds its exact uncompressed and compressed bytes. The snapshot preserves the complete nine committed operation intents for independent native commit/registry recomputation.
