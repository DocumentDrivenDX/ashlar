# Local current-query mechanics

This authored two-query fixture composes original Weft CQP objects, unchanged
compiler SQL, bounded Spark capture, complete pinned Delta source capture,
conditional refinement and positional public projection. Two rows are eligible
and one excluded, including Unicode, NUL and empty String values. Eligibility is
authored test data; it establishes no Unity Catalog, Subject, protected-source
or canonical publication authority.

For local execution, explicitly select the Spark contributors profile and a
reviewed development build exposing its lowerer and checker. The profile has
no SUM support or public-release claim. Databricks remains the adapter default;
its qualified builtin SQL requires the Databricks runtime.

Build the compiler from reviewed Weft commit
`c1973ececa5b684a8abcfc6606d66bb1c5ade1bb` with the explicit
`current-spark-scalar-candidate` feature, for example with `maturin build
--manifest-path /absolute/weft/crates/weft-python/Cargo.toml --features
current-spark-scalar-candidate`. Extract the resulting development wheel and
supply the selected native-module and Python-shim SHA-256 digests below.
The [execution evidence](../../docs/helix/04-build/evidence/current-local-end-to-end-20261015/README.md)
retains the tested build and exact local results.

Use an explicit Python environment with Spark 4.0.1, Delta 4.0.0, JDK 21,
the selected Weft native module and Python shim, and matching Delta Spark/storage
JARs. The runner checks their hashes, starts one local worker at 512 MiB, and
requires an existing owned 0700 output parent. Run from the Ashlar repository:

```sh
PYTHONPATH=src:tools /absolute/selected/python -B tools/run_local_current_query_example.py \
  --engine spark \
  --weft-root /absolute/private/reviewed-weft-module \
  --weft-module-sha256 SELECTED_NATIVE_MODULE_SHA256 \
  --weft-shim-sha256 SELECTED_PYTHON_SHIM_SHA256 \
  --weft-source-commit SELECTED_REVIEWED_SOURCE_COMMIT \
  --request /absolute/ashlar/examples/current-query-local/mixed-original-uses.request.json \
  --java-home /absolute/jdk21 \
  --delta-jar /absolute/delta-spark_2.13-4.0.0.jar \
  --delta-jar /absolute/delta-storage-4.0.0.jar \
  --schema ashlar_current_fresh \
  --output /absolute/private/fresh-local-current
```

The Spark binding is `weft.spark.current-values/0.1-candidate`; Databricks keeps
its original binding version. All selected engine methods must exist before
Spark starts. The result identifies the actual selected engine, source revision,
module/shim digests and development-versus-public selection. Both routes keep
compiler SQL unchanged and use the same two authored queries. Local Spark also
requires an unchanged catalog/database and complete bounded native user-function
inventory through closing. Harmless Delta functions may remain; functions that
shadow emitted builtins refuse. No functions are registered or shadow resolution repaired.

The operator must supervise the entire process with a finite deadline and owned
process cleanup. The runner runs `COUNT(*)` and `SELECT r.token` once each and
retains their exact request, binding, compiler artifact, refinement and public
result files. A final `result.json` appears only after both checks and Spark
closing succeed. Partial files on failure are diagnostic output, not a successful
result. Unsupported unmodified SQL is retained as a failure; no function shim or
SQL rewrite is installed. Retained source tables are confined to the fresh local
warehouse under the output directory.

The request fixture is copied unchanged from Weft public commit
`ff3695af5ece9d386b3dcbf266cf23d632997733`,
`tests/current-query-designation/fixtures/mixed-original-uses.request.json`.
The two explicit query requests retain that model/source declaration while
selecting the named local example queries. Runtime schema hashes use
`ashlar.local-spark-schema-json/0.1`; this is a local host convention, not a
Weft-defined native fingerprint or remote security profile.
