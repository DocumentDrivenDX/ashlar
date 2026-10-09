# Independent typed Weft local compatibility probe

This fixture is separately authored UMF core0.7. It binds development type27 and
properties31–34 to a required signed64 integer, decimal18,2, boolean and string.
It does not reinterpret the current core0.8 typed fixture or any domain pack.
The two original JSON rows include an integer above the binary64 safe-integer
range, exact decimal tokens, both boolean values and a Unicode string.

The runner invokes the pinned public UMF Record API after its explicit public
core0.7→0.8 upgrade, retains the complete upgrade and original check receipts,
and requires complete field and Record checks. It writes the unchanged original
props text to a two-row local Delta table and retains the table's actual UUID and
version. It compiles typed projection, filter, grouped COUNT, equality self-join
and global COUNT with the actual pinned Weft compiler, retaining each original
request, logical plan, typed descriptors, obligations and emitted SQL.

This is an independent experimental Spark3.5.3/Delta3.2.1 compatibility profile.
The compiler only exposes its Databricks-qualified target here. Its required
native-profile and immutable-publication obligations remain refused locally.
The compiler-shaped manifest locator is explicitly the observed local object
UUID, never an immutable manifest or native publication claim. Screening tries
original emitted integrity and user SQL unchanged; it adds no SQL rewrite,
compatibility shim, hidden cast or alternative result. Unsupported SQL is
retained as an engine compatibility gap. No query result is published or ACKed.

Use existing local packages and jars, with a fresh output directory:

```sh
export JAVA_HOME=/path/to/existing/jdk17
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/path/to/existing/spark-python
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
PYTHONPATH=src:tools "$PYSPARK_PYTHON" -B tools/run_local_weft_typed_probe.py \
  --umf-source /path/to/clean/umf-c45c72a2 \
  --weft-source /path/to/clean/weft-2744531735c2 \
  --wheel /path/to/pinned/weft-wheel \
  --jars /path/to/existing/delta-jars \
  --output /tmp/ashlar-local-weft-typed-new
ASHLAR_LOCAL_WEFT_PROBE=/tmp/ashlar-local-weft-typed-new \
  PYTHONPATH=src:tools:tests python3 -B -m unittest test_local_weft_typed_probe
```

The local runtime does not establish Unity Catalog interoperability, native
Databricks build compatibility, accepted catalog IDs, immutable retention,
caller authorization or query-runtime readiness. Extending the production
binding requires admitting exact original schema/field receipts, every compiler
obligation and column source identity independently before any SQL/result use.
