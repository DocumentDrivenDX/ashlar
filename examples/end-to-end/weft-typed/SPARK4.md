# Executable local typed Weft route

`tools/run_local_weft_typed_spark4.py` executes the five original compiled typed
queries and every emitted scalar-integrity statement on two local Delta rows.
It compares exact integer, decimal, boolean and Unicode output carriers with
independently authored expectations. Three additional local versions inject
signed64 overflow, decimal rounding and a boolean stored as a string. Original
compiler integrity SQL must identify exactly the corrupt field before user SQL;
the original successful snapshot must remain unchanged.

The required local dependencies are Spark4.0.1, Delta4.0.0 for Scala2.13, a full
JDK21, Bun and the pinned public UMF/Weft checkouts and compiler wheel. The runner
checks the original compiler extension, both Delta jar hashes and clean source
revisions. Matching open-source jars can be obtained from Maven Central:

- `io.delta:delta-spark_2.13:4.0.0`
- `io.delta:delta-storage:4.0.0`

Existing Sail0.6.6 rejects unchanged `COLLATE UTF8_BINARY` and the numeric variant
path; its `parse_json('12.50')` reports DOUBLE. Existing Spark3.5.3 likewise
rejects the collation syntax. The Spark4 route uses original SQL and parameters,
with no substitution or compatibility rewrite.

```sh
export JAVA_HOME=/path/to/full/jdk21
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/path/to/spark4.0.1/python
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
PYTHONPATH=src:tools "$PYSPARK_PYTHON" -B tools/run_local_weft_typed_spark4.py \
  --umf-source /path/to/clean/umf-c45c72a2 \
  --weft-source /path/to/clean/weft-2744531735c2 \
  --wheel /path/to/pinned/weft-wheel \
  --jars /path/to/delta4-jars \
  --output /tmp/ashlar-typed-spark4-new
ASHLAR_LOCAL_WEFT_SPARK4=/tmp/ashlar-typed-spark4-new \
  PYTHONPATH=src:tools:tests python3 -B -m unittest test_local_weft_typed_spark4
```

This is an independent experimental local engine profile. The original compiler
still targets its Databricks-qualified profile; the required native-profile and
immutable-publication obligations remain refused. The actual local object table
UUID is retained as a compiler-shaped locator, with no manifest authority. The
runner publishes no result and acknowledges no source. Successful local SQL
compatibility does not qualify Databricks builds, Unity Catalog interoperability,
accepted catalog IDs, caller authorization or production query runtime.
