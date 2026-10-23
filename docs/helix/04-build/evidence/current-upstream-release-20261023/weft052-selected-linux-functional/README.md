# Selected Weft 0.4.52 Linux compiler check

The exact selected Linux x86_64 wheel built from Weft source `32ec02f0c3fd24a97b7726500dc5992c6475908d` was installed with `--no-index --no-deps` in an offline Linux amd64 Python 3.11 container. One compiled Source-F request and one unsupported-version refusal matched the frozen public 0.4.51 raw outputs byte-for-byte. The result is retained in `result.json`; `receipt.json` binds the wheel, container image, test script, inputs, and result hashes.

The single check used `python:3.11-slim` at image ID `sha256:b95327ce1439e687237841b0210677aeadd62d55a060a5f6bcb9c15bf6251dd3`, with `--platform linux/amd64 --network none --memory 1g --cpus 2`. Its reported runtime was `x86_64`, Python 3.11.17. The selected wheel SHA-256 was `9145b7f683dc84325cfe3ab7c4b885685db05be814a4d554a4b739ae43d5cbdc`. No alternate wheel, platform matrix, database, Spark, or Unity Catalog operation was run.

This qualifies only the two selected compiler-only cases on that installed Linux wheel. It does not qualify native SQL execution, Databricks runtime behavior, Truss feed custody, or the Ashlar release as a whole. The [selected Linux packaging packet](../weft052-selected-linux/README.md) retains the GitHub build provenance.
