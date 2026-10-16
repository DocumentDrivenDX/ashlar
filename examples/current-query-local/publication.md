# Query an original commerce publication locally

Select an existing original-commerce publication and its actual ordinary
PostgreSQL source/ACK scope using the export-only configuration profile
`ashlar-original-commerce-export-configuration/0.1`. Its closed members are
`profile`, `publication` (absolute retained publication directory), and `source`.
Copy `source` unchanged from the original publisher selection; do not invent
signatures, roles or ACK scope from a historical report.

Use the explicitly selected Spark compiler build and query runtime described in
[the local guide](README.md), with Python 3.11, Spark 4.0.1, Delta 4.0.0,
SQLite 3.53.1, JDK 21 and the exact query JAR directory required by
`ashlar_host.commerce.runtime_paths`. The original source/publication must remain
available. Provisioning a fresh source and publishing are separate operations
described in [the original-commerce guide](../original-commerce-local/README.md).

Run under an operator-owned finite process deadline:

```sh
PYTHONPATH=src:tools /absolute/selected/python -B tools/run_publication_current_query_example.py \
  --configuration /absolute/private/export-only.json \
  --weft-root /absolute/private/reviewed-weft-module \
  --weft-module-sha256 SELECTED_NATIVE_MODULE_SHA256 \
  --weft-shim-sha256 SELECTED_PYTHON_SHIM_SHA256 \
  --java-home /absolute/jdk21 \
  --jars /absolute/exact-query-jars \
  --output /absolute/private/fresh-publication-query
```

The output parent must be canonical, owned by the current user and mode0700.
The runner creates a fresh local staging schema, derives the complete products
bag and required SKU values from the admitted original publication, then executes
COUNT and SKU projection through unchanged Weft SQL and complete source
refinement. The source reader holds the original complete native vector and
current protected ACK through staging, execution and closing. Querying does not
advance ACKs. Final output follows both publication-reader and Spark closing.

The authored local policy documents describe a conditional query profile. They
do not establish application Subject/action/disclosure permissions or managed
Unity Catalog enforcement. Pure mapping/lifecycle tests do not qualify a native
publication run; retain the actual invocation and closing result separately.
