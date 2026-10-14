# Run typed queries on one publication

`ashlar query-truss-typed-batch` shares one owned Spark runtime across a finite
gallery of existing query routes. It reads an explicitly selected Truss
publication; it does not publish changes or acknowledge source events.

Start with the [typed Truss quickstart](QUICKSTART.md) and retain its reader
configuration. If its source server has stopped, reopen it and generate a reader
configuration for that same installation using the quickstart's commands.
Select the compatible compiler and indexed 0.4.2 installation described in the
[query guide](README.md). The compatible candidate has its own platform and
binary pins; a public Weft wheel for another backend cannot substitute for it.

Save this batch in a private file. Replace compiler paths and select the actual
document and accepted revision from your reader's model. These three queries use
the bundled `typed-related-record` model:

```json
{
  "profile": "ashlar-truss-typed-query-batch/0.1",
  "compilers": {
    "compatible": {"executable": "/absolute/path/to/compatible-weft"},
    "indexed": {
      "index": "/absolute/path/to/distributions/index.json",
      "installation": "/absolute/path/to/qualified-weft-installation"
    }
  },
  "queries": [
    {
      "name": "quantities",
      "family": "scalar",
      "sql": "SELECT r.quantity AS quantity FROM TypedRecord r",
      "revision": "1",
      "document_id": "typed-related-record",
      "physical_revisions": ["1"],
      "maximum_rows": 2
    },
    {
      "name": "quantity-counts",
      "family": "numeric-grouped-count",
      "sql": "SELECT r.quantity AS quantity,COUNT(*) AS total FROM TypedRecord r GROUP BY r.quantity",
      "revision": "1",
      "document_id": "typed-related-record",
      "physical_revisions": ["1"],
      "maximum_rows": 2
    },
    {
      "name": "two-hop-paths",
      "family": "two-hop-count",
      "sql": "SELECT COUNT(*) AS paths FROM TypedRecord r CROSS JOIN EXPAND_PATHS(r.related,related) AS p",
      "revision": "1",
      "document_id": "typed-related-record",
      "maximum_rows": 1
    }
  ]
}
```

```sh
ashlar query-truss-typed-batch \
  --configuration /absolute/private/reopened-reader.json \
  --batch /absolute/private/typed-batch.json \
  --output /absolute/private/fresh-batch-result.json
```

Include exactly the compiler entries required by the selected families. Scalar
projections, scalar equality joins and numeric grouped counts use `compatible` and require `physical_revisions`;
two-hop counts and root projections use `indexed` and reject that member. A compatible-only batch
does not need an indexed installation. Unknown families refuse rather than
falling back to another compiler or changing SQL.

To include an admitted equality join in the same batch, add a `scalar-join`
query with the same revision selections and its own row bound. For the bundled
model, use `SELECT a."record-id" AS left_id,b."record-id" AS right_id FROM TypedRecord a JOIN TypedRecord b ON a.quantity=b.quantity`.
This reuses the standalone join route's exact consumed-field admission and
complete pair multiplicity; it does not add relationship traversal or arithmetic.

For one row per authored two-hop occurrence, select `two-hop-projection` with
`SELECT r."record-id" AS root FROM TypedRecord r CROSS JOIN EXPAND_PATHS(r.related,related) AS p`
and a row bound large enough for the complete bag. Use the same `indexed`
selection, revision and document ID as the count case, without
`physical_revisions`. The initial profile projects one required STRING root-key
field, unordered and unfiltered. Two parallel root self-loops emit four root
rows; an isolate emits none. Output is neither deduplicated nor truncated.
The [installed local traversal check](../../docs/helix/04-build/evidence/truss-authored-two-hop-projection-native-20261014/README.md)
qualifies that exact four-row bag on retained Truss15 data with indexed Weft0.4.2.

Each executor retains its source/model admission, integrity obligations, native
pins and closing checks. Every query must match the original complete manifest
and publication. Original request and response text is retained; indexed queries
also retain independent recompilation text. The combined report is written only
after all queries, input renewal and owned closure succeed. A failed query
withholds the whole report; retain any partial private diagnostics.

Limits are one to eight uniquely named queries, 10,000 aggregate requested and
returned rows, and 64 MiB of report/protocol content. Exact integers and decimals
remain in their admitted carrier forms. This composition does not widen any
family's SQL subset, numeric facets, source authority or compiler qualification.

The [installed batch check](../../docs/helix/04-build/evidence/truss-typed-query-batch-installed-20261013/result.json)
records three completed query routes over the retained Truss 15 parallel-edge
publication, including exact quantities and a two-hop path count of four. Its
evidence separates the successful CLI report from a later outer-check failure;
checkpoint comparison is not qualified by that run.
