# Query an immutable publication

Use the original commerce model and the publication produced by the
[local setup guide](README.md). The following queries are the exact cases in
`tools/run_commerce_publication_weft.py`; the runner compiles and executes them
against that publication and compares their complete results with the original
source oracle.

## Count products

```sql
SELECT COUNT(*) AS n FROM products p
```

Count admitted product occurrences. The result is an exact integer token; do not
convert it through a floating-point number. A count does not deduplicate objects
or establish that a source interval was completely delivered.

## Read exact prices

```sql
SELECT p.id, p.unit_price FROM products p
```

The original UMF model declares the price's decimal precision and scale; the
binding selects its physical home. Keep exact decimal values and original source
lexicals separately; presentation formatting
must not change equality or hide a capacity refusal. Preserve row multiplicity.

## Join products to suppliers

```sql
SELECT p.id AS product_id, s.name AS supplier_name
FROM products p JOIN suppliers s ON p.supplier_id = s.id
```

This is a property-equality join. Preserve every matching occurrence. An explicit
edge traversal is a different query and requires an admitted relationship mapping;
it cannot silently replace this join.

## Run the cases

Select your existing fresh commerce publication, the exact Delta4 jar directory
and the admitted Weft binary described in the setup guide. Set these paths
explicitly; the runner does not download a compiler or select cloud compute:

```sh
export ASHLAR_PUBLICATION=out/commerce-publication
export ASHLAR_DELTA4_JARS=/path/to/delta4-jars
export ASHLAR_WEFT_RUNTIME=/path/to/admitted-weft-runtime
PYTHONPATH=src:tools python3 tools/run_commerce_publication_weft.py \
  --publication "$ASHLAR_PUBLICATION" \
  --output out/commerce-queries \
  --jars "$ASHLAR_DELTA4_JARS" \
  --compiler "$ASHLAR_WEFT_RUNTIME"
```

Use the separate Spark4.0.1/Delta4.0.0 reader environment from the setup guide.
`out/commerce-queries` must not exist. The publication must use the explicit
commerce development binding profile 0.2 to select all three cases. The runner
retains original compiler requests, artifacts and its full query report. It runs
these fixed examples; this command is not an arbitrary SQL endpoint.

## Match a query to its boundary

| Operation | Required semantic boundary | Required execution boundary |
| --- | --- | --- |
| Singleton lookup | Complete typed identity and exact key predicates | Publication resolver, original UUID/version pins and effective read policy; Fabric is unnecessary |
| String or decimal projection | Original model/version, admitted Field home, presence and exact scalar representation | Every emitted source-integrity check, exact native schema and closed decoder |
| Property join | Explicit scan/Field identities and preserved matching multiplicity | Original compiler SQL, full publication vector and buffered guarded release |
| Edge traversal | Qualified independent edge identity, endpoints and direction | A separately admitted relationship or engine-release profile; a join proof is insufficient |
| Aggregate | Exact scalar domain, grouping and overflow/empty-input semantics | Admitted aggregate capability and emitted capacity checks; global, grouped and distinct counts need their own scope |
| Native graph projection | Reversible identities, complete cells and declared capability losses | Exact immutable release, engine/protocol version and independent native oracle |

Do not infer a wider capability from a successful example. The
[original commerce query receipt](../../docs/helix/04-build/evidence/commerce-publication-weft-20261009.json)
names the actual local source, compiler and runtime scope. It does not qualify
Databricks deployment, real Truss source authority or another engine. Preserve
source, ACK, table and file custody through opening, execution and closing checks;
refuse drift or missing authority instead of reading current table heads.
