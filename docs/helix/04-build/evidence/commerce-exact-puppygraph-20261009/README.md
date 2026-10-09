# Original commerce exact PuppyGraph scenarios

The actual local PuppyGraph 1.13.0 Cypher and Gremlin runs each passed all four original commerce scenario bags: partial return `[[6,2,6]]`, fulfillment `F2`, settlement `PAY1`, and refund `RF1`. Both first checked every original full carrier (11 nodes, 10 edges), native identity/incidence, and all unfiltered partial subtract/add and refund multiply intermediates against the independent original source oracle. The retained reports contain the unchanged submitted queries and native results.

The named `ashlar-commerce-exact-graph-arithmetic/0.1` profile uses signed64 quantities and signed64 money coefficients with original authored scale 2. These are explicitly finite analytic representations; original unbounded Integer and Decimal meanings, complete raw carriers and lexical tokens remain unchanged. Public UMF source admission precedes representation admission. All candidate intermediates are checked before driver acquisition, and native integer intermediates precede scenario filtering. No float or Gremlin math operation participates. Tests cover capacity excess, exact tokens including negative zero, missing/rounded intermediates, and withheld results after closing failure.

`receipt.json` binds the original immutable release/custody, model/graph/CSV inputs, public admission and sealed DuckDB projection. `model.json` and activation receipts retain the complete prior-plus-addition request and observed node/edge inventories. The catalog inventory is omitted by the native API; this is explicitly recorded rather than described as actively verified. Prior original-commerce, R1/R2 and authored shared fixtures were replayed after the addition. All four sealed database hashes matched before and after both scenario runs. The dedicated container used two CPUs, 3 GiB and loopback endpoints, and was stopped afterward.

The final native b reports include opening/closing image, ports, resource bounds and complete sealed database inventory. A subsequent narrow checker correction additionally requires and compares the nonempty Docker container ID; these existing reports predate that correction and do not claim to exercise it. Closing-custody regression tests cover replacement/change refusal. No repeat native activation was needed. The initial Gremlin attempt refused static Object access; the retained successful query uses explicit Vertex/Long casts.

This evidence qualifies the original finite fixture only. It does not establish broad full-signed64 native arithmetic boundaries, general Decimal promotion, a canonical publication/ACK for this analytic projection, Unity Catalog, or production serving authority.

Run the local checks from the repository root:

```sh
PYTHONPATH=src:tools python -m unittest discover -s tests -p test_commerce_exact_puppygraph.py
```

The two tools expose explicit input and trusted-digest arguments through `--help`. Native execution requires the retained sealed databases and the dedicated existing PuppyGraph instance; it never provisions a cloud resource. `runtime.json` identifies the actual host database and dependency versions. `fingerprints.json` binds this frozen slice; original report bytes are retained unchanged.
