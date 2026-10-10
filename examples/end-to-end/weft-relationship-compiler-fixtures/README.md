# Original-commerce one-hop compiler fixture

`request.json` contains the unchanged original UMF0.8 commerce model and a
separate authored `products.supplier_id` edge binding. Table UUIDs and publication
ID are explicitly synthetic unpublished test inputs. This is compiler and host
unit evidence, with no native publication, ACK or engine-query qualification.

`response.json` is original stdout from indexed candidate realization
`weft-362a9c6-ab899150-aarch64-apple-darwin-candidate`, executable SHA recorded in
`custody.json`. The complete compiler package manifest is retained unchanged.
The independent trusted distribution index remains owned by Ashlar's public
installer; this fixture does not install or authorize executable bytes.

Run portable host tests from the checkout:

```sh
PYTHONPATH=src:tools python3 -m unittest discover -s tests -p test_weft_relationship_host.py -v
```

All tests run without a compiler or engine. To additionally recompile the exact
request on the separately qualified indexed installation, set
`ASHLAR_RELATION_COMPILER` to its verified runtime executable. The optional replay
must produce byte-identical original stdout; it does not replace portable tests.

The copied public0.2 schemas are byte-identical to Weft commit362a9c6, as recorded
in custody. `schema-receipt.json` retains actual Ajv Draft2020-12 structural
validation; compiler/source semantics are separately evidenced by original
request and response. Parallel edge occurrence bags must not be deduplicated.
General relationship multiplicity versus distinct associated Record semantics
remains an explicit upstream qualification gap; two-hop traversal is not claimed.
