# Payments source

Convert the pinned original UMF 0.8 payments model and graph to a whole-entity
source transaction with explicit development bindings:

```sh
ashlar payments-source --ontology /absolute/path/ontology.json \
  --graph /absolute/path/graph/fixture.json --source-system selected-payments \
  --output /absolute/private/payments-candidate
```

Select the bundled [ontology](upstream/ontology.json) and
[graph](upstream/graph/fixture.json). The fresh output contains `source.jsonl`,
`bindings.json`, and byte-exact `ontology.json` and `graph.json`. All 13 objects
and 14 edges retain their original identities, composite keys, endpoints, currency
text and decimal tokens, including exponent notation. Original rows remain in
retained content.

Before publication, a host must hold and validate the original inputs through
`FinitePackDefinition("payments")` and the public UMF dataset boundary, then
establish its source and writer authority. Candidate bindings describe
correspondence; they grant no authority. The [public admission evidence](../../../docs/helix/04-build/evidence/payments-public-source-20261013/result.json)
records complete dataset validation and three invalid-dataset controls.

The bundled [pack](upstream/pack.json) retains its financial scenario SQL for
separate compiler and execution admission. Exact source preservation does not
establish settlement arithmetic or currency-conversion enforcement.
