# Create records and their links together

`create-truss-record-group` submits one public Truss group containing all selected
records followed by their independent edges. Use it when a record and its required
relationship must exist together at commit. Truss validates the accepted source
and final group; Ashlar does not infer enforcement from an authored link.

```sh
ashlar create-truss-record-group \
  --configuration /absolute/private/group.json \
  --output /absolute/private/fresh-result.json
```

The configuration selects an ordinary caller connection, Bun, unchanged original
UMF document, revision, String origin map and request key. Keep credentials in the
private operator configuration. The closed shape is:

```json
{
  "profile": "ashlar-truss-record-group/0.1",
  "connection_uri": "OPERATOR_SELECTED_ORDINARY_CONNECTION",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/original.json",
  "document_revision": "ORIGINAL_REVISION",
  "document_id": "urn:umf:domain:commerce",
  "source_version": "0.8.0",
  "origin_path": "/absolute/private/origin.json",
  "request_key": "ORIGINAL_GROUP_REQUEST_KEY",
  "records": [
    {"alias": "supplier", "module_id": "domain", "record_id": "suppliers", "values": {"id": {"kind": "string", "text": "S1"}, "name": {"kind": "string", "text": "Synthetic supply"}}},
    {"alias": "product", "module_id": "domain", "record_id": "products", "values": {"id": {"kind": "string", "text": "P1"}, "supplier_id": {"kind": "string", "text": "S1"}, "sku": {"kind": "string", "text": "SKU1"}, "unit_price": {"kind": "decimal", "text": "12.50"}}}
  ],
  "edges": [
    {"alias": "supplier-link", "module_id": "domain", "relationship_id": "products.supplier_id", "source": {"state": "alias", "alias": "product"}, "target": {"state": "alias", "alias": "supplier"}, "values": {}, "order_key": {"state": "null"}}
  ]
}
```

The example selects the original commerce supplier/product fields and relationship.
Use an already accepted matching catalog and an ordinary authorized writer; the
selected keys must not already exist. Replace operator placeholders with the
actual paths, accepted revision and original request key. Property keys are the
unchanged declared Field `name`, not its element ID. This two-record example is
separate from the full seeded eleven-object fixture and has no independent native
pass claim. Values use the existing explicit scalar tags:
`{"kind":"string","text":"..."}`, integer/decimal lexical text with their
corresponding `kind`, `{"kind":"boolean","value":true}`, or `{"kind":"null"}`.
Datetime text stays in its original String field. No numeric float coercion occurs.
A stored endpoint instead uses `state: stored`, `document_id`, `module_id`,
`record_id` and positive integer `entity_id`; the host resolves its current public
identity rather than trusting the supplied ID. Text order keys use
`{"state":"text","text":"..."}`.

Configuration is capped at 64 KiB, with at most sixteen records and thirty-two
edges. All aliases must be unique; edge endpoints may reference record aliases,
not edge aliases. Source versions 0.7.0 and 0.8.0 must match the document unchanged.
Output is a fresh private report after complete public readback, committed receipt
replay and cleanup. The report preserves allocated native identities separately
from original authored values and aliases. This does not publish an Ashlar graph
or consume/acknowledge its feed. Source review and inert tests do not establish a
native commerce run. If commit is uncertain, retain the original configuration and
request key for reconciliation; the command performs no automatic retry.
