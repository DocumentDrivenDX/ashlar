# Original historical medical graph admission

The original medical 1.0.0 graph now passes actual public UMF compact dataset admission at reviewed and landed `8e76c74d14203225d1ef159c9132bb9e9b0cdffe`: 51 supplied records, 51 keys and 62 occurrences, valid/complete. The explicit caller selects `umf.csv-boolean-lexical/1.0.0` for only the three original `True` Boolean cells. Their exact token/source-row/Field and original public receipts are retained; the independent archive CSV oracle binds that unverified context to retained original bytes. One Boolean null and all eight native null cells remain present null. No implicit profile activation or generic coercion occurs.

The independent oracle reconstructs every 51 full source-value row, exact source primary-key identity and 62 directed FK occurrences from the own normalized archive CSV and authored TableSpec schemas, then compares complete bags to the original graph. Original 1.0 pack/archive/model/graph bytes remain unchanged and distinct from current 1.1. The core model has no authored revision: original graph producer uses owning pack 1.0 as `graph.schema.revision`. This convention is disclosed and retained, never added to the core document. Integer source tokens and authored String date/decimal/JSON text remain exact; no clinical/FHIR/terminology reinterpretation follows.

Four changed public dataset inputs refuse: duplicate key, wrong Boolean carrier, unresolved target and missing required relationships. These are separate changed-input controls, not rewritten originals. This slice is source admission only: no native storage, publication, ACK, query runtime or canonical/native identity claim.

Reproduce from Ashlar:

```sh
PYTHONPATH=tools python3 tools/medical_historical_oracle.py > /fresh/original-oracle.json
bun tools/check_medical_historical_dataset.ts /path/to/clean/umf-8e76 /fresh/original-oracle.json /fresh/public-receipt.json
PYTHONPATH=tools python3 -m unittest discover -s tests -p test_medical_historical_oracle.py
```

The checker requires the exact clean public revision and independent original oracle bytes. The fingerprints bind all owned source/evidence. Upstream Python support CI 37997804496 and microsite CI 37997804444 both completed successfully for that public revision.
