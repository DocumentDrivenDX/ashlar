# Original medical source admission

This evidence admits the original medical 1.1.0 CSV projection using public UMF compact dataset operations at `a95c3ec18a8f904decde884a4fa252988d2a5b0f`. The independent source oracle checks 17 retained resource files byte for byte, all 17 native reference pointers, original authored scalar types, and the distinct original absence versus CSV null observations. The public finite supplied dataset is valid and complete: 51 records, 51 keys and 62 relationship occurrences. Four changed inputs refuse: duplicate key, wrong boolean carrier, unresolved target and missing required relationships.

Original files are retained in [medical sources](../../../../../examples/domain-packs/medical/source-custody.json). The current CSV source profile `ashlar-medical-current-csv/0.1` explicitly converts canonical `true`/`false` only for authored Boolean Fields, retains integer tokens, keeps all authored Strings unchanged and maps the declared `\N` native null marker to present null. The original resource absence remains a separate observation. Date-looking, decimal-looking and JSON-valued String Fields acquire no additional semantics.

The retained graph belongs to original medical **1.0.0**, whose original pack bytes and archive are retained separately. Its three `True` boolean tokens are faithful normalized archive CSV lexical values. They require a separately selected, governed lexical conversion profile; this receipt does not admit or rewrite that graph. No storage, publication, ACK, Weft, native engine, clinical, terminology or broader FHIR validation is claimed.

Reproduce from the Ashlar repository:

```sh
PYTHONPATH=tools python3 -m unittest discover -s tests -p test_medical_source_oracle.py
bun tools/check_medical_current_dataset.ts /path/to/clean/umf-a95 docs/helix/04-build/evidence/medical-source-admission-20261009/current-input.json /fresh/output.json
```

The public checker requires the exact clean source revision, original copied source hashes and exact retained input bytes. Its output retains the complete original request and original public receipts for success and all four controls. `fingerprints.json` binds the copied evidence and owned tooling. All operations in this slice are local source/admission checks; no Spark or external service was used.
