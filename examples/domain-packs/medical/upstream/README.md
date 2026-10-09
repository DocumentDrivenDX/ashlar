# Medical family 1.1.0

A bounded official HL7 FHIR R4 4.0.1 sample corpus using the same domain-pack
contract and TableSpec schema profile as the legal pack. UMF owns this metadata,
the schemas and export tooling. TableSpec owns row ingestion, archive generation,
loading and data tests. This pack has external CSV row bindings and no generator
or population scale presets; fixture counts describe the published corpus.

There are 17 original resources: two patients, two practitioners, three
organizations, one encounter, eight observations and one medication. The resource
index and reference projection bring the tabular representation to eight tables.
Original JSON bytes are retained both in `sources/` and `resources.resource_json`.
Resource keys use `ResourceType/id`. Quantities and clinical date/time literals
retain source spelling; coded values and units are not converted. The selected
references resolve locally; the reference schema also represents unresolved
references with a null target, without invented resources.

Export the consumer snapshot through the shared tool:

```sh
bun scripts/export-domain-pack.ts --pack spec/domain-packs/medical/pack.json --output /path/to/tablespec/examples/medical --include-sources
```

Add `--check` to verify exact snapshot bytes. In TableSpec, use
`tablespec sample-data ingest --pack examples/medical/domain-pack.json --output /tmp/medical.zip`.
The shared CLI validates pinned inputs and all tabular constraints before output.
Archive manifests map both schema and source references to their ZIP members.

Redistribution declarations are specific to the selected examples. HL7-owned
content is CC0; embedded LOINC/UCUM content carries separate notices. See
`licenses/NOTICE.txt`, source metadata and the governing implementation evidence.
This subset excludes restricted terminology. CMS sources are reference-only
with uncleared redistribution status; their rows are not bundled. These examples
are illustrative, not a validated clinical simulation or FHIR Bundle.

## LOINC notice

This material contains content from LOINC (http://loinc.org). LOINC is copyright
© Regenstrief Institute, Inc. and the Logical Observation Identifiers Names and
Codes (LOINC) Committee and is available at no cost under the license at
http://loinc.org/license. LOINC® is a registered United States trademark of
Regenstrief Institute, Inc.

## Composable medical subpacks

The clinical source corpus is unchanged in medical 1.1.0. Independently versioned siblings add
[carrier workflows](../medical-carrier/README.md),
[epidemiological aggregates](../medical-epidemiology/README.md),
[imaging/PACS metadata](../medical-imaging/README.md) and
[terminology references](../medical-terminology/README.md).
Each has its own pack metadata, local source pins, schemas, row bindings and rights
notices. Select/export each explicitly; this list does not authorize automatic
retrieval, patient matching or merging population records with individuals.

## Complete family export and browser

The Medical overview links Clinical, Carrier, Epidemiology, Imaging and Terminology.
Each 1.1.0 pack has fixed tabular targets and a table-derived ontology; combined
inventory: 29 tables, five ontology schemas and 517 projected rows. Carrier adds
13 unchanged CMS DE-SynPUF CSV records beside retained FHIR R4 resources. Imaging
adds one unchanged CC BY 3.0 TCIA LIDC-IDRI CT slice beside the private-sequence
synthetic control. Source provenance, selected releases and limitations stay local.

Export all five packs, including permitted original CSV/FHIR/DICOM bytes:

```sh
bun scripts/export-domain-family.ts --pack spec/domain-packs/medical/pack.json --output /tmp/medical-family --include-sources
```

Use `--check` to verify that export. The output keeps ID/version directories and
`family.json`; ingest each `domain-pack.json` separately through TableSpec. Composition
never merges patients or creates links between clinical, CMS and TCIA sources.
Previous 1.0.0 browser bookmarks redirect to the current qualified subset.
