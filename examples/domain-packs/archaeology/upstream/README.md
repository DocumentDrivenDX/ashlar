# archaeology reference pack

Version 1.0.0. Authored synthetic fixtures only. Tables and ontology schemas are UMF-owned; TableSpec owns dataset replay/ingestion. Graph schema is a candidate for Truss/Ashlar binding, not tested native intake.

## Inventory

| Concept/table | Fixture rows | Authored fields |
| --- | --- | --- |
| projects | 1 | id, name |
| sites | 1 | id, project_id@projects, name |
| seasons | 1 | id, site_id@sites, excavation_year:INTEGER |
| areas | 1 | id, season_id@seasons, native_code |
| squares | 1 | id, area_id@areas, native_code, grid, datum |
| contexts | 2 | id, square_id@squares, native_locus, material |
| excavation_activities | 1 | id, context_id@contexts, recorded_at, action |
| stratigraphic_assertions | 2 | id, source_context_id@contexts, target_context_id@contexts, relation, author, evidence, status |
| find_lots | 1 | id, context_id@contexts, basket_code |
| objects | 1 | id, lot_id@find_lots, material, object_kind |
| samples | 2 | id, context_id@contexts, material, parent_id?@samples |
| analyses | 2 | id, sample_id@samples, method, analyst |
| pottery_results | 1 | id, object_id@objects, form, fabric, sherd_count:INTEGER, estimated_vessels:INTEGER, weight:DECIMAL, unit |
| soil_results | 1 | id, analysis_id@analyses, property, value:DECIMAL, unit, preparation |
| fauna_results | 1 | id, analysis_id@analyses, taxon, element, nisp:INTEGER, mni:INTEGER, method |
| observations | 1 | id, context_id@contexts, property, value, unit |
| interpretations | 2 | id, context_id@contexts, phase, earliest:INTEGER, latest:INTEGER, calendar, author, evidence, basis, supersedes_id?@interpretations |
| assets | 6 | id, kind, reference, availability, caption, scale, orientation, source_id?, revision?, sha256? |
| asset_subjects | 6 | id, asset_id@assets, context_id?@contexts, object_id?@objects, region |
| interpretation_evidence | 2 | id, interpretation_id@interpretations, pottery_result_id?@pottery_results, fauna_result_id?@fauna_results |
| stratigraphic_evidence | 2 | id, assertion_id@stratigraphic_assertions, asset_id@assets |
| provenance | 1 | id, project_id@projects, native_subject_id, publisher, native_reference, transformation |

## Scenario checks

- cycle: Keep contradictory stratigraphic assertions visible
- dating: Overlapping interpretations keep both authors and evidence
- media: One square drawing depicts two contexts
- missing-media: External assets are not falsely included
- specialists: NISP, MNI, sherds and vessels keep distinct denominators
- lineage: Find-to-context chain is retained
- evidence-links: Interpretations retain resolved specialist evidence alongside native source literals
- sample: Subsample lineage and soil preparation remain explicit

## Generation and limits

Use TableSpec sample-data replay with this pack. Small/demo/large are 1/10/100 independent components. Seed and component ordinal namespace identity strings; all foreign keys follow that mapping. This scales row count, not population validity, time span or network complexity. Original literals and templates remain preserved.

No third-party dataset or native-standard vocabulary is bundled. Ecology is a synthetic scientific-method fixture; archaeology is a synthetic context/evidence corpus with illustrative SVGs, not Madaba Plains records or actual excavation photos. External source selection remains separately governed.
