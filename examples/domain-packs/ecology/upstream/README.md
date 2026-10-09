# ecology reference pack

Version 1.0.0. Authored synthetic fixtures only. Tables and ontology schemas are UMF-owned; TableSpec owns dataset replay/ingestion. Graph schema is a candidate for Truss/Ashlar binding, not tested native intake.

## Inventory

| Concept/table | Fixture rows | Authored fields |
| --- | --- | --- |
| watersheds | 1 | id, name, geometry, crs |
| catchments | 1 | id, watershed_id@watersheds, area:DECIMAL, area_unit |
| reaches | 2 | id, catchment_id@catchments, native_framework |
| network_links | 1 | id, upstream_id@reaches, downstream_id@reaches |
| monitoring_sites | 2 | id, reach_id@reaches, native_id, publisher |
| projects | 1 | id, name |
| methods | 3 | id, name, matrix, fraction |
| observed_properties | 3 | id, name |
| sampling_events | 5 | id, site_id@monitoring_sites, project_id@projects, method_id@methods, event_time |
| samples | 4 | id, event_id@sampling_events, matrix |
| observations | 6 | id, sample_id@samples, property_id@observed_properties, value?:DECIMAL, threshold?:DECIMAL, unit, result_kind, qualifier |
| taxa | 1 | id, name, rank, concept_version |
| identifications | 1 | id, taxon_id@taxa, confidence, life_stage |
| effort | 2 | id, event_id@sampling_events, amount?:DECIMAL, unit, method |
| occurrences | 3 | id, event_id@sampling_events, identification_id@identifications, effort_id@effort, count:INTEGER, detection |
| habitat_metrics | 1 | id, catchment_id@catchments, value:DECIMAL, unit, basis, algorithm |
| management_interventions | 1 | id, reach_id@reaches, action, event_time |
| fishing_events | 1 | id, site_id@monitoring_sites, effort:DECIMAL, effort_unit, catch_count:INTEGER |
| site_matches | 1 | id, site_id@monitoring_sites, candidate_native_id, evidence, resolution |

## Scenario checks

- effort-event: Occurrence and sampling effort share actual survey event
- connected-measurements: Two connected reaches retain temperature and dissolved oxygen with probe methods
- censor: Below-detection nitrate is not zero
- match: Coordinate-only match stays unresolved
- effort: Exclude effort-unknown detection from quantitative comparison
- zero: Known-effort non-detection is distinct from missing occurrence
- network: Reach linkage preserves upstream direction
- comparability: Matrix, fraction and taxonomy rank are retained
- fishing: Catch effort does not become scientific survey effort

## Generation and limits

Use TableSpec sample-data replay with this pack. Small/demo/large are 1/10/100 independent components. Seed and component ordinal namespace identity strings; all foreign keys follow that mapping. This scales row count, not population validity, time span or network complexity. Original literals and templates remain preserved.

No third-party dataset or native-standard vocabulary is bundled. Ecology is a synthetic scientific-method fixture; archaeology is a synthetic context/evidence corpus with illustrative SVGs, not Madaba Plains records or actual excavation photos. External source selection remains separately governed.
