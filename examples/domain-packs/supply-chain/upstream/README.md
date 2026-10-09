# supply-chain reference pack

Version 1.0.0. Authored synthetic fixtures only. Tables and ontology schemas are UMF-owned; TableSpec owns dataset replay/ingestion. Graph schema is a candidate for Truss/Ashlar binding, not tested native intake.

## Inventory

| Concept/table | Fixture rows | Authored fields |
| --- | --- | --- |
| organizations | 1 | id, name |
| facilities | 1 | id, organization_id@organizations, name |
| products | 1 | id, sku |
| lots | 1 | id, product_id@products, lot_code |
| items | 2 | id, lot_id@lots, serial |
| containers | 3 | id, parent_id?@containers, kind |
| shipments | 2 | id, container_id@containers, facility_id@facilities, status |
| source_events | 1 | id, publisher, native_id |
| events | 2 | id, shipment_id@shipments, upstream_event_id@source_events, event_time, action |
| sensor_readings | 2 | id, container_id@containers, value:DECIMAL, unit, method, upper_limit:DECIMAL |
| containment | 2 | id, item_id@items, container_id@containers |
| shipment_items | 2 | id, shipment_id@shipments, item_id@items |

## Scenario checks

- split-excursion: A split lot reaches two shipments and exposes temperature excursion
- excursion: Compare temperature against explicit upper limit
- replay: Expose duplicate upstream event identity
- lineage: Trace serialized item through lot and nested container
- sensor: Keep method and temperature unit with measurements

## Generation and limits

Use TableSpec sample-data replay with this pack. Small/demo/large are 1/10/100 independent components. Seed and component ordinal namespace identity strings; all foreign keys follow that mapping. This scales row count, not population validity, time span or network complexity. Original literals and templates remain preserved.

No third-party dataset or native-standard vocabulary is bundled. Ecology is a synthetic scientific-method fixture; archaeology is a synthetic context/evidence corpus with illustrative SVGs, not Madaba Plains records or actual excavation photos. External source selection remains separately governed.
