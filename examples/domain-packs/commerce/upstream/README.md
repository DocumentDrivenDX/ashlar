# commerce reference pack

Version 1.0.0. Authored synthetic fixtures only. Tables and ontology schemas are UMF-owned; TableSpec owns dataset replay/ingestion. Graph schema is a candidate for Truss/Ashlar binding, not tested native intake.

## Inventory

| Concept/table | Fixture rows | Authored fields |
| --- | --- | --- |
| customers | 1 | id, name |
| suppliers | 1 | id, name |
| products | 1 | id, supplier_id@suppliers, sku, unit_price:DECIMAL |
| orders | 1 | id, customer_id@customers, ordered_at, status |
| order_lines | 1 | id, order_id@orders, product_id@products, quantity:INTEGER |
| fulfillments | 2 | id, line_id@order_lines, quantity:INTEGER |
| invoices | 1 | id, order_id@orders, amount:DECIMAL, currency |
| payments | 1 | id, invoice_id@invoices, amount:DECIMAL, currency |
| returns | 1 | id, line_id@order_lines, quantity:INTEGER |
| refunds | 1 | id, return_id@returns, amount:DECIMAL, currency |

## Scenario checks

- partial-return: Partial shipment and return preserve remaining inventory
- fulfillment: Find over-fulfilled lines without repairing source quantities
- settlement: Compare exact payment, invoice and refund amounts
- refund: Return and refund use the original product price

## Generation and limits

Use TableSpec sample-data replay with this pack. Small/demo/large are 1/10/100 independent components. Seed and component ordinal namespace identity strings; all foreign keys follow that mapping. This scales row count, not population validity, time span or network complexity. Original literals and templates remain preserved.

No third-party dataset or native-standard vocabulary is bundled. Ecology is a synthetic scientific-method fixture; archaeology is a synthetic context/evidence corpus with illustrative SVGs, not Madaba Plains records or actual excavation photos. External source selection remains separately governed.
