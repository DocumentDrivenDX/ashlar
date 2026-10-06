# Catalog-managed singleton comparison

Fifty measured composite source/type/id lookups per layout, cache-disabled
persistent SQL driver, existing shared 2X-Small warehouse. A new isolated
catalog-managed 16MiB liquid-id table is copied from the ordinary 1M-object
fixture and optimized. Full outer-join properties/retained equality passed;
returned full carriers match for every lookup pair. Both shapes read one file
at the median; all 100 measurements have zero result-cache hits.

| Layout | Engine median / p95 ms | Compile median / p95 ms | Server p95 ms | Caller median / p95 ms |
| --- | --- | --- | --- | --- |
| Ordinary Delta | 80 / 93 | 97 / 116 | 203 | 284 / 325 |
| Catalog-managed Delta | 111 / 158 | 91 / 139 | 273 | 314 / 371 |

Catalog commits did not improve singleton timing in this fixture; the catalog
variant fails even the 100ms engine screening gate. Do not adopt it as a read
latency optimization based solely on advertised metadata benefits. Its scoped
atomicity evidence remains useful for publication, but coupling transaction
storage and the selected singleton path needs further measurement. Fifty samples
and one source/type do not establish universal overhead, scale or connector
compatibility. The source tables have distinct physical files, feature sets and
names; this is a practical candidate comparison, not isolation of every internal
catalog service effect.

Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_catalog_lookup_20261005_g2/`,
107 completed statement metrics. Harness `native_catalog_lookup.py`; regenerate
with `summarize_client_reads.py ashlar_catalog_lookup_20261005_g2`. No warehouse
or workspace preview setting changed. Synthetic tables remain isolated.
