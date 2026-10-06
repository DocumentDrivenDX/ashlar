# Fixed-version concurrent reader screening

Eight CLI clients issued 256 singleton reads against serving VERSION AS OF 0 in
`client_dev.ashlar_ingest_20261005_a2`, retaining the original property value after
batches 1 and 2 had updated current state. All 256 returned the expected identity
and original exact property carrier. Query history contains all 256 reads and
reports zero result-cache hits. Descriptive engine p95 was 366ms; server total
p95 was 659ms. The provisional 100ms engine gate failed.

The reads overlapped stage-3 preparation. They did not overlap the subsequent
canonical/serving MERGEs: the ingest CLI POST timed out after 45 seconds, although
Databricks had completed the stage-3 statement in 14.776 server seconds (14.486
execution seconds). Statement `01f1c100-c31a-1f85-8cae-6bd31624eed2` was inspected
and recovered by the same handle; it was never resubmitted. The remaining
publication continued later. This interruption invalidates an uninterrupted
end-to-end freshness claim for that batch. It also motivates a persistent client
with recoverable request/statement identity before a production concurrency test.

No multi-table publisher recovery or continuous arrival was tested. This proves
only scoped old-version readability and uncached native read timing with eight
clients during staging on existing shared serverless Photon 2X-Small compute.
There is no billion-node scale admission or cold-data claim.

Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_ingest_20261005_a2/`
contains `concurrent-readers-ingest.json`, `timeout-history.json` and the recovered
statement record in `statements.jsonl`. No warehouse settings changed. Synthetic
isolated tables remain for inspection; cost attribution is not yet available.
