# Actual local publication, source/ACK and current-query integration

The installed original-commerce workflow initialized a fresh six-table Delta
installation, published the unchanged UMF0.8 commerce source through ordinary
PostgreSQL source/ACK roles, and completed publication-backed COUNT and SKU
queries. The final query reports appeared only after query, full publication
reader, configuration and Spark closing succeeded.

## Observed results

- Initialization: exit0, 31.588 seconds; six distinct native table UUIDs retained
  in `installation.json` and reconciled with `publication-report.json.gz`.
- Publication: exit0, 48.538 seconds; 21 original events, 11 objects, 10 edges,
  21 history rows and an unchanged exact replay. This whole-operation duration
  includes setup, admission, replay and durable protected ACK, not ingest-only time.
- Both successful query runs returned COUNT `1` and SKU `SKU1`, the complete
  products bag from that original graph. Native refinement matched for both.
- Opening and closing ACK observations were byte-equivalent within each run,
  with position `1`, and their protected observation payloads also matched across
  runs. No query advanced the checkpoint or republished the source.
- Both runs retained the exact same original model, manifest and four-role
  publication vector/UUIDs. The six-table registry additionally includes attempts
  and manifest; it must not be confused with the four graph-data snapshot roles.

| Local observation | Full snapshot renewal (`9f8c58de`) | Original graph-read renewal (`4fd6248b`) |
| --- | ---: | ---: |
| Whole supervised process | 390.662s | 72.166s |
| Reader workflow including closing | 382.673s | 61.521s |
| COUNT staging | 28.765s | 3.421s |
| COUNT guarded execution/refinement | 111.169s | 6.771s |
| SKU staging | 29.116s | 2.210s |
| SKU guarded execution/refinement | 116.179s | 6.156s |

These are single sequential functional observations, not controlled benchmarks.
No raw SQL execution/fetch split or scale qualification is inferred. The renewed
path retains full opening/closing snapshot admission, fresh original native-byte,
identity/vector, source/ACK checks, and complete staged bag/schema/version checks.
It removes repeated full snapshot decoding between those boundaries, using the
existing original graph-read renewal port. It does not cache authorization.

## Reproduction and retained records

Follow [the publication query guide](../../../../../examples/current-query-local/publication.md)
and [the installed original-commerce setup](../../../../../examples/original-commerce-local/README.md).
`provenance.json` records the private retained run root, executed host revisions,
compiler hashes and exact SDK versions. Publication used Spark3.5.3/Delta3.2.1
and JDK17; queries used Spark4.0.1/Delta4.0.0, SQLite3.53.1 and JDK21.
Each Spark operation selected one worker and a 512MiB driver. The fresh isolated
PostgreSQL17.9 ARM fixture selected one CPU/512MiB and explicit loopback trust.

`publication-report.json.gz`, `installation.json` and `fixture-inventory.json` retain
the actual source/registry correspondence. `full-snapshot-query-report.json.gz` and
`renewal-query-report.json.gz` contain the exact requests, bindings, compiler
artifacts, conditional refinement results, original manifest/vector and actual
ordinary opening/closing ACK observations. Matching status files retain the
supervisor's terminal result. The gzip archives preserve exact original report bytes; raw and compressed hashes
are in `provenance.json`. These records are evidence, never runtime grants.

The first initialization attempt refused an executable hash changed during
runtime restoration. Its native root and output were confirmed absent before
the corrected invocation; the failed status is retained. An earlier query using
the original duplicate-check implementation timed out at330seconds and produced
no final report. Its status remains separate. Subsequent queries reused the same
publication/source/ACK without resetting or reseeding them; fresh staging
workspaces were selected. Only successful runs qualify complete closing.

## Limits

This proves the explicit original private-commerce source and required-String
query subset with a reviewed development Weft Spark feature from `c1973ece`.
Conditional reports correctly retain `authority:false`, `securityReady:false`
and required native admission. It does not prove production authentication,
UMF Subject/action/disclosure authorization, managed Unity Catalog, actual Truss
feed acceptance, other models or graph engines, or large-graph performance.
The retained source/ACK fixture and publication remain available for continuation.

No Databricks compute was launched for these operations. The USD25 owner cap
remains applicable to a future dedicated trial. Classic compute creation was
refused, SQL endpoint creation was denied, and read-only inspection found a
working serverless-jobs route. No serverless job was submitted because its total
cost bound was not established; budget alerts alone must not be represented as
a hard spending cap. The shared warehouse was untouched.
