# Full new-node extent: R651–R653

The private append node extent now contains 8M nodes, 8M retained raw records,
and 32M property events. All live rows match the independent local R645 oracle
in 240 complete-field SHA256 multiset groups, with no invalid-membership bucket.
R652 audits all 155 exact final native statements and 45 attributed blind appends.
The cryptographic collision assumption remains explicit.

The native phase took 924.378 seconds, read 18,847,786,864 bytes, wrote
17,559,968,444 bytes and recorded zero spill. It stayed within the conditional
R646 bounds of 80GB read, 35GB write and 2,700 seconds. These observations are
bootstrap append and verification costs, not publication freshness or billing caps.
The local full oracle took 681.757 seconds separately.

| Owned table role | Pinned version | Live rows | Files | Active bytes |
| --- | --- | --- | --- | --- |
| `append_node_object_current_r643` | 15 | 8,000,000 | 105 | 6,366,428,180 |
| `append_node_source_record_r643` | 15 | 8,000,000 | 124 | 6,549,369,968 |
| `append_node_property_journal_r643` | 15 | 32,000,000 | 123 | 5,843,885,272 |

The tables are in `client_dev.ashlar_entropy_20261006_r86`; exact UUIDs, history,
schemas and final profiles are in
[out/native/ashlar_append_node_growth_r651/audited-summary-r652.json](out/native/ashlar_append_node_growth_r651/audited-summary-r652.json).
The physical node table remains an 8M-node extent. An existing 8M-node table plus
this extent is not evidence of singleton performance on one 16M-node table.

Existing published nodes, edges, manifest and ACKs were untouched. The published
vector remains N6/E10/R7/J7/A8/T7; the existing edge physical head remains the
separately qualified maintenance head12. This experiment does not reconcile that
head for a new publication.

R654/R655 now verify the complete pinned old/new node union: 16M rows and
16M distinct non-null typed identities. Both endpoints of all 2.5M new edges
resolve (5M references, zero missing/null keys); the complete forward adjacency
multiset matches those edges. The read-only run took 25.531 seconds and read
447,899,478 bytes with zero writes/spill; all17 exact final statements are audited.
Pins are old nodes6/new nodes15/new edges0/new adjacency0. This establishes
scoped closure across two physical node extents, not a single16M-node table,
atomic publication, source fence or whole80M-edge graph.

Next grow the remaining new edge extent with independent local field oracles
and measured bounds.
No complete 16M/80M graph or billion-scale admission is established. The chosen
Unity Catalog Delta architecture, Truss semantics, parameterized native singleton
queries, deferred UMF binding and bounded engine mappings remain unchanged.
Warm/cold singleton, concurrency and publication freshness gates remain unproved
or failed in their recorded scopes; this milestone does not relax them.
