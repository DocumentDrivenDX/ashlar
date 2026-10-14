# Full original commerce fixture through Truss, Delta and Weft

The pinned original UMF 0.8 commerce graph fixture's eleven objects and ten edges
passed one public Truss atomic group, complete typed readback and committed replay.
The original ontology and fixture bytes were unchanged. Their exact selections,
released Truss 0.0.22 wheel, installed Ashlar wheels, compiler and local runtime
are in `origins.json`. The prior [catalog acceptance](../truss22-commerce-catalog-20261013)
separately establishes the actual accepted catalog and allocation IDs.

The first installed workflow stopped after its catalog publication because the
next public page was 1,180,407 bytes: the CLI selected a 2 MiB limit but the old
core parser admitted only 1,125,000. Its 46.283-second failed attempt is retained.
The page's hash and 274-byte cursors were valid; a public read-only capture
preserved the complete pending page without acknowledging it.

After the reviewed parser/archive framing fix, existing publication
`truss-graph-1` continued to
`truss-graph-ce9a7e185f33f545e6cd30bdda650ed240bfb4db4fa86d4165c654e370d4567f`
in 41.195 seconds. This reused the exact pending page, journal, source epoch and
consumer: no repeated create group, reset, new registration or replacement source.
The completed continuation retains the full native effect-parity report and four
table versions. The original source plan bytes hash to that report's recorded
digest. Publication completed before the exact source-retained receipt ACK;
the source advanced to the original page's next cursor. Restarted source reads
retained that exact checkpoint.

The product singleton completed in 4.809 seconds. The outer continuation harness
then failed an overly strict lexical assertion, so its exit code is **1**, not an
end-to-end success. Truss's original typed history event carries decimal text
`12.5`; the singleton preserves exactly `12.5`. The original authored fixture and
group readback retain `12.50`. These interfaces have distinct original byte views
with equal exact decimal values. Neither source nor result tokens were rewritten
to hide that distinction.

Only the remaining read-only query then ran, in 11.417 seconds (12.112-second
successful supervisor):

```sql
SELECT p.unit_price AS price FROM products p
```

Its original compiler artifact selected declared precision 18, scale 2 and
returned `{"price":{"decimal_token":"12.50"}}` on the completed publication.
The emitted numeric-domain guard returned zero violations; original request and
response strings, hashes, complete publication pins and source closing checks
are retained. Before/after raw source checkpoints match, including the checkpoint
captured before the earlier singleton. No source ACK or publication ran in this
query attempt.

`original-captures.tar.gz` preserves complete original source pages, group/replay,
continuation, singleton, compiler protocol, source plan, logs, supervisors and
harnesses; `capture-inventory.json` identifies every member's original size/hash.
The private 39 MiB journal, source database and wheel binaries remain outside Git;
the exact selected source-plan row is retained without exporting the whole journal.
Spark used local[1]/512 MiB and Delta 4.0.0. There were no cloud runs or scale tests.

This qualifies the full original fixture's atomic source creation/replay, local
Delta graph publication, durable source-retained receipt ACK, singleton and one
required decimal projection. It does not qualify the remaining integer, join,
traversal/count or settlement/refund gallery, commerce updates/deletes/evolution,
protected production ACK/retention, Unity Catalog, graph engines or all packs.
Both unsuccessful outer attempts remain visible; their completed public stages
are qualified separately from the final successful query.
