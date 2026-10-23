# R05 canonical materializer notes

The local materializer is a fixture-specific preparation helper for the frozen
five-node/four-edge source. It applies the checked UMF 0.5.6 `spark-delta4/0.1`
path realization under Spark 4.2.0, inserts only the frozen bounded rows, and
returns paths only after schema, row-count, writer-7, clustering-column and
statistics-property metadata checks pass. These checks cover the selected
observable fields; they do not establish every UMF realization disposition or
full `.4` compatibility. It does not start Spark and no native run is claimed.

Use a fresh private mode-0700 directory beneath the operator's temporary area.
The materializer holds `.ashlar-graphframes-action.lock` across DDL, writes, and
admission. If the process exits partway through, partial table directories can
remain and no successful paths are returned. Preserve the private root and use
the stable lock. Safe removal requires a future explicit owner cleanup path to
reacquire the lock and verify owned paths and Delta UUIDs. No cleanup helper is
qualified here. Do not remove the root or replace the lock inode while the
holder/action may be using it.

The three-row cap belongs to the separate R05 one-hop action descriptor; this
materializer does not execute or collect that action. No publication, source
progress, ACK, Unity Catalog, production authority, or scale qualification is
provided here.
