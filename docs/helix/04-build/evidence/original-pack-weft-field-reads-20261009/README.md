# Original pack Weft field reads

Six original archaeology/ecology statements executed unchanged through the frozen
Weft compiler and publication reader on local Spark 4.0.1 / Delta 4.0.0. The run
took 65.62 seconds before final Spark cleanup, on one local worker with 512 MiB
driver memory. No cloud compute or scale benchmark was used.

| Pack | Original case | Exact native result bag |
| --- | --- | --- |
| Archaeology | missing-media | `[["[42,0,\"AS6\"]"]]` |
| Archaeology | specialists | `[["5","2","3","1"]]` |
| Archaeology | lineage | `[["[42,0,\"O1\"]","1"]]` |
| Ecology | match | `[["[42,0,\"SM1\"]"]]` |
| Ecology | network | `[["[42,0,\"R1\"]","[42,0,\"R2\"]"]]` |
| Ecology | fishing | `[["angler-hour"]]` |

Original graph identities, model versions and numeric meaning remain intact.
Every emitted scalar-integrity check ran before user SQL; the four integer
outputs additionally retained separate public UMF source-validity and native
capacity checks. Wrong-schema controls refused with six and one source violations
respectively, before user SQL. Each positive result has original opening/closing
ordinary PostgreSQL ACK observations and full publication-vector custody. Both
readers closed successfully, all 132 original Delta files remained unchanged,
and Spark stopped before evidence was written.

`report.json.gz` decompresses to the exact original 29,890,469-byte JSON report;
its uncompressed hash is in `fingerprints.json`. Compression removes repeated
receipt bytes from repository storage without changing the original evidence.
Requests and actual compiler artifacts remain ordinary JSON. The runtime copy
preloads the committed default 4 MiB custody implementation, isolating this run
from concurrent work on a separate medical capacity profile. The source freeze
and exact runtime/source/fixture fingerprints make that distinction explicit.

`runtime-run.py` records the original invocation; it is not directly runnable
from this evidence directory. To replay that invocation, stage a copy of it as
`run.py` beside a copy of `runtime-local_delta_custody.py` named
`local_delta_custody.py`, and select a fresh output directory in the staged
driver. Keep these archived originals unchanged. The explicit publication,
compiler, UMF, source-guard and existing runtime paths in that driver must be
available with their recorded hashes. This is reproduction of a private native
experiment, not a packaged setup command.

This qualifies these six private local reads only. The remaining eleven original
statements, later comparison/positional compiler changes, Unity Catalog, accepted
Truss integration, remote source authority and larger-scale performance are not
established by this run. The earlier authored-SQL baseline remains separate.
