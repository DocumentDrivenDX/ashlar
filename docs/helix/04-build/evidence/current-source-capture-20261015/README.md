# Local pinned source capture

A single Spark 4.0.1 / Delta 4.0.0 run captured two eligible rows from three
local rows in 15.219 seconds, using one core and a 512 MiB driver. The capture
retained signed64 extrema, Unicode, NUL and empty String values, unioned repeated
physical value columns and read the original Delta version after a later update.
An undersized population budget refused. Spark closed before success was saved.

`run.py` retains the exact invocation script and explicit existing runtime/JAR
paths; `capture.json` and `summary.json` retain its observations. The binding is
an independently authored source-capture fixture, not an accepted CQP binding.
Its local schema digest is a test observation, not a claimed Weft schema-digest
convention. This run qualifies local capture only. It does not establish managed
Unity Catalog caller, UUID/schema correspondence, policy, publication or source
and action authority. Those remain the held authority owner's responsibilities.

The final iteration additionally tightens aggregate structural-cell and JSON
capacity limits; focused unit controls cover these limits without repeating the
native run.
