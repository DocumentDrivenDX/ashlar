# Managed-current local native harness consolidation

The one-shot canonical pair in `tools/run_managed_current_local_native.py` and
`tools/run_managed_current_local_native_supervisor.py` is a reviewable
consolidation of the R61 attempt14 lifecycle. It pins the retained runtime
manifest, uses the original cursor-zero genesis for the A1 independent source
readback (while the continuation plan remains bound to its advanced admission
cursor), and writes the original cursor-zero source genesis, the separate phase admission genesis
(cursor zero for A0 and the advanced cursor for A1), source selection, request,
manifest, destination receipt, and vector before independent readback and ACK
checks.

This consolidated pair was not run as a whole and is not a new qualification
result. The historical source publication and recovery evidence is retained
separately; the exact superseded worker/supervisor iterations are copied
byte-for-byte under this directory and listed with SHA-256 and byte length in
[script-archive.json](script-archive.json). The successful retained A1 ACK
recovery is [recorded here](../managed-current-attempt14-a1-recovery/closeout.json).

The R61 run and recovery are local-development only. Delta readback is
path-based; neither establishes Databricks or Unity Catalog execution. The
recovery follows a source-hold gap and did not replay the source group or Delta
publication.
