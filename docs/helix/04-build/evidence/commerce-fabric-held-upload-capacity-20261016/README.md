# Held commerce Fabric upload: capacity refusal

One fresh candidate attempted an actual native OneLake upload while using the
existing active FTL4 trial workspace. The finite run exited 1 after
84.258854333 seconds, before graph creation, initial-load observation, admission,
activation, or the user GQL query. No retry or paid-capacity workaround was made.

The original first directory PUT returned HTTP **503**, with exact bounded body
retained as base64 in `onelake-operation-refused.json`. Its native error code is
`RejectedDueToResourceConstraints`: the organization’s Fabric compute capacity
had exceeded its limits. This is an external capacity refusal, not a completed
source/ACK, engine, loaded-graph, or query qualification. The prior attempt A’s
lost native response remains unknown; this response does not establish its status.

`trial-capacity.json` retains the actual pre-upload workspace/capacity readback;
`selection.json` selects the fresh candidate and paths. The read-only closing
local journal observation confirms candidate state `recorded`, null model and
operation handle, active revision zero, no first-load handle, and no admission.
It does not prove absence of partial remote directory effects. Do not replay the
failed PUT or reuse the candidate as a fresh submission.

The runner used immutable committed Ashlar source
`0ea486c15249906703701044446a50b5756ea5b7`; `source-selection.json` retains the
archive hash and 432 file pins. The selected existing SDK environment was Python
3.12.14, Spark 4.0.1, Delta 4.0.0 and psycopg 3.2.13; the runner selects local[1]
and 512 MiB. The supervisor bounds work to 700 seconds plus at most 20 seconds
of process-group cleanup. The copied scripts preserve the exact invocation and
preflight. Original logs remain private at paths pinned in `log-pins.json`;
their hashes are retained here without copying the native stderr stream.

No source/ACK submissions, capacity changes, Databricks compute, or paid resources
were requested by this workflow. Native observation stopped at the refused upload.
