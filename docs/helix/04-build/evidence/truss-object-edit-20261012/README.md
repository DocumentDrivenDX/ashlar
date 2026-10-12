# Public Truss non-key object update

On 2026-10-12, the authored `examples/truss-native/note.umf.json` document was
accepted through the public schema example at catalog revision `2`. It is an
original UMF 0.7.0 document, not a converted domain pack or a change to an existing
accepted document. The original accepted report and catalog are retained here.

`edit-object.py` then created Note `4` at version `1` and updated its declared
non-key `note-text` to `Edited non-key note text`, reaching version `2`.
Full public readback preserved `note-id = ashlar-note-1`, native identity, type
and retained values. Both original requests returned committed receipt replays
with identical semantics and original receipt tokens. Complete public results
and Base64 evidence remain in `edit-readback.json`.

The one run completed in 5.761 seconds with exit 0 on the existing Ashlar-owned
synthetic PostgreSQL 18.6 cluster and Truss/psycopg 3.3.6/libpq 18.6 tuple. No
runtime install, role-membership changes, inventory or wider suite was involved.
Connections and the server closed before successful output; its postmaster marker
was absent afterward. This proves this document addition and non-key update,
not compatible evolution of an existing document, imports, feed/ACK, Weft or Delta.
