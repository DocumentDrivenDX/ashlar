# Fresh local Truss bootstrap and restart

On 2026-10-12, `examples/truss-native/setup-local.py` bootstrapped a fresh owned
local database using the existing installed Truss package, PostgreSQL 18.6 and
psycopg 3.3.6/libpq 18.6. Public APIs installed the catalog, accepted the original
authored Note schema, and configured ordinary reader/writer access. Installation
`60acba05-8eb5-4452-9fa6-bde318999bdd` reached catalog revision `1`.

After setup stopped its server, the public `LocalPostgres` owner restarted the
same data directory. The current connection URI was obtained from that owner;
none was cached in the bootstrap receipt. The ordinary `ashlar_note_consumer`
account ran `edit-object.py`, creating Note `1` and updating its non-key text to
version `2` with exact readback and committed receipt replays.

The one setup/restart/edit run completed in 20.448 seconds with exit 0. Both
server intervals closed, and the postmaster marker was absent afterward. No
download, package build, shared server, runtime inventory or wider suite was used.
Original public bootstrap/report and edit results are retained in `bootstrap.json`
and `edit-readback.json`. Credentials and connection URI are excluded.

This proves this fresh local setup and restart workflow. Remote provisioning,
schema evolution, imports, feed/ACK, Weft and Delta integration remain outside
this result and are still required where specified by the delivery plan.
