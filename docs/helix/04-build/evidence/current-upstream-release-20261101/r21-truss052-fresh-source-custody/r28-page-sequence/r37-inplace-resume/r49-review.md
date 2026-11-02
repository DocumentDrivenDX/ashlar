# R49 in-place recovery packet

R49 is a bounded continuation of the committed R42 publication using the retained R36 PostgreSQL source and six-role Delta destination. It never replays publication effects. R48 is preserved as a supervisor-only sandbox refusal: `/bin/ps` returned `EPERM` before worker launch; its attempt root is absent and both child logs are empty. The R48 read-only reconciliation records the exact unchanged journal, Delta heads, stopped PGDATA and no-ACK state. R49 worker validates those immutable R48 receipts and reconciled values before any recovery work.

Effect-free preflight passed through the actual R37 supervisor and R49 worker. It verified runtime/JAR admission, recovery input shape, archived page digest, stopped PGDATA and read-only SQLite journal. It opened no PostgreSQL connection, started no Spark process, wrote no Delta data and attempted no ACK. Both R49 output and supervisor roots were absent after preflight.

The parent must dispatch this exact scope through `require_escalated`; R48 demonstrated that the supervisor's `/bin/ps` census is denied in the sandbox. R49 has no dispatch receipt and remains pending Sol review. Bounds remain one attempt, 900 seconds, 2 GiB process RSS, 512 MiB aggregate monitored tree, no cloud, no automatic retry, and no cleanup of retained source/evidence.

Scope, worker, supervisor and preflight receipt hashes are recorded in `r49-effect-free-preflight.json`. R48 refusal and reconciliation are hash-bound by the R49 scope and revalidated by the worker.
