# Original Truss commerce indexed settlement attempt

The reviewed original-commerce adapter is on main as
`68c72121721665b4417c59d23234255815bdb7ca`. Thirty-one focused tests and the
module boundary check pass; all six exact-commit CI jobs pass. Independent review
covered bindings, the corrected pure oracle, held source/capacity execution,
CLI closing tests and command documentation. These are source-level results.

The fresh private wheel SHA-256 is
`2a7220b19db816ab2f006e8f78534f2a94571ec67bcbf55b70d2632bcb5cc4a6`;
existing SDK and compiler dependencies were reused. Attempt A launched before
wheel installation completed and failed at import, before source/Spark opening.
Attempt B used the completed installation and reached local Spark startup, but
its 60-second supervisor killed the owned group. It produced no query report or
native pass. The retained captures do not locate the remaining elapsed time; no unchanged
retry followed.
The exact owned PostgreSQL instance was then stopped normally, preserving data.
No source mutation, publication or ACK command was invoked. Deadline termination
does not prove cooperative query closing or an unchanged final checkpoint.

The bounded archive retains both exact scripts, logs and terminal supervisor
results, installed source hashes and the before-checkpoint. It omits large reused
binaries and source/publication archives; their paths and wheel hash are in the
installed custody record. `inventory.json` hashes every archived capture.
The promised native commerce query qualification remains outstanding.
