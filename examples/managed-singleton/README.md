# Read an existing managed publication

Install the toolkit wheel with its `managed-read` extra in the selected Python
environment. The command reads one object from the last completed configured-JSONL
publication, using the canonical eight-table `ashlar-delta/0.3` layout.

Copy [the invocation template](configuration.template.json) to an operator-owned
file. Replace every placeholder and example identity, ordinal, port and path with
the actual selections. Hash the original installation, intake proof and source
configuration bytes; retain their original paths and journal/WAL state. Do not
copy or edit the source configuration or journal to fit another installation.

Select an already-running dedicated serverless SQL warehouse and the original
actor. Reader compute may differ from writer compute; original requests remain
unchanged. Select the existing ordinary pin-reader login and registered complete
vector. Its loopback client port may differ from PostgreSQL's observed server
port. The private credential file is referenced by path and must have mode 0600.
The command does not provision roles, grants, pins or compute.

The producer requires Bun, Git and the clean UMF source revision
`c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3`. This is a finite configured-source
development profile with full source parity checks, not a general graph query
or a production latency qualification. Keep `maximum_statements` at 128 and allow
the explicit cleanup reserve within the whole deadline.

```sh
python -m ashlar read-managed-singleton --configuration /absolute/operator/read.json
```

Successful termination prints a closed JSON result and retains the same result
in the fresh private output's `report.json`. An admitted missing object has
`row: null`. Bounded diagnostics and original admission evidence remain private.
Failure withholds the report; partial evidence alone does not establish success.
No publication or source ACK is advanced. The
[consumer contract](../../docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md#installed-managed-publication-singleton)
defines the authority and closing requirements.
