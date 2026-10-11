# Original commerce on an isolated local installation

Use the installed commands below to initialize a fresh six-table local Delta
installation and publish Ashlar's unchanged original commerce source. Start with
[initialize.template.json](initialize.template.json) and
[publish.template.json](publish.template.json): copy each to your chosen absolute
configuration path and replace every `REPLACE_WITH_…` value. The templates are
complete closed configurations; unfilled placeholders are intentionally invalid.
Do not add keys or substitute boolean/float values for integer limits.

Install Ashlar, PySpark 3.5.3, Delta Spark 3.2.1 and psycopg in the explicitly
selected Python 3.11 environment. The isolated child imports these installed
packages; `PYTHONPATH` cannot supply them. Select JDK 17 and the publication
profile's exact four-JAR directory, as specified in
[the indexed commerce runbook](../end-to-end/COMMERCE-INDEXED-WEFT.md).
Neither command installs or downloads dependencies. Set `JAVA_HOME` to the same
absolute directory selected in both configurations. Fill the lowercase SHA-256
values from the actual interpreter, `JAVA_HOME/bin/java` and
`JAVA_HOME/lib/jvm.cfg` bytes.

Select the unchanged original model/graph and the public UMF producer's source,
Bun and Git paths. Keep those selections and the runtime/JAR selections identical
between configurations. Choose separate, absent native root and initialization
output paths with existing canonical parents. The capacity object's
`max_intent_bytes` bounds each retained intent payload; it does **not** bound the
SQLite journal file. Preserve the selected capacity with the installation.

First provision the separately authorized isolated fixture using
[the private fixture guide](../private-outbox-original-commerce/README.md).
Copy its observed installation ID into the initialization configuration. Populate
publication `source` from that fixture's actual inventory: port, service/source
schemas, scope ID, consumer, original source signature and distinct writer,
reader and protected ACK roles. Keep the fixed loopback host, database,
authentication, feed and epoch shown in the template. Do not infer identities
from role naming conventions or reuse an older installation's scope. Remove
ambient `PG*` settings; the adapter refuses them. This connector uses isolated
loopback trust, empty password and `/dev/null` passfile with bounded waits; it
provides no production or Truss authority.

Run initialization with the selected installed interpreter:

```sh
/absolute/environment/bin/python -I -B -m ashlar initialize-original-commerce --configuration /absolute/initialize.json
```

Initialization checks all six generated schemas and version-zero empty tables,
then registers the public journal and reservations. It releases
`INITIALIZATION_OUTPUT/installation.json` only after closing admission. Preserve
that exact handoff, native root and journal. Compute the handoff's SHA-256 from
its original bytes and put its absolute path and digest in the publication
configuration. Its `installation_id` must equal the fixture scope's installation
ID. The handoff records correspondence and grants no current authority.
Choose a new absent publication output, then run:

```sh
/absolute/environment/bin/python -I -B -m ashlar publish-original-commerce --configuration /absolute/publish.json
```

Publication renews actual ordinary source privileges/signature/original bytes,
native registry/reservations and installation lock, and advances protected ACKs
after durable publication. It does not initialize tables, provision PostgreSQL,
create grants or discover credentials. Exact already-appended source bytes may
be reused after a pre-native interruption; this command does not resume partial
native publication. Preserve partial state on failure and do not silently reset
or retry over it.

Both commands own a separate process session with a 300-second bound, local
one-worker Spark and 512 MiB driver selection. Timeout/cancellation reap the owned
process group; hard termination does not prove cooperative native closing.
A native component success followed by evidence-retention failure remains a
failed whole invocation.

The four unchanged original queries use the separate
[implicit-arithmetic query guide](../../docs/helix/05-deploy/commerce-implicit-arithmetic-cli.md),
installed Weft 0.4.2, Spark 4, Delta 4 and JDK 21. Its explicit provider must return
current ordinary source and protected-ACK factories bound to this publication.
These setup templates do not supply that provider or qualify the four-query
runtime. Full four bags and closing source/native custody require separate
verification; no cloud or managed Unity Catalog claim follows.
