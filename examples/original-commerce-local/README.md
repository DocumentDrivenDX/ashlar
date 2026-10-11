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

## Install the fixed 0.4.2 compiler

Select the immutable Weft 0.4.2 index and its matching realization package. The
older compiler in the indexed commerce runbook is a different qualification;
it cannot substitute for this profile. Use a fresh installation output:

```sh
/absolute/query-environment/bin/python -I -B -m ashlar install-weft-implicit-arithmetic \
  --index /absolute/weft042/index.json \
  --package /absolute/weft042/realization \
  --output /absolute/fresh-weft042-installation
```

The installed host verifies the fixed realization and its closed schema and
manifest resources. Preserve the selected index, package and installation bytes.

## Configure and run the four original queries

Prepare a separate installed Python 3.11 query environment with PySpark 4.0.1,
Delta Spark 4.0.0 and psycopg 3.2.13. Its Python SQLite library must be 3.53.1;
installing a virtual environment does not replace the interpreter's SQLite
library. Select the full JDK 21 distribution, including `jdk.incubator.vector`,
and the exact two regular Delta 4 JARs listed in the indexed runbook. Keep JDK 17
and the publication SDK separate. Select runtime packages through this installed
environment, not an inherited `PYTHONPATH` or another Python's binary extensions.
New wheel or SDK bytes require their own qualification.

Copy and complete [query-provider.template.py](query-provider.template.py) as
explained in [provider.md](provider.md). Its three constants select the exact
retained publication configuration, that file's SHA-256 and the private fixture
container name. Compute the completed provider's SHA-256 after editing it. The
provider uses the original registration's reader callback and protected ACK
`metadata_factory`; it provisions nothing. The actual reader must still renew
ordinary roles, signature, source bytes, protected ACK scope, retained journal,
reservations and native UUID/version custody.

Copy [query.template.sh](query.template.sh) to an operator-owned path and fill
every placeholder. Select the same original model, graph, pack, clean public UMF
source and publication used above. Set the observed fixture endpoint, fixed
compiler installation, provider path/digest and a new absent query output.
The template deliberately closes inherited connection/runtime environment
settings and selects JDK 21 and the query interpreter explicitly. It invokes the
actual public flags; the query command has no JSON `--configuration` profile.

Run the completed template through an explicitly selected finite process
supervisor. The query CLI starts Spark directly; it does not provide the
initialization/publication commands' separate 300-second child supervisor.
The qualified local query run used a 300-second native bound, one local Spark
worker and a 512 MiB driver selection. Preserve stdout, stderr and terminal
status, and reap only the process group you created on timeout or cancellation.
A successful report requires all four complete result bags, unchanged original
compiler SQL, fresh compiler correspondence, reader closure, Spark shutdown and
closing source/native/JAR/invocation checks. Intermediate cases are provisional.

The [four-query evidence](../../docs/helix/04-build/evidence/original-commerce-local-query-20261011-d/README.md)
qualifies partial return, fulfillment, settlement and refund on this finite local
original source. Its independently checked initialization and publication are
linked from the receipt. The result does not qualify arbitrary new runtime
bytes, general interrupted recovery, production authentication, managed Unity
Catalog, cloud or real Truss integration. On failure preserve the output and
native state; diagnose the owning refusal before choosing a fresh query output.
Never reset, reappend or republish to repair a query.
