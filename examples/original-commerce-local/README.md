# Original commerce on an isolated local installation

These installed commands initialize a fresh local Delta installation and publish
Ashlar's unchanged original commerce source. They use the six reviewed
UMF-generated local physical carriers, public source admission and the existing
publication operator. This source interface has pure conformance controls;
installation and native behavior require qualification for the selected runtime.

Install Ashlar and its required publication dependencies in the explicitly selected
Python 3.11 environment. The isolated child must import the installed `ashlar_host`
package, PySpark 3.5.3, Delta Spark 3.2.1 and psycopg directly from that environment.
`PYTHONPATH` does not supply packages to the child. Select a compatible JDK 17 and
the publication profile's exact four-JAR directory. Compute SHA-256 values for the
selected interpreter, `JAVA_HOME/bin/java` and `JAVA_HOME/lib/jvm.cfg`; put those
values in the configuration. Set `JAVA_HOME` to that same absolute directory.

The separate private outbox fixture setup must already have created the explicit
loopback PostgreSQL database, ordinary source writer/reader and protected ACK
roles. Select its original-commerce scope and independently observed source
signature. The unused second fixture scope remains separate. No initializer or
publisher creates PostgreSQL resources, grants privileges or discovers
credentials. Remove ambient `PG*` environment settings before invoking these
commands. The packaged adapter refuses them and uses explicit loopback address,
empty password, `/dev/null` passfile, disabled SSL/GSS encryption and bounded
connection/statement waits. This is isolated development trust, not authenticated
production or Truss authority.

Create an initialization JSON file with exactly these fields:

- `profile`: `ashlar-original-commerce-local-initialization/0.1`.
- `root` and `output`: distinct, absent absolute paths with existing canonical
  parents; `installation_id`: the explicit installation identity.
- `model`, `graph`, `jars`: absolute paths to the unchanged original commerce
  inputs and selected publication JAR directory.
- `producer`: `source`, `bun`, `git`, `timeout_seconds`,
  `maximum_output_bytes`, `maximum_receipt_bytes`, selecting the public producer.
- `runtime`: `python`, `python_sha256`, `java_home`, `java_sha256`,
  `jvm_config_sha256`.
- `capacity`: `null`, or the exact supported operation-capacity object
  `{"profile":"ashlar-private-local-operation-capacity/0.1","max_intent_bytes":8388608}`.

Run the installed command with that selected interpreter:

```sh
/absolute/environment/bin/python -I -B -m ashlar initialize-original-commerce --configuration /absolute/initialize.json
```

Initialization refuses an existing root or output. It creates and checks six
empty version-zero Delta tables, captures their actual UUIDs, initializes the
public local journal and reservations, and writes `output/installation.json` only
after closing admission. The handoff records correspondence; it grants no source,
writer or publication authority. Preserve the original root, journal and handoff.
A failed invocation can leave partial native state and must not be treated as a
completed installation or silently retried over that state.

Create a publication JSON file with profile
`ashlar-original-commerce-local-publication/0.1`, the same `model`, `graph`, `jars`,
`producer` and `runtime`, a fresh publication `output`, the absolute `installation`
handoff path and its exact `installation_sha256`. Add `source` with exactly
`host`, `port`, `database`, `authentication`, `scope`, `source_schema`,
`source_signature_sha256`, `writer_role`, `reader_role`, `ack_role`.
`host` is `127.0.0.1`, database `truss_e2e`, authentication
`isolated-loopback-trust`; the three roles are distinct. `scope` contains exactly
`service_schema`, `scope_id`, `installation_id`, `consumer`, `feed`, `epoch`;
feed and epoch are `private-original-commerce-fixture` and `original-commerce-v1`.
Use the fixture's actual values, not inferred role naming conventions. The
source scope's `installation_id` must equal the retained handoff's
`installation_id`; an older installation scope is refused before publisher or
native effects.

```sh
/absolute/environment/bin/python -I -B -m ashlar publish-original-commerce --configuration /absolute/publish.json
```

Publication renews actual source role, privileges, signature and original bytes,
uses the canonical installation lock and current native reservations, and advances
protected ACKs after durable publication. Exact already-appended original source
bytes may be reused after a pre-native interruption. This initial-publication
command does not resume a partially written native installation. Existing query
and recovery commands never initialize tables or journals.

Both installed commands own a separate process session with a 300-second bound,
local one-worker Spark and 512 MiB driver selection. Timeout and cancellation
terminate and reap the owned process group. The parent renews configuration and
runtime custody before releasing a bounded result. Hard termination does not
claim that the killed child completed cooperative native closing checks. No
managed Unity Catalog, production PostgreSQL, Truss or cloud qualification follows
from this local interface.


For the four unchanged original commerce queries, install the independently
selected Weft 0.4.2 realization and use the separate query profile's Spark 4,
Delta 4 and complete JDK 21 runtime. The publication SDK above does not qualify
that query runtime. Invoke `query-commerce-implicit-arithmetic` with its explicit
configuration, provider path and provider SHA-256. That provider must return the
public ordinary protected-ACK factory and `CommerceQuerySourceFactory` bound to
the retained publication's exact installation, original scope, roles and source
signature. The query command never provisions a source or creates a native
installation; no provider-free query workflow is claimed here. Verify the actual
full four bags and closing source/native custody before qualifying that runtime.
A newly created fixture's protected-ACK installation identity must match this
new installation; an older fixture inventory cannot be silently reused.
