# Use Ashlar with Truss

Ashlar accepts original UMF schemas through Truss, consumes committed source
changes into Delta, and queries an explicitly selected publication. Use Python
3.11 with the compatible Truss PostgreSQL runtime, Bun, JDK 21, Spark 4.0.1 and
Delta 4.0.0. Typed numeric workflows require Truss's main-source numeric support
after `v0.0.1`. Install Ashlar in that selected environment from the repository
root with `python -m pip install .`.

All paths below are absolute operator-selected paths. Replace the placeholders
with existing executables and JARs. Example outputs must be fresh private
locations; retain partial output if a command fails.

## Start with one typed record

Save `/absolute/private/record-example.json`:

```json
{
  "profile": "ashlar-local-truss-record-example/0.1",
  "bun": "/absolute/path/to/bun",
  "java_home": "/absolute/path/to/jdk21",
  "jars": ["/absolute/path/to/delta-spark.jar", "/absolute/path/to/delta-storage.jar"]
}
```

```sh
ashlar run-truss-record-example --configuration /absolute/private/record-example.json \
  --output /absolute/private/record-example
```

This creates a fresh Truss installation, accepts the bundled typed record model,
creates one record, publishes its catalog and object pages, and reads the named
singleton. The result preserves integer `9007199254740993`, decimal `1.2300`, a
Boolean value, and absent optional fields. Read `result.json` for allocated
`type_id` and `entity_id`; the saved `reader-configuration.json` selects the exact
publication.

## Add arithmetic or authored relationships

Two commands provide ready-made workflows with explicit compiler selection:

| Command | Additional configuration | Result |
| --- | --- | --- |
| `run-truss-arithmetic-example` | Profile `ashlar-local-truss-arithmetic-example/0.1`; `weft_index` and `weft_installation` for the installed public 0.4.2 distribution | Exact singleton plus integer addition and decimal multiplication/subtraction |
| `run-truss-typed-related-example` | Profile `ashlar-local-truss-typed-related-example/0.1`; `compiler` path to the pinned local candidate executable | Two typed records, R1→R2 and R2→R2 edges, exact singletons, forward/inverse key queries |

Each configuration also contains the same `bun`, `java_home` and `jars` selections
shown above. Pass it with `--configuration` and select a fresh `--output` directory.
Compiler installations and their runtime requirements are described in the
[detailed guide](README.md). The pinned local candidate currently requires
macOS 27.0.1 on arm64. Authored relationships retain their original endpoint
keys, direction, multiplicity and lifecycle. The relationship example explicitly
uses a bounded 2 MiB catalog/feed page limit.

For your own data, use `setup-truss-local`, `create-truss-record` and
`create-truss-edge`, then `publish-truss-graph`. Use `update-truss-record`,
`delete-truss-record`, `update-truss-edge`, `delete-truss-edge` and
`continue-truss-graph` for subsequent changes. Their exact
configuration shapes are in the [source command guide](README.md).

## Reopen and query a saved example

Keep this command running in one terminal:

```sh
ashlar serve-truss-local --data-directory /absolute/private/record-example/data \
  --connection-output /absolute/private/live-connection.json
```

In another terminal:

```sh
ashlar configure-truss-example-reader --example-directory /absolute/private/record-example \
  --reader-configuration /absolute/private/record-example/reader-configuration.json \
  --connection-configuration /absolute/private/live-connection.json \
  --output /absolute/private/reopened-reader.json
ashlar read-truss-record --configuration /absolute/private/reopened-reader.json \
  --type-id <allocated-type-id> --entity-id <allocated-entity-id> \
  --output /absolute/private/record-read.json
```

The reader helper preserves the selected publication and changes the ordinary
connection for that same installation. For a query-capable example, select its
saved reader configuration instead. `query-truss-scalars`,
`query-truss-scalar-join`, `query-truss-scalar-join-count`,
`query-truss-numeric-grouped-count` and `query-truss-typed-related-keys` provide
explicit typed query routes; `query-truss-arithmetic`, `query-truss-two-hop-count` and
`query-truss-two-hop-target-count` select the installed 0.4.2 compiler. The two-hop
routes distinguish complete path occurrences from distinct authored terminal key
tuples. Qualify fields with their record alias. Every route retains original
model/binding bytes, executes its compiler obligations, and checks complete
results under publication custody. See the [query examples](README.md) for the
supported SQL subsets and required options.

## Export the same graph to GraphFrames

While the source is running, export the selected publication:

```sh
ashlar export-truss-graph --configuration /absolute/private/reopened-reader.json \
  --output /absolute/private/export.json
```

The report contains immutable release and custody paths and SHA256 selectors.
Use its actual report digest to prepare a GraphFrames configuration. In a
separate environment with Ashlar, PySpark 3.5.3 and GraphFrames 0.12.3:

```sh
ashlar configure-graphframes-export --export-report /absolute/private/export.json \
  --export-report-sha256 <sha256-of-export.json> \
  --java-home /absolute/path/to/jdk21 \
  --jar /absolute/path/to/io.graphframes_graphframes-spark3_2.12-0.12.3.jar \
  --jar /absolute/path/to/io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar \
  --output /absolute/private/graphframes.json
ashlar run-graphframes-export --configuration /absolute/private/graphframes.json \
  --output /absolute/private/graphframes-result.json --maximum-hops 2
```

GraphFrames operates on the same immutable node and edge identities, with
one/two-hop occurrence and degree/count checks. Property JSON remains exact text;
typed numeric property filtering needs its own adapter. Exporting and querying
do not ingest or acknowledge new source data.

These local workflows use development authority and caller-asserted Truss ACK.
Their exact runtime and native query subsets are qualified by the linked
[execution evidence](README.md). Cloud deployment and production authorization
require their respective profiles.

After source changes, export the newly selected publication to a fresh location
and configure GraphFrames with that export's own release and custody pair. Keep
the earlier pair together when reading its snapshot. The
[edge lifecycle release example](../../docs/helix/04-build/evidence/truss-typed-edge-release-graphframes-20261013/result.json)
reads both snapshots after an endpoint update and edge deletion, preserving the
isolated node and exact property carriers. Mixed publication pairs refuse.
