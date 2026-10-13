# Query an immutable graph release in PuppyGraph

Install the toolkit with `pip install '.[puppy]'` from the repository root.
Select the original report produced by `ashlar export-truss-graph` (or another
admitted immutable-export command) and its SHA-256. The report selects the original
release and custody files. No running Truss or Spark process is needed to query them.

Save these operator settings in a private file with mode `0600`, replacing every
placeholder. Release and custody selectors come exclusively from the export report:

```json
{
  "profile": "ashlar-local-immutable-puppy-corpus/0.1",
  "output": "/absolute/private/fresh-puppy-output",
  "docker": "/absolute/path/to/docker",
  "docker_host": "unix:///absolute/path/to/docker.sock",
  "image": "sha256:CACHED_IMAGE_SHA256",
  "name": "ashlar-immutable-example",
  "username": "YOUR_LOCAL_USERNAME",
  "password": "YOUR_LOCAL_PASSWORD",
  "http_port": 18081,
  "bolt_port": 17687,
  "deadline_seconds": 120
}
```

Use an existing cached PuppyGraph image digest and two free loopback ports.
The command creates one owned container with 2 CPUs and 3 GB memory, mounts the
prepared carrier read-only, and stops/removes that container on exit. It does not
pull images. Select a fresh output directory under an existing private parent.

```sh
ashlar configure-puppy-immutable-export \
  --export-report /absolute/private/export-report.json \
  --export-report-sha256 ORIGINAL_EXPORT_REPORT_SHA256 \
  --operator-configuration /absolute/private/operator.json \
  --output /absolute/private/puppy.json
ashlar query-puppy-immutable-export --configuration /absolute/private/puppy.json
```

Setup verifies the report digest and actual paired release/custody owner, then
writes a fresh private configuration after closing checks. It starts no engine
and grants no source authority. Keep both settings files private: they contain
the local engine password. The existing fully specified query configuration
remains valid for callers that already retain their selectors.

The command validates the paired files before runtime effects, prepares their
complete canonical carriers, and compares seven Cypher queries with independent
original-node/edge bags. Those queries cover nodes, directed edges, singleton,
a bounded list, degrees, two-hop edge occurrences and path/distinct-target counts.
Separate `MATCH` clauses retain reuse of an edge across the two hop positions.
The source graph must be nonempty and have at most 100 nodes and 100 edges.

`result.json` is written only after query, custody and owned cleanup succeed.
On failure, retain partial output; no successful result is published. Property
JSON stays exact opaque text. This command does not qualify numeric property
filters, live source authority or source ACK operations.

For Gremlin, install `pip install '.[puppy-gremlin]'`. Use the same configuration
with `profile` set to `ashlar-local-immutable-puppy-corpus/0.2`, add
`"language": "Gremlin"` and `"gremlin_port": 18182`, and select a third free
loopback port distinct from the HTTP and Bolt ports. The command remains the same.
The original 0.1 configuration continues to select Cypher.

Gremlin compares the same seven independent source bags, retaining parallel
edge identities and edge reuse. Only the distinct-target count deduplicates.
Both native projections verify complete canonical rows, identities and endpoints
before and after the corpus. Incoming GraphSON frames are retained before strict
decoding; duplicate keys and unsupported scalar families refuse. Count conversion
accepts exact signed 64-bit SDK values without changing graph identity strings.

The [installed-wheel check](../../docs/helix/04-build/evidence/truss15-puppy-immutable-cypher-20261013/result.json)
passed all seven Cypher cases over an original Truss 15 parallel-edge release,
using Python 3.12.15, DuckDB 1.5.0 and Neo4j 5.28.2 with the pinned PuppyGraph
1.13.0 image. All four ordered edge pairs, one distinct target and isolated-node
zero counts matched; the owned container was removed before success publication.
Readiness alone may retry temporary backend errors within the selected deadline.
Model submission and graph queries are never retried.

The [installed Gremlin check](../../docs/helix/04-build/evidence/truss15-puppy-immutable-gremlin-20261013/result.json)
passed the same seven cases using GremlinPython 3.7.3 and the same pinned image
and original release. Both language projections passed their opening and closing
checks, and the owned container was removed. Original incoming GraphSON frames
are retained in the evidence archive.
