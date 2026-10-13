# Query an immutable graph release in PuppyGraph

Install the toolkit with `pip install '.[puppy]'` from the repository root.
Select the release JSON and custody receipt produced by `ashlar export-truss-graph`
(or another admitted immutable-export command), retaining both original SHA-256
values. No running Truss or Spark process is needed to query those retained files.

Save this configuration in a private file, replacing every placeholder:

```json
{
  "profile": "ashlar-local-immutable-puppy-corpus/0.1",
  "release_path": "/absolute/private/original.graph.json",
  "custody_path": "/absolute/private/original-custody.json",
  "release_sha256": "RELEASE_SHA256",
  "custody_sha256": "CUSTODY_SHA256",
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
ashlar query-puppy-immutable-export --configuration /absolute/private/puppy.json
```

The command validates the paired files before runtime effects, prepares their
complete canonical carriers, and compares seven Cypher queries with independent
original-node/edge bags. Those queries cover nodes, directed edges, singleton,
a bounded list, degrees, two-hop edge occurrences and path/distinct-target counts.
Separate `MATCH` clauses retain reuse of an edge across the two hop positions.
The source graph must be nonempty and have at most 100 nodes and 100 edges.

`result.json` is written only after query, custody and owned cleanup succeed.
On failure, retain partial output; no successful result is published. Property
JSON stays exact opaque text. This command does not qualify numeric property
filters, Gremlin, live source authority or source ACK operations.

The [installed-wheel check](../../docs/helix/04-build/evidence/truss15-puppy-immutable-startup-20261013/result.json)
passed exact carrier preparation, then refused at PuppyGraph 1.13.0 startup before
model upload. Successful native execution of this seven-query profile remains
pending; the owned container was removed.
