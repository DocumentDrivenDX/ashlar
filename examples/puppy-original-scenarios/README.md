# Run the original domain scenarios in PuppyGraph

With Python 3.11 or later, install `pip install '.[puppy-gremlin]'` from the Ashlar
repository. The public
command runs all eight archaeology and nine ecology scenarios over their original
saved graph releases. It preserves their property equality joins, optional joins,
presence witnesses, ordering and exact signed64 values. Original SQL, native
translations, independent expectations and native responses remain separate views.

Select a clean UMF checkout at `07357eadbda9cf8299b9d72d004cbb77f8091aee`, with its
Bun dependencies installed. Select the unchanged ontology, fixture and pack files
under `examples/domain-packs/{archaeology,ecology}/upstream`. Each saved release
and custody pair needs its independently retained SHA-256 selectors, such as those
recorded by the original export command. Reading these saved pairs requires no
running Spark or Truss instance.

Save the following configuration with mode `0600` under an owned parent with mode
`0700`, replacing every placeholder.
Use absolute paths, an existing cached PuppyGraph image digest, available loopback
ports and a fresh output directory under an owned parent with mode `0700`.

```json
{
  "profile": "ashlar-original17-puppygraph-invocation/0.1",
  "packs": [
    {
      "name": "archaeology",
      "model": "/absolute/ashlar/examples/domain-packs/archaeology/upstream/ontology.json",
      "graph": "/absolute/ashlar/examples/domain-packs/archaeology/upstream/graph/fixture.json",
      "pack": "/absolute/ashlar/examples/domain-packs/archaeology/upstream/pack.json",
      "export": {
        "release_path": "/absolute/private/archaeology/release.graph.json",
        "custody_path": "/absolute/private/archaeology/custody.json",
        "release_sha256": "SELECTED_ARCHAEOLOGY_RELEASE_SHA256",
        "custody_sha256": "SELECTED_ARCHAEOLOGY_CUSTODY_SHA256"
      }
    },
    {
      "name": "ecology",
      "model": "/absolute/ashlar/examples/domain-packs/ecology/upstream/ontology.json",
      "graph": "/absolute/ashlar/examples/domain-packs/ecology/upstream/graph/fixture.json",
      "pack": "/absolute/ashlar/examples/domain-packs/ecology/upstream/pack.json",
      "export": {
        "release_path": "/absolute/private/ecology/release.graph.json",
        "custody_path": "/absolute/private/ecology/custody.json",
        "release_sha256": "SELECTED_ECOLOGY_RELEASE_SHA256",
        "custody_sha256": "SELECTED_ECOLOGY_CUSTODY_SHA256"
      }
    }
  ],
  "producer": {
    "source": "/absolute/clean-pinned-umf",
    "bun": "/absolute/path/to/bun",
    "git": "/absolute/path/to/git",
    "timeout_seconds": 30,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 4194304
  },
  "runtime": {
    "docker": "/absolute/path/to/docker",
    "docker_host": "unix:///absolute/path/to/docker.sock",
    "image": "sha256:SELECTED_CACHED_IMAGE_SHA256",
    "name": "ashlar-original-scenarios",
    "protocol": "Cypher",
    "http_port": 18081,
    "bolt_port": 17687
  },
  "credentials": {
    "username": "CHOOSE_A_LOCAL_USERNAME",
    "password": "CHOOSE_A_LOCAL_PASSWORD"
  },
  "output": "/absolute/private/fresh-original-scenarios",
  "bounds": {
    "native_seconds": 120,
    "whole_seconds": 300,
    "stdout_bytes": 1048576,
    "stderr_bytes": 1048576,
    "maximum_rows": 1000,
    "maximum_bytes": 1048576
  }
}
```

```sh
ashlar query-original-pack-puppygraph --configuration /absolute/private/scenarios.json
```

For Gremlin, set `protocol` to `Gremlin` and add `gremlin_port: 18182` and
`native_map_profile: "ashlar-puppygraph-1.13-flat-valuemap/0.1"` inside `runtime`.
Use a distinct third port and a fresh output and container name for each invocation.

The command validates both original sources through public UMF, prepares four
readonly DuckDB tables and starts one owned container capped at 2 CPUs and 3 GiB.
It waits for HTTP and Bolt readiness before submitting the model once. Authored queries
run once in the selected protocol. The runtime owns its readiness/query deadline;
the parent bounds the whole invocation, reserving 35 seconds for cleanup. The
whole ceiling must exceed that reserve. Row and byte bounds apply during capture.

Success returns the path and SHA-256 of `result.json` after source, file, query
client, process and container closing checks. Ordered schema observations refer
to exact response bodies stored once; every occurrence remains visible. GraphSON
wire frames remain under the private output for Gremlin. A failure retains partial
output and a separate private supervision directory; it returns no successful
command result. This workflow reads original finite files and immutable releases;
it grants no canonical publication, protected ACK or Unity Catalog authority.
