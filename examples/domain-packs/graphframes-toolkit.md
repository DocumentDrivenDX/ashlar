# Query original graph releases with GraphFrames

Use an explicitly admitted original archaeology or ecology release. The host
selects either a live complete-publication hold or the retained
`ashlar-private-local-graph-custody/0.1` export profile. The retained profile requires
both independently trusted digests and its original opening and closing protected
ACK observations. It does not assert that the canonical Delta snapshots remain
available. A missing or malformed current retention inventory cannot silently
select a different authority mode.

Install Ashlar into the host environment. Provision a **separate Python 3.11
environment** with `pyspark==3.5.3`, `delta-spark==3.2.1`, and
`graphframes-py==0.12.3`, and install the same Ashlar source revision into that
environment using a copied regular executable (for example `python3.11 -m venv --copies`). The selected JAR directory must contain exactly the four named JARs
and SHA256 values in `ashlar_host.graphframes_release.JARS`. Select the exact
regular Python executable and `JAVA_HOME/bin/java` bytes by SHA256. The host
uses isolated Python module selection (`-I -B -m`) and passes the environment explicitly; it does not discover an active Spark session
or change a canonical Spark 4 / Delta 4 installation.

The selected operator module exports:

```python
# Trusted operator code; byte selection is not source or lease authorization.
def open_graphframes(invocation, release, custody_bytes):
    # Return an owned context yielding exactly GraphFramesOfflineSession or
    # GraphFramesPublicationSession, matching invocation.mode.
    # The source is an actual FinitePackSource with current independent policy
    # and a genuine selected public dataset producer. Close every owned source,
    # policy/transport and runtime context before returning normally.
    ...
```

For retained mode, supply `GraphFramesOfflineSession` with the original release,
receipt and trusted receipt digest. Its policy exposes `writer(context)` and
`admit_original_export(release, custody_bytes, source, context)`. Both session types also require `runtime_policy` and `runtime_context`. Admission returns
`None` only after independently admitting the original source, publication vector,
protected ACK scope and exact export. The owner renews that admission and original
source bytes throughout the worker interval and at closing.

For live mode, supply `GraphFramesPublicationSession` with the actual executor,
backend, complete `PinVector`, graph policy, original source, publication ID,
roles, supported profiles/revisions and finite row capacities. Its independently
admitted `outer_hold(context)` remains open through graph reads, the isolated
worker, child reaping and closing renewal. A returned `GraphRelease` alone does
not extend a publication hold.

The runtime policy exposes `admit_graphframes_runtime(config, context)`, returning
an exact `GraphFramesRuntimeInventory`, and
`renew_graphframes_runtime(inventory, config, context)`, returning `None` only
while actual runtime custody remains valid. It independently inspects the selected
interpreter's actual installed Ashlar worker resolution, complete SDK distributions
and transitive import dependencies, including zero-byte files and path-selection
mechanisms. It must hold and renew the complete import inventory, refuse new or
substituted members, and bind it to the explicitly selected worker revision and
inventory digests. Inventory hashes are correspondence; they do not grant runtime
or source authority. Ashlar independently hashes every listed regular file at
opening, each renewal, and closing. The SDK inventory has the three named
SDK entries plus `transitive-imports`; each entry contains its full selected file
closure. The provider closes its actual runtime custody context with all other
owned resources.

Use absolute operator-selected paths in a closed JSON invocation:

```json
{
  "profile": "ashlar-graphframes-invocation/0.1",
  "mode": "protected-ack-export",
  "pack": "/srv/ashlar/archaeology/upstream/pack.json",
  "release": "/srv/ashlar/exports/archaeology/release.graph.json",
  "release_sha256": "<independently trusted original release SHA256>",
  "custody": "/srv/ashlar/exports/archaeology/custody.json",
  "custody_sha256": "<independently trusted original custody SHA256>",
  "output": "/srv/ashlar/runs/archaeology-graphframes-001",
  "runtime": {
    "python": "/srv/graphframes/bin/python-real",
    "python_sha256": "<selected executable SHA256>",
    "java_home": "/srv/java",
    "java_sha256": "<selected bin/java SHA256>",
    "worker_revision": "<selected installed Ashlar source Git revision>",
    "worker_inventory_sha256": "<SHA256 of canonical worker file descriptors>",
    "sdk_inventory_sha256": "<SHA256 of canonical SDK and transitive file descriptors>",
    "jars": "/srv/graphframes/jars",
    "timeout_seconds": 300,
    "maximum_stream_bytes": 1048576,
    "maximum_evidence_bytes": 16777216
  }
}
```

Live configuration uses `"mode": "live-publication"` and omits the four retained
release/custody fields. Run the installed command after selecting the exact operator module bytes:

```sh
ashlar query-graphframes \
  --configuration /srv/ashlar/operators/archaeology-graphframes.json \
  --provider /srv/ashlar/operators/graphframes_provider.py \
  --provider-sha256 '<independently selected provider SHA256>'
```

The same operation is available through the public installed owner:

```python
from ashlar_host.graphframes_cli import run_graphframes_command

report = run_graphframes_command(
    "/srv/ashlar/operators/archaeology-graphframes.json",
    "/srv/ashlar/operators/graphframes_provider.py",
    "<independently selected provider SHA256>",
)
```

Run ecology with its unchanged original pack and a separate fresh output. The
complete corpus contains archaeology's eight and ecology's nine original cases.
GraphFrames relational operations preserve their authored property-equality joins,
LEFT absence/present-null distinctions and occurrence bags. They do not replace
those joins with traversals or execute repaired Weft SQL. Each run also requires
all objects, all independent edges, one hop, two hops, grouped occurrence and
distinct destination counts, singleton, filtered limit, and isolated-node parity.

The worker materializes a distinct immutable local Delta 0 carrier vector with
its own observed UUIDs, versions and protocol features. It never downgrades or
relabels the original canonical vector. Complete native schemas and rows are
retained before semantic comparison. Capacity overflow, observer refusal, source
or runtime drift, and cleanup failures withhold the final report. Original raw
observations remain available for diagnosis. A successful final report follows
child cleanup and all outer host contexts closing successfully.
