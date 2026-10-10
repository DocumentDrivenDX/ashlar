# Publish and query the original commerce graph with installed Ashlar

Install the reviewed Ashlar wheel, publish its unchanged UMF 0.8 commerce fixture,
and query that publication through the indexed Weft compiler. The installed
`publish-commerce` and `query-commerce` commands own the host workflow; no Ashlar
checkout or `tools/` imports are needed at runtime.

The [installed workflow evidence](../../docs/helix/04-build/evidence/installed-commerce-native-20261010/manifest.json)
records one fresh publication and three queries: a String projection, an exact
global count, and the authored product-to-supplier `RELATED_KEYS` projection.
It uses a private PostgreSQL outbox and local Delta with explicit development IDs.
The compiler is `weft-362a9c6-ab899150-aarch64-apple-darwin-candidate`, for the
observed Apple Silicon/macOS 27.0.1 profile, with `weft-compile/0.2.0` and
`weft-backend/0.2.0`. Its immutable bytes do not include the later distinct-neighbor
degree correction. Keep this recipe on the original single-edge commerce fixture.
Other platforms and compiler realizations need their own evidence.

## Select explicit resources

Supply the reviewed wheel as a local file. Its filename and version alone are
insufficient to select these bytes:

| Wheel | SHA-256 |
|---|---|
| `ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl` | `57a13eb1a53edaf08b1d5f22e342e8f30a6665d1844e1b3178e0f7125f85ca07` |

Use separate native SDK environments for publication and queries:

| Resource | Publication | Query |
|---|---|---|
| Python | 3.11; observed 3.11.17 | 3.11; observed 3.11.17 |
| Python's SQLite library | observed 3.53.1 | exactly 3.53.1, checked by the host |
| PySpark | 3.5.3 | 4.0.1 |
| delta-spark | 3.2.1 | 4.0.0 |
| graphframes-py | 0.12.3 | not required |
| psycopg / psycopg-binary | 3.2.13 | 3.2.13 |
| JDK | 17.0.9 | 21.0.12.1 |

GraphFrames 0.12.3 requires Python 3.10 or later. The query journal requires the
observed SQLite read-only WAL behavior; creating a virtual environment does not
change the base interpreter's SQLite library. Select an appropriate Python build
before proceeding. The recorded run used one Python interpreter with separately
selected SDK search paths and the installed wheel. The two-environment setup below
is a relocatable recipe for the same declared profile, not a claim that arbitrary
new dependency installations have already passed native qualification.

Replace every `/absolute/path/...` value below. `ASHLAR_WORK` is a user-owned
working directory outside an Ashlar checkout. The environment and output paths
must be fresh. Bun, Git and Docker must be installed; Docker must be reachable on
`PATH`. Native SDKs and JARs are explicit prerequisites, not wheel dependencies.

```sh
export ASHLAR_WHEEL=/absolute/path/to/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl
export ASHLAR_PUBLISH_BASE=/absolute/path/to/python3.11
export ASHLAR_QUERY_BASE=/absolute/path/to/python3.11-with-sqlite-3.53.1
export ASHLAR_WORK=/absolute/path/to/commerce-run
export ASHLAR_PUBLISH_ENV=/absolute/path/to/commerce-publish-env
export ASHLAR_QUERY_ENV=/absolute/path/to/commerce-query-env
export ASHLAR_JAVA17_HOME=/absolute/path/to/jdk-17.0.9
export ASHLAR_JAVA21_HOME=/absolute/path/to/jdk-21.0.12.1
export ASHLAR_DELTA3_JARS=/absolute/path/to/commerce-publish-jars
export ASHLAR_DELTA4_JARS=/absolute/path/to/commerce-query-jars
export ASHLAR_BUN=/absolute/path/to/bun
export ASHLAR_GIT=/absolute/path/to/git
```

Prepare the two environments from the exact wheel. A locally rebuilt wheel or a
new SDK environment requires its own qualification, even with the same version:

```sh
unset PYTHONPATH PYTHONHOME SPARK_HOME
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
"$ASHLAR_PUBLISH_BASE" - <<'PYWHEEL'
import hashlib, os
from pathlib import Path
raw = Path(os.environ['ASHLAR_WHEEL']).read_bytes()
assert len(raw) == 186993
assert hashlib.sha256(raw).hexdigest() == '57a13eb1a53edaf08b1d5f22e342e8f30a6665d1844e1b3178e0f7125f85ca07'
PYWHEEL
"$ASHLAR_PUBLISH_BASE" -m venv "$ASHLAR_PUBLISH_ENV"
"$ASHLAR_QUERY_BASE" -m venv "$ASHLAR_QUERY_ENV"
"$ASHLAR_PUBLISH_ENV/bin/python" -m pip install "$ASHLAR_WHEEL" pyspark==3.5.3 delta-spark==3.2.1 graphframes-py==0.12.3 'psycopg[binary]==3.2.13'
"$ASHLAR_QUERY_ENV/bin/python" -m pip install "$ASHLAR_WHEEL" pyspark==4.0.1 delta-spark==4.0.0 typing-extensions==4.15.0 'psycopg[binary]==3.2.13'
mkdir "$ASHLAR_WORK"
cd "$ASHLAR_WORK"
export ASHLAR_PUBLISH_RESOURCES="$("$ASHLAR_PUBLISH_ENV/bin/python" -c 'from ashlar_host.resources import RESOURCE_ROOT; print(RESOURCE_ROOT)')"
export ASHLAR_QUERY_RESOURCES="$("$ASHLAR_QUERY_ENV/bin/python" -c 'from ashlar_host.resources import RESOURCE_ROOT; print(RESOURCE_ROOT)')"
```

Each phase uses its own installed copy of the same exact packaged inputs. The installed host also carries its
SQL resources and public-validator script. The model SHA-256 is
`51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a`;
the graph SHA-256 is
`8052dbcf2cf48a5e344c492d661872bc0a3f80a5c7be033ab5008dea9c86119e`.
The host refuses changed inputs. Do not put another checkout's `src` or `tools`,
or another Python version's binary extensions, on either runtime's search path.

## Supply dedicated JAR directories

Each directory must contain exactly the listed regular files, with no symlinks or
extra entries. The installed host checks filenames and SHA-256 before starting
Spark and again before releasing a successful report.

Publication directory, four files:

| File | SHA-256 |
|---|---|
| `io.delta_delta-spark_2.12-3.2.1.jar` | `088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499` |
| `io.delta_delta-storage-3.2.1.jar` | `4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7` |
| `io.graphframes_graphframes-spark3_2.12-0.12.3.jar` | `69d7909628caf42bcbe1c7e6e5a5a0901040d00583738d931af41bd72af1d3dc` |
| `io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar` | `8bbd2bbb4c7a3b137e51b4f32f49a30a2ccf70eeb07b452cffa20090dd481b96` |

Query directory, two files:

| File | SHA-256 |
|---|---|
| `delta-spark_2.13-4.0.0.jar` | `538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e` |
| `delta-storage-4.0.0.jar` | `9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d` |

## Prepare the public validator and indexed compiler

Clone the pinned UMF source and install its locked dependencies. Both installed
commands invoke its public dataset validator against a clean checkout; supplied
validation flags cannot replace this step. Weft installation verifies a locally
supplied realization against Ashlar's trusted index.

```sh
"$ASHLAR_GIT" clone https://github.com/DocumentDrivenDX/umf.git "$ASHLAR_WORK/umf"
"$ASHLAR_GIT" -C "$ASHLAR_WORK/umf" checkout --detach c7c95e1c4ea5b72541f47fa0350ca467ff02f395
(cd "$ASHLAR_WORK/umf" && "$ASHLAR_BUN" install --frozen-lockfile)
"$ASHLAR_GIT" clone --filter=blob:none --no-checkout https://github.com/DocumentDrivenDX/weft.git "$ASHLAR_WORK/weft-distribution"
"$ASHLAR_GIT" -C "$ASHLAR_WORK/weft-distribution" sparse-checkout set distributions
"$ASHLAR_GIT" -C "$ASHLAR_WORK/weft-distribution" checkout 41a42ed5fc3c780ce900c61144733ad93359dc76
"$ASHLAR_QUERY_ENV/bin/python" -B -m ashlar install-weft \
  --index "$ASHLAR_WORK/weft-distribution/distributions/index.json" \
  --package "$ASHLAR_WORK/weft-distribution/distributions/realizations/weft-362a9c6-ab899150-aarch64-apple-darwin-candidate" \
  --output "$ASHLAR_WORK/weft-installation"
```

The trusted index SHA-256 is
`71e441ce80b36a50cb8fc668431fefddec71e5e83908eb7fe78abe40c41ba2de`.
There is no operator checksum or platform override. See the
[installation guide](WEFT-INSTALLATION.md) for package verification and transport
outcomes. This workflow uses the indexed compile/0.2 realization; the separate
Paths/0.4 compiler and its evidence are not selected here.

Check the selected Python and SDK versions without starting Spark:

```sh
"$ASHLAR_PUBLISH_ENV/bin/python" - <<'PYPUBLISH'
import importlib.metadata as md, sqlite3, sys
assert sys.version_info[:2] == (3, 11)
for name, version in {'pyspark':'3.5.3', 'delta-spark':'3.2.1', 'graphframes-py':'0.12.3', 'psycopg':'3.2.13', 'psycopg-binary':'3.2.13'}.items():
    assert md.version(name) == version, name
print('Publication Python:', sys.version.split()[0], 'SQLite:', sqlite3.sqlite_version)
PYPUBLISH
"$ASHLAR_QUERY_ENV/bin/python" - <<'PYQUERY'
import importlib.metadata as md, sqlite3, sys
assert sys.version_info[:2] == (3, 11)
assert sqlite3.sqlite_version == '3.53.1'
for name, version in {'pyspark':'4.0.1', 'delta-spark':'4.0.0', 'psycopg':'3.2.13', 'psycopg-binary':'3.2.13', 'typing-extensions':'4.15.0'}.items():
    assert md.version(name) == version, name
print('Query Python:', sys.version.split()[0], 'SQLite:', sqlite3.sqlite_version)
PYQUERY
"$ASHLAR_JAVA17_HOME/bin/java" -version
"$ASHLAR_JAVA21_HOME/bin/java" -version
```

## Prepare the private PostgreSQL substrate

Provide the dedicated PostgreSQL 17.9 container named `ashlar-e2e-truss-pg17`,
labeled `ashlar.purpose=end-to-end-development`, with one CPU, 512 MiB memory,
and only `127.0.0.1:15432` published to container port 5432. Its database is
`truss_e2e`. Docker Desktop must be running. The installed commands do not provision
the container. For a fresh substrate, this block creates it with a generated
password without printing that password; an existing container is checked first.

```sh
"$ASHLAR_PUBLISH_ENV/bin/python" - <<'PYPOSTGRES'
import json, os, secrets, subprocess, time
name = 'ashlar-e2e-truss-pg17'
probe = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True, timeout=10)
if probe.returncode:
    subprocess.run([
        'docker', 'run', '-d', '--name', name,
        '--label', 'ashlar.purpose=end-to-end-development', '--memory', '512m', '--cpus', '1',
        '-p', '127.0.0.1:15432:5432', '-e', 'POSTGRES_PASSWORD', '-e', 'POSTGRES_DB=truss_e2e',
        '--health-cmd', 'pg_isready -U postgres -d truss_e2e',
        '--health-interval', '2s', '--health-timeout', '2s', '--health-retries', '15', 'postgres:17.9'
    ], env=dict(os.environ, POSTGRES_PASSWORD=secrets.token_urlsafe(32)),
       check=True, capture_output=True, timeout=120)
else:
    c, = json.loads(probe.stdout)
    assert c['Config']['Image'] == 'postgres:17.9'
    assert c['Config']['Labels'].get('ashlar.purpose') == 'end-to-end-development'
    assert c['HostConfig']['Memory'] == 536870912 and c['HostConfig']['NanoCpus'] == 1000000000
    if not c['State']['Running']:
        subprocess.run(['docker', 'start', name], check=True, capture_output=True, timeout=30)
for attempt in range(30):
    c, = json.loads(subprocess.check_output(['docker', 'inspect', name], timeout=2))
    assert c['NetworkSettings']['Ports'].get('5432/tcp') == [{'HostIp':'127.0.0.1', 'HostPort':'15432'}]
    assert not c['State']['OOMKilled']
    state = c['State'].get('Health', {}).get('Status')
    if state == 'healthy':
        break
    if state == 'unhealthy':
        raise SystemExit('Private PostgreSQL health check failed')
    time.sleep(1)
else:
    raise SystemExit('Private PostgreSQL did not become healthy')
print('Dedicated private PostgreSQL is healthy')
PYPOSTGRES
```

The host obtains the dedicated container's credentials internally, then establishes
ordinary source-specific sessions. Retain the generated publication's private
installation material locally; do not publish it. This development outbox does not
establish real Truss catalog, feed or production authority.

## Publish, stop, then query

Run these phases sequentially. Each Spark process uses `local[1]`, a 512 MiB driver,
one shuffle partition and UTC. The explicit producer limits below bound public
validation to 60 seconds, 1 MiB per output stream and a 4 MiB receipt; they do not
set a deadline for the whole Spark phase. Use fresh output directories for each
new attempt and preserve failed reports, journals and native files for inspection.

```sh
JAVA_HOME="$ASHLAR_JAVA17_HOME" SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$ASHLAR_PUBLISH_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$ASHLAR_PUBLISH_ENV/bin/python" \
"$ASHLAR_PUBLISH_ENV/bin/python" -B -m ashlar publish-commerce \
  --model "$ASHLAR_PUBLISH_RESOURCES/ontology.json" --graph "$ASHLAR_PUBLISH_RESOURCES/graph.json" \
  --umf-source "$ASHLAR_WORK/umf" --bun "$ASHLAR_BUN" --git "$ASHLAR_GIT" \
  --producer-timeout-seconds 60 --producer-maximum-output-bytes 1048576 \
  --producer-maximum-receipt-bytes 4194304 \
  --postgres-container ashlar-e2e-truss-pg17 --postgres-host 127.0.0.1 \
  --postgres-port 15432 --postgres-database truss_e2e \
  --output "$ASHLAR_WORK/publication" --jars "$ASHLAR_DELTA3_JARS" \
  --source-system private-original-commerce-fixture \
  --binding-profile ashlar-commerce-development-bindings/0.2
```

Wait for exit zero and `$ASHLAR_WORK/publication/report.json`. Check that the
publication retained 11 objects, 10 edges and 21 history rows with no tombstones,
and that its full table UUID/version vector, public admission and protected source
ACK agree. Confirm the publication Spark process has stopped before querying.
The query needs the same live PostgreSQL substrate and original publication files:

```sh
JAVA_HOME="$ASHLAR_JAVA21_HOME" SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$ASHLAR_QUERY_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$ASHLAR_QUERY_ENV/bin/python" \
"$ASHLAR_QUERY_ENV/bin/python" -B -m ashlar query-commerce \
  --model "$ASHLAR_QUERY_RESOURCES/ontology.json" --graph "$ASHLAR_QUERY_RESOURCES/graph.json" \
  --umf-source "$ASHLAR_WORK/umf" --bun "$ASHLAR_BUN" --git "$ASHLAR_GIT" \
  --producer-timeout-seconds 60 --producer-maximum-output-bytes 1048576 \
  --producer-maximum-receipt-bytes 4194304 \
  --postgres-container ashlar-e2e-truss-pg17 --postgres-host 127.0.0.1 \
  --postgres-port 15432 --postgres-database truss_e2e \
  --output "$ASHLAR_WORK/queries" --jars "$ASHLAR_DELTA4_JARS" \
  --index "$ASHLAR_WORK/weft-distribution/distributions/index.json" \
  --installation "$ASHLAR_WORK/weft-installation" \
  --publication "$ASHLAR_WORK/publication"
```

## Inspect results and retain custody

A successful command prints its report path; a host refusal exits with status two
and a bounded refusal message. No successful report is released until the reader
holds, closing checks and Spark cleanup required by that phase have completed.
Do not treat exit status alone as retained publication or query evidence.

The query report keeps the compiler requests/artifacts, guard results, source
oracles and closed intervals. Expected values retain their original lexical
String carriers:

| Query | Expected result |
|---|---|
| `SELECT p.id FROM products p` | `{"id":"[42,0,\"P1\"]"}` |
| `SELECT COUNT(*) AS n FROM products p` | `{"n":"1"}` |
| `SELECT p.id, RELATED_KEYS(p."products.supplier_id", 2) AS suppliers FROM products p ORDER BY p.id` | `{"id":"[42,0,\"P1\"]","suppliers":{"items":[["[42,0,\"S1\"]"]],"truncated":false}}` |

The observed installed run preserved all 77 original publication files. The
closing inventory additionally contained a 32,768-byte SQLite shared-memory
sidecar and an empty WAL file. Record such additions separately from unchanged
original files. Retain original files, journals, installation credentials and index
custody together. Replacing a journal or choosing a fresh directory does not recover
an uncertain earlier attempt.

After both commands have exited and their Spark processes have stopped, the
private substrate may be stopped while retaining its state:

```sh
docker stop ashlar-e2e-truss-pg17
```

Restart that same substrate before further publication-bound reads. Do not remove
the container, native tables, retained index/installation, or journals while you
still need their custody. The evidence archive retains selected reports and hashes;
it does not bundle the external SDK/JDK closure, native tables or credentials.

The three recorded queries qualify only this finite original source and the
recorded versions. They do not establish general parallel-edge multiplicity,
observable truncation, two-hop paths, Unity Catalog, Fabric or production source
authority. The [query gallery](QUERY-GALLERY.md) keeps other qualified subsets and
remaining boundaries separate. All compute in this recipe is local.
