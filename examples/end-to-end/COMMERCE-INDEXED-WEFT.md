# Publish and query the original commerce graph locally

This checkout workflow publishes the unchanged UMF 0.8 commerce fixture, then uses
an installed, independently indexed Weft compiler for a String projection, exact
count and authored product-to-supplier relationship. It uses a private PostgreSQL
outbox and local Delta. Development IDs are explicit fixture configuration;
this does not install Truss or qualify Unity Catalog, Fabric or production authority.

Use Ashlar source revision `12ddc5f07c40da3cf67a04520cc40671781dbecb` for this
recipe. Commands run from that checkout. The compiler realization is
`weft-362a9c6-ab899150-aarch64-apple-darwin-candidate`, qualified for Apple Silicon
on observed macOS 27.0.1. Other platforms require separately reviewed realizations.
The newer distinct-neighbor degree correction is not included in this immutable
package; this workflow selects the original single-edge commerce fixture only.

## Prepare explicit resources

Select Python 3.11 for publication: GraphFrames 0.12.3 requires Python 3.10 or
later. Use a separate Python 3.11 query environment, Bun, Docker, JDK 17 for
publication and JDK 21 for queries. The recorded JDK versions are 17.0.9 and
21.0.12.1; package version ranges alone do not qualify another native runtime.
Supply existing JAR directories; the commands below do not discover private
runtime paths. Replace these six values with your actual absolute paths:

```sh
export ASHLAR_PUBLISH_PYTHON=/absolute/path/to/python3.11
export ASHLAR_QUERY_PYTHON=/absolute/path/to/python3.11
export ASHLAR_JAVA17_HOME=/absolute/path/to/jdk17
export ASHLAR_JAVA21_HOME=/absolute/path/to/jdk21
export ASHLAR_DELTA3_JARS=/absolute/path/to/spark3-jars
export ASHLAR_DELTA4_JARS=/absolute/path/to/delta4-jars
```

The publication directory must contain these four files:

- `io.delta_delta-spark_2.12-3.2.1.jar`
- `io.delta_delta-storage-3.2.1.jar`
- `io.graphframes_graphframes-spark3_2.12-0.12.3.jar`
- `io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar`

The query directory must contain exactly these two files. The host checks their
SHA-256 digests before starting Spark:

| File | SHA-256 |
|---|---|
| `delta-spark_2.13-4.0.0.jar` | `538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e` |
| `delta-storage-4.0.0.jar` | `9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d` |

Create two isolated Python 3.11 environments; do not mix Spark versions. The
recorded read-only journal runtime uses SQLite 3.53.1; other SQLite builds need
separate verification of read-only WAL opening without preexisting sidecars:

```sh
"$ASHLAR_PUBLISH_PYTHON" -m venv .venv-commerce-publish
.venv-commerce-publish/bin/python -m pip install . pyspark==3.5.3 delta-spark==3.2.1 graphframes-py==0.12.3 'psycopg[binary]==3.2.13'
"$ASHLAR_QUERY_PYTHON" -m venv .venv-commerce-query
.venv-commerce-query/bin/python -m pip install . pyspark==4.0.1 delta-spark==4.0.0 typing-extensions==4.15.0 'psycopg[binary]==3.2.13'
mkdir -p out
```

Select fresh paths for every new setup or query attempt. Keep failed installation
files and journals intact; changing output paths is not recovery of an old run.

## Prepare the validator and installed compiler

```sh
git clone https://github.com/DocumentDrivenDX/umf.git out/umf-commerce
git -C out/umf-commerce checkout --detach c7c95e1c4ea5b72541f47fa0350ca467ff02f395
(cd out/umf-commerce && bun install --frozen-lockfile)
git clone --filter=blob:none --no-checkout https://github.com/DocumentDrivenDX/weft.git out/weft-distribution
git -C out/weft-distribution sparse-checkout set distributions
git -C out/weft-distribution checkout 41a42ed5fc3c780ce900c61144733ad93359dc76
.venv-commerce-query/bin/ashlar install-weft --index "$PWD/out/weft-distribution/distributions/index.json" --package "$PWD/out/weft-distribution/distributions/realizations/weft-362a9c6-ab899150-aarch64-apple-darwin-candidate" --output "$PWD/out/weft-installation"
```

Ashlar owns trusted index digest `71e441ce80b36a50cb8fc668431fefddec71e5e83908eb7fe78abe40c41ba2de`; there is no operator checksum or
platform override. The [installation guide](WEFT-INSTALLATION.md) explains the
complete package checks. Both publication and query independently rerun the
original public UMF dataset validator against the clean pinned source checkout.

## Publish, stop, then query

Start the labeled private PostgreSQL 17.9 substrate:

```sh
.venv-commerce-publish/bin/python tools/start_truss_sandbox.py
.venv-commerce-publish/bin/python - <<'PYHEALTH'
import subprocess, time
for attempt in range(60):
    status = subprocess.check_output(
        ['docker', 'inspect', '--format', '{{.State.Health.Status}}',
         'ashlar-e2e-truss-pg17'], text=True).strip()
    if status == 'healthy':
        break
    if status == 'unhealthy':
        raise SystemExit('Private PostgreSQL health check failed')
    time.sleep(1)
else:
    raise SystemExit('Private PostgreSQL did not become healthy within 60 seconds')
PYHEALTH
```

Wait for the bounded health check above before publication; startup alone does
not establish readiness. It prints no credentials.

Its fixed admitted route is container `ashlar-e2e-truss-pg17`, loopback
`127.0.0.1:15432`, database `truss_e2e`. The host obtains private container
credentials and establishes ordinary source-specific sessions; you do not paste
credentials into configuration. This route is a development outbox, not a Truss
catalog or feed. The container has one CPU and a 512 MiB memory limit.

Run publication first and wait for it to exit before starting the query. Each
Spark process uses one local worker and a 512 MiB driver:

```sh
JAVA_HOME="$ASHLAR_JAVA17_HOME" SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv-commerce-publish/bin/python" PYSPARK_DRIVER_PYTHON="$PWD/.venv-commerce-publish/bin/python" PYTHONPATH=src:tools .venv-commerce-publish/bin/python tools/run_commerce_outbox_publication.py --output "$PWD/out/commerce-publication" --jars "$ASHLAR_DELTA3_JARS" --umf-source "$PWD/out/umf-commerce"
```

After successful publication and cleanup:

```sh
JAVA_HOME="$ASHLAR_JAVA21_HOME" SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv-commerce-query/bin/python" PYSPARK_DRIVER_PYTHON="$PWD/.venv-commerce-query/bin/python" PYTHONPATH=src:tools .venv-commerce-query/bin/python tools/run_indexed_commerce_weft.py --index "$PWD/out/weft-distribution/distributions/index.json" --installation "$PWD/out/weft-installation" --publication "$PWD/out/commerce-publication" --umf-source "$PWD/out/umf-commerce" --jars "$ASHLAR_DELTA4_JARS" --output "$PWD/out/commerce-indexed-query" --ack-container ashlar-e2e-truss-pg17 --ack-host 127.0.0.1 --ack-port 15432 --ack-database truss_e2e
```

## Read the results

A successful setup report names the actual table UUID/version vector, original
11 objects/10 edges/21 history rows, public admission and protected source ACK.
The query report retains the unchanged compiler requests, SQL, checks, source
oracles and closing intervals for all three queries. The relationship result
retains the original key tokens for P1→S1 and explicit truncation metadata.
Reports are released only after the required closing checks and Spark stop.

These instructions do not themselves establish a native pass. The fixed fixture
contains one selected relationship occurrence; it does not qualify parallel-edge
multiplicity, observable truncation or two-hop paths. The
[query gallery](QUERY-GALLERY.md) lists separately qualified subsets and remaining
boundaries. All compute here is local; no shared/default Databricks endpoint is used.
