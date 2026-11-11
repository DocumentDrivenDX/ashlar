# ashlar

Property graphs on warehouse tables. Ashlar keeps typed nodes and edges, with their
source identity and publication history, in Delta tables. It reads source transactions
(JSONL, or a Truss feed), publishes them as immutable, versioned table states, and runs
Weft-compiled queries against one pinned publication. [UMF](https://github.com/DocumentDrivenDX/umf)
owns the schema semantics and [Weft](https://github.com/DocumentDrivenDX/weft) owns query
compilation; Ashlar owns the publication and the execution adapters. Databricks Unity
Catalog is the first target. Documentation: <https://documentdrivendx.github.io/ashlar/>.

## Install

The distribution is `umf-ashlar` (import `ashlar`). `pyproject.toml` allows Python 3.9 or later; CI tests 3.11 and 3.13. The core has no
runtime dependencies. Extras add the engines:

| Extra | Adds |
| --- | --- |
| `local-delta` | PySpark and Delta for local publication |
| `typed-sdk` | UMF core, access and Weft for typed models and queries |
| `truss-local` | Truss, for the local Truss source workflow |
| `managed-read` | Databricks SDK and psycopg, for the experimental managed path |
| `duckdb`, `puppy`, `puppy-gremlin` | graph export and PuppyGraph query adapters |
| `diagnostics` | OpenTelemetry (Python 3.11) |

## Quickstart (about five minutes)

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install umf-ashlar
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
ashlar inspect-configured-source examples/end-to-end/configured-source.json
```

Run these from a checkout, which supplies the example inputs. The first command prints
three JSONL batches holding the complete committed transaction bytes; the second checks a
configured model and bindings, applies create, update and delete changes locally and
verifies exact replay. Neither touches a database or acknowledges a source.
CI runs these exact commands against the built wheel (`scripts/quickstart.sh`).

Next steps: the [end-to-end local workflow](examples/end-to-end/README.md), the
[Truss examples](examples/truss-native/README.md), and the
[original commerce graph](examples/original-commerce-local/README.md).

## What is and is not tested

CI runs on Linux with Python 3.11 and 3.13. Every change runs the unit suite, the module
boundary checks and the wheel build with the quickstart above. A nightly job runs the slow
suite and the Spark tier (local Spark 4.2 / Delta, Java 21, and the Bun TypeScript tests).

| Area | Status |
| --- | --- |
| Source inspection, configured sources, replay | Tested (quickstart and unit tests) |
| Local Delta publication, publication-bound reads, graph export | Unit tests with fakes; the nightly Spark tier runs part of them on local Spark |
| Typed UMF, Weft and Truss integration | Needs released Truss, UMF and Weft installed; those tests skip without them and are not run in CI yet |
| Databricks / Unity Catalog managed publication | **Experimental.** Mocked unit tests only; CI has no Databricks workspace and nothing here has been run against one. See [`examples/databricks`](examples/databricks/README.md) |
| PuppyGraph, Fabric GQL | **Experimental.** Adapters are unit-tested against fakes; no live engine is exercised |
| Scale (1B nodes / 5B edges) | Not attempted |

## Layout

`src/ashlar` is the portable toolkit and `src/ashlar_host` the host adapters. The table
contracts, the SQL package and the UMF models are under `docs/helix/02-design` and `sql/`.
See the [changelog](CHANGELOG.md) and [roadmap](ROADMAP.md).

## Development

```sh
pip install -e '.[dev,diagnostics]'
pytest -n 4 -m "not slow and not spark" tests
```

## License

First-party software is dual-licensed under MIT OR Apache-2.0, at your option. See
[LICENSE.md](LICENSE.md). Third-party materials retain their original notices.
