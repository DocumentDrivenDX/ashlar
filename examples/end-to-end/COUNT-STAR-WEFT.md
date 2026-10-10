# Compile and query the COUNT(*) commerce gallery

Select the explicit `count-star` compiler profile to count complete row or path
occurrences, group those counts, and filter complete groups with `HAVING`.
`install-weft-count-star`, `compile-weft-count-star` and
`query-commerce-count-star` select this profile separately from the existing
compiler commands and defaults.

## Install the selected source and compiler

Use Python 3.11 and fresh absolute directories. Install Ashlar from the immutable
source revision below; `pip install` builds a package from that source. A package
version or filename alone does not identify the build.

```sh
export ASHLAR_COUNT_STAR_SOURCE=/absolute/path/to/count-star-toolkit
export ASHLAR_COUNT_STAR_ENV=/absolute/path/to/count-star-env
export ASHLAR_COUNT_STAR_WORK=/absolute/path/to/count-star-work

git clone --filter=blob:none --no-checkout https://github.com/DocumentDrivenDX/ashlar.git "$ASHLAR_COUNT_STAR_SOURCE"
git -C "$ASHLAR_COUNT_STAR_SOURCE" sparse-checkout set --no-cone '/src/' '/pyproject.toml' '/README.md'
git -C "$ASHLAR_COUNT_STAR_SOURCE" checkout --detach 889ab24abf3f165cdafbc7947619131358656872
python3.11 -m venv "$ASHLAR_COUNT_STAR_ENV"
"$ASHLAR_COUNT_STAR_ENV/bin/python" -m pip install --no-deps "$ASHLAR_COUNT_STAR_SOURCE"
mkdir "$ASHLAR_COUNT_STAR_WORK"

git clone --filter=blob:none --no-checkout https://github.com/DocumentDrivenDX/weft.git "$ASHLAR_COUNT_STAR_WORK/weft"
git -C "$ASHLAR_COUNT_STAR_WORK/weft" sparse-checkout set distributions
git -C "$ASHLAR_COUNT_STAR_WORK/weft" checkout --detach bd490abc877a1992666c7b45412b14a0ca6c2cac
"$ASHLAR_COUNT_STAR_ENV/bin/ashlar" install-weft-count-star \
  --index "$ASHLAR_COUNT_STAR_WORK/weft/distributions/index.json" \
  --package "$ASHLAR_COUNT_STAR_WORK/weft/distributions/realizations/weft-5ddcebd-count-star-aarch64-apple-darwin-candidate" \
  --output "$ASHLAR_COUNT_STAR_WORK/compiler"
```

The selected Weft realization targets Apple Silicon/macOS and backend
`ashlar.databricks.paths-keys` version `0.4.1-count-star-having-candidate`.
Ashlar verifies its trusted index SHA-256
`7eddc39661f6cb400afae897aa0a7d8cd5dee0aec026ed45a4e197fa7c9558a6`
and the complete package before installation. Normal Git checkout modes are
accepted as source input; installed owned copies are read-only. Operators cannot
override the trust pins or substitute another platform realization.

Installation and compilation start no database or Spark process. Retain the index
and installation to reopen the compiler; the source realization is unnecessary
for later compilation. Keep any rebuild's wheel metadata and payload hashes with
its source revision. The retained installed-CLI recipe records the tested wheel's
exact correspondence to this source; another build does not inherit native query
qualification from its version string.

## Compile all fourteen original commerce intents

First prepare the original commerce publication with the
[commerce setup guide](COMMERCE-INDEXED-WEFT.md). Set `ASHLAR_PUBLICATION` to its
absolute path. Compilation uses the publication's retained model, bindings,
manifest and table registry; it does not contact its PostgreSQL source.

The installed public request builder selects the explicit protocol and preserves
the original SQL, model, bindings and complete publication vector:

```sh
export ASHLAR_PUBLICATION=/absolute/path/to/commerce-publication
"$ASHLAR_COUNT_STAR_ENV/bin/python" -B - <<'PYREQUESTS'
import json, os
from pathlib import Path
from ashlar_host.count_star_query import (
    commerce_count_star_request, commerce_path_cases, commerce_count_star_cases,
)
publication = Path(os.environ['ASHLAR_PUBLICATION'])
output = Path(os.environ['ASHLAR_COUNT_STAR_WORK']) / 'requests'
output.mkdir()
model = (publication / 'original-ontology.json').read_bytes()
bindings = json.loads((publication / 'development-bindings.json').read_bytes())
report = json.loads((publication / 'report.json').read_bytes())
manifest = report['native_manifest']
registry = report['table_registry']
aliases = {
    name: name.replace('local.', 'spark_catalog.', 1)
    for name in json.loads(manifest['table_versions_json'])
}
for name, sql in commerce_path_cases() + commerce_count_star_cases():
    request = commerce_count_star_request(sql, model, bindings, manifest, registry, aliases)
    with (output / (name + '.json')).open('x') as stream:
        json.dump(request, stream, separators=(',', ':'), ensure_ascii=False)
        stream.write('\n')
PYREQUESTS
mkdir "$ASHLAR_COUNT_STAR_WORK/responses"
for request in "$ASHLAR_COUNT_STAR_WORK/requests/"*.json; do
  name="${request##*/}"
  "$ASHLAR_COUNT_STAR_ENV/bin/ashlar" compile-weft-count-star \
    --index "$ASHLAR_COUNT_STAR_WORK/weft/distributions/index.json" \
    --installation "$ASHLAR_COUNT_STAR_WORK/compiler" \
    < "$request" > "$ASHLAR_COUNT_STAR_WORK/responses/$name" || exit "$?"
done
```

The fourteen intents comprise the ten original path/property queries and the
[four COUNT(*) queries](QUERY-GALLERY.md#count-complete-commerce-occurrences).
A semantic `blocked` response is protocol output with exit zero; inspect its
status and diagnostics. Transport or installation refusal exits with status two.
Preserve emitted SQL and host obligations unchanged. Compilation establishes a
plan; it grants no publication, source acknowledgement or native execution authority.

## Execute the publication-bound gallery

Select the query runtime, JDK21, Delta4 JARs, pinned public UMF validator and
private source substrate from the commerce setup guide. Its older Ashlar wheel
must be replaced by the selected source installation above in the query
environment. Keep that prepared query environment, including its native SDKs,
separate from the compiler-only environment. Select its Python/SQLite profile
before creating that environment;
a virtual environment cannot change the base interpreter's SQLite library. The
native dependencies remain explicit prerequisites. Use the
installed resource root from that environment, never another checkout's imports:

```sh
"$ASHLAR_QUERY_ENV/bin/python" -m pip install --no-deps --force-reinstall "$ASHLAR_COUNT_STAR_SOURCE"
export ASHLAR_QUERY_RESOURCES="$("$ASHLAR_QUERY_ENV/bin/python" -c 'from ashlar_host.resources import RESOURCE_ROOT; print(RESOURCE_ROOT)')"
JAVA_HOME="$ASHLAR_JAVA21_HOME" SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$ASHLAR_QUERY_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$ASHLAR_QUERY_ENV/bin/python" \
"$ASHLAR_QUERY_ENV/bin/ashlar" query-commerce-count-star \
  --model "$ASHLAR_QUERY_RESOURCES/ontology.json" --graph "$ASHLAR_QUERY_RESOURCES/graph.json" \
  --umf-source "$ASHLAR_WORK/umf" --bun "$ASHLAR_BUN" --git "$ASHLAR_GIT" \
  --producer-timeout-seconds 60 --producer-maximum-output-bytes 1048576 \
  --producer-maximum-receipt-bytes 4194304 \
  --postgres-container ashlar-e2e-truss-pg17 --postgres-host 127.0.0.1 \
  --postgres-port 15432 --postgres-database truss_e2e \
  --output "$ASHLAR_COUNT_STAR_WORK/native-queries" --jars "$ASHLAR_DELTA4_JARS" \
  --index "$ASHLAR_COUNT_STAR_WORK/weft/distributions/index.json" \
  --installation "$ASHLAR_COUNT_STAR_WORK/compiler" \
  --publication "$ASHLAR_PUBLICATION" \
  --maximum-artifact-bytes 16777216 --maximum-rows 1000 \
  --maximum-cell-bytes 1048576 --maximum-total-cell-bytes 16777216
```

The native command must hold the original source, protected ACK and full table
UUID/version vector through execution and closing checks. Complete source
integrity and capacity guards run before `HAVING` and before releasing results;
a filtered group cannot hide an overflow or incomplete bag. Retain the full
report, independent source values and cleanup outcome. Installed compilation
and offline admission evidence do not substitute for this separate native proof,
Unity Catalog, Fabric or real Truss source qualification.
