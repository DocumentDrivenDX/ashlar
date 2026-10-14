# Install and use the indexed Weft compiler

Ashlar verifies a locally supplied Weft distribution against its committed trusted
index pin before installing executable bytes. The initial candidate is qualified
for Apple Silicon on the observed macOS 27.0.1 host. Other hosts require a separately
qualified realization. Installation and compilation use local files and start no
database or graph engine.

Run from the Ashlar checkout with Python 3.9+ and fresh output directories:

```sh
python3 -m venv .venv-ashlar
.venv-ashlar/bin/python -m pip install .
mkdir -p out
git clone --filter=blob:none --no-checkout https://github.com/DocumentDrivenDX/weft.git out/weft-distribution
git -C out/weft-distribution sparse-checkout set distributions
git -C out/weft-distribution checkout 41a42ed5fc3c780ce900c61144733ad93359dc76
.venv-ashlar/bin/ashlar install-weft --index out/weft-distribution/distributions/index.json --package out/weft-distribution/distributions/realizations/weft-362a9c6-ab899150-aarch64-apple-darwin-candidate --output out/weft-installation
.venv-ashlar/bin/ashlar compile-weft --index out/weft-distribution/distributions/index.json --installation out/weft-installation < examples/end-to-end/weft-installation-request.json > out/weft-compile-response.json
```

The sample is Weft's unchanged `columns-native:64-empty-column-column` compile
request, SHA-256 `f99ccab34a1add755375051480788dd9f2d74d551a7a2440193e6649968d76e4`.
It retains the original fixture model, bindings, publication vector and SQL.
It demonstrates compilation; its fixture publication is not a live Ashlar
publication or permission grant. Read the compiler response's host obligations
before using a compiled plan with publication-bound query orchestration.

The application owns the index revision, hash and realization selection. Operators
supply the index file, package and installation paths; there are no trust-pin or
platform overrides. The complete package inventory and original conformance
artifacts are verified before availability is published. Restart needs the retained
index and installation, without the original package directory.

Compiler stdout is returned byte-for-byte after closing custody checks. A semantic
`blocked` response remains protocol output with exit status zero. Transport,
installation, platform and input-size refusals exit with status two and a bounded
category on stderr. Input and response limits are 16 MiB. Installation outcomes
use stderr; `cleanup-pending` means installation committed successfully but staging
housekeeping needs attention. It is a transient maintenance indication, not durable
recovery state. Ordinary file writes do not promise crash durability.

This indexed candidate is distinct from the separately qualified LEFT-query
compiler. Each [query-gallery receipt](QUERY-GALLERY.md#qualified-original-query-subsets)
retains its own exact compiler and native scope; a package installation does not
qualify every backend profile, Unity Catalog, Truss authority or Fabric execution.

## Install CountStar for finite-pack queries

The archaeology/ecology finite-pack workflow selects the separate CountStar
compiler. The initial `install-weft` example above does not supply that profile.
Use the already installed Ashlar environment and choose new absolute checkout and
installation paths; replace every path below. The installation output must be
absent. This profile is qualified only for Apple Silicon on macOS 27.0.1.

```sh
ASHLAR_ENV=/absolute/path/to/installed-ashlar-environment
WEFT_COUNT_SOURCE=/absolute/path/to/fresh-weft-count-distribution
WEFT_COUNT_INSTALLATION=/absolute/path/to/fresh-weft-count-installation

git clone --filter=blob:none --no-checkout https://github.com/DocumentDrivenDX/weft.git "$WEFT_COUNT_SOURCE"
git -C "$WEFT_COUNT_SOURCE" sparse-checkout set distributions
git -C "$WEFT_COUNT_SOURCE" checkout bd490abc877a1992666c7b45412b14a0ca6c2cac
"$ASHLAR_ENV/bin/ashlar" install-weft-count-star \
  --index "$WEFT_COUNT_SOURCE/distributions/index.json" \
  --package "$WEFT_COUNT_SOURCE/distributions/realizations/weft-5ddcebd-count-star-aarch64-apple-darwin-candidate" \
  --output "$WEFT_COUNT_INSTALLATION"
```

The selected index SHA-256 is
`7eddc39661f6cb400afae897aa0a7d8cd5dee0aec026ed45a4e197fa7c9558a6`.
Ashlar owns that trust pin and realization selection; these variables supply file
locations. Keep this index and installation for the finite-pack query generator's
`--index` and `--installation` arguments. Restart uses their retained bytes without
the original package directory. No compiler build, private temporary installation,
manual chmod or alternative profile is needed.

This installs the `0.4.1-count-star-having-candidate` compiler and its original
protocol resources. It starts no graph engine and does not qualify a supplied
publication, source authority, new platform or native query result. Continue with
[the finite-pack Quickstart](../domain-packs/QUICKSTART.md) for explicit source,
runtime and publication selections.
