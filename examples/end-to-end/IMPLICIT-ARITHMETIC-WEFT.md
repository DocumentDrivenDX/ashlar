# Install and compile the fixed implicit-arithmetic profile

Install Ashlar from a revision containing `install-weft-implicit-arithmetic` and
`compile-weft-implicit-arithmetic`. Retain the selected source and wheel provenance.
This additive profile preserves the existing 0.4.1 commands and defaults.

Supply a local Weft checkout at index revision
`cdd8fa698683cd87bb126916a75200e80e19d831`. The public distribution owner selects
index SHA-256 `6aa24c395dc721ef6cfef281f34628051a041530ba426d8dc31e9126e9510399`
and realization `weft-f36f206-implicit-arithmetic-aarch64-apple-darwin-candidate`.
Its qualified platform is Darwin arm64, macOS 27.0.1. Paths locate these fixed
products; they cannot override the profile, trust pins or platform.

```sh
ashlar install-weft-implicit-arithmetic \
  --index /absolute/weft/distributions/index.json \
  --package /absolute/weft/distributions/realizations/weft-f36f206-implicit-arithmetic-aarch64-apple-darwin-candidate \
  --output /absolute/fresh-implicit-arithmetic-installation

ashlar compile-weft-implicit-arithmetic \
  --index /absolute/weft/distributions/index.json \
  --installation /absolute/fresh-implicit-arithmetic-installation \
  < /absolute/original-042-request.json \
  > /absolute/042-response.json
```

Installation requires a fresh destination and verifies the indexed product before
publishing it. Installation status goes to stderr. Compilation reads bounded raw
stdin (at most 16 MiB) and emits the original compiler response bytes on stdout.
A protocol `blocked` response is retained with exit status zero; inspect its status.
A transport or installation refusal exits with status two and a payload-free
stderr diagnostic. Cancellation propagates to the caller.

Build the request using the public
`ashlar_host.implicit_arithmetic_request.commerce_implicit_arithmetic_request`
owner and the complete original commerce pack and publication binding. Preserve
all four original scenario SQL strings, including unnamed arithmetic. The fixed
backend version is `0.4.2-implicit-arithmetic-labels-candidate`, paired with the
0.4.2 compile, SQL and IR schemas. Do not relabel a 0.4.1 request or response.

For publication prerequisites see the [commerce guide](COMMERCE-INDEXED-WEFT.md).
`query-commerce-implicit-arithmetic` additionally requires an explicit ordinary
session provider and the original pack; its source, protected ACK and native
publication checks must remain current throughout execution and cleanup. Compiler
or installation hashes supply correspondence, never publication authority.

The [retained installed qualification](../../docs/helix/04-build/evidence/commerce-042-installed-cli-20261011-b/README.md) covers the current finite compiler subset: compiler API, offline admission,
help and invalid-configuration refusal. These commands expose that fixed package
through the CLI. Full native execution of the four original 0.4.2 commerce queries
still requires separate qualification; compilation alone does not establish it.
