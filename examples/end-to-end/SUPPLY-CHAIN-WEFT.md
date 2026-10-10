# Compile the original supply-chain queries

Install Ashlar from the selected source revision and install the separately indexed count-star compiler using [the count-star guide](COUNT-STAR-WEFT.md). Keep the original supply-chain `pack.json`, `ontology.json` and `graph/fixture.json` unchanged.

Provide a compiler metadata JSON object with exactly `bindings`, `manifest`, `registry`, and `aliases`. Bindings must be the original `ashlar-supply-chain-development-bindings/0.2` carriers. Manifest and registry describe the same complete six-role publication; aliases map each four-role pinned table to a distinct three-component Spark identifier. These inputs construct requests and grant no publication or source authority.

```sh
ashlar compile-supply-chain-count-star \
  --pack /absolute/supply-chain/upstream/pack.json \
  --model /absolute/supply-chain/upstream/ontology.json \
  --graph /absolute/supply-chain/upstream/graph/fixture.json \
  --metadata /absolute/compiler-metadata.json \
  --index /absolute/weft/distributions/index.json \
  --installation /absolute/selected-count-star-installation \
  --output /absolute/unused-supply-chain-compile-output \
  --maximum-artifact-bytes 1048576
```

The command compiles and freshly recompiles all five unchanged queries: split excursion, excursion, replay, lineage and sensor. It preserves the original aliasless `COUNT(*)`/`HAVING COUNT(*)>1` replay query, independent event occurrences, carrier identifiers and present-null lineage value. Every artifact passes the selected offline 0.4.1 admission contract. Raw request and response observations are retained before parsing; a final report requires byte-identical recompilation and closing original input custody.

Original input files and their directory ancestry must be absolute, regular, unsymlinked and unchanged by cooperating writers throughout the command. Use a fresh output directory. Compiler evidence and independent source bags remain separate from native SQL execution: native querying additionally requires an independently established publication, actual held source/publication readers and closing release admission.
