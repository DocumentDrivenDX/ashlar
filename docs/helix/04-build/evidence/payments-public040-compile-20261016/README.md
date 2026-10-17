# Payments compile-only check with public Weft 0.4.40

The authentic public 0.4.40 wheel now registers the qualified Ashlar backend.
The previously authored `weft-compile/0.2.0` / `weft-sql/0.2.0` settlement request
with target `ashlar.databricks / 0.1.0-candidate / dbsql-candidate` returns
`WFT-BACKEND-VERSION` in 5.8 ms, where public 0.4.39 returned
`WFT-BACKEND-MISSING`. A separately selected target declared by the exact
0.4.40 source, `ashlar.databricks / 0.1.0-qualified /
dbsql2026.39-qualified`, compiles the unchanged settlement SQL in 21.7 ms with
no diagnostics. Original UMF 0.8 model text, model pins and physical binding
bytes are identical between the two newly authored requests.

These are declared 0.2.0 requests, not relabeled historical 0.4.2 requests.
The exact published compiler source still does not admit the 0.4.2 protocol.
The qualified output requires Databricks SQL 2026.39 and the explicit build hashes
and host obligations retained in its `targetContext`. Compilation does not
establish adoption by the existing local Spark publication or satisfy those
native runtime, authorization, retention and publication obligations. No native
query, source/ACK operation, cloud run or financial result occurred.

The actual emitted settlement amount guard still accepts only BIGINT or
qualifying DECIMAL carriers and excludes DOUBLE. The
[held local diagnostic](../payments-held-decimal-guard-20261016/README.md)
separately proves the original `1E+2` property is observed as DOUBLE and causes
one violation on the retained Spark publication. This compile check does not
resolve that gap. The broader
[public 0.4.40 typed SDK native evidence](../typed-sdk-pages-20261016/public040/README.md)
qualifies its own workflows and must not be substituted for a settlement pass.

The selected official arm64 wheel SHA-256 is
`24d5514c779bd43fceafb6f89268f4fb4dc6a28b1332370fcaefd9f4c20b63cc`
(6,572,971 bytes), matching the already retained official release asset digest.
Its self-reported version is 0.4.40 and release source commit is
`685691d3f7e2b9cd98285c74a56a6707a7b01cfd`. It was extracted into a fresh private
directory and imported explicitly; no ambient package was installed or changed.
Exact source files were read from that commit in the sibling Git object store.
See the existing [published compiler verification](../typed-sdk-pages-20261016/public040/published-compiler.json)
for independent release hashes and packaging evidence.

`manifest.json` records raw and compressed hashes for nine small records:
selection, result, runner, both exact requests/responses and the two governing
compiler source files. The wheel and complete package are not copied. The
upstream decimal reproduction remains prepared but unsent.
