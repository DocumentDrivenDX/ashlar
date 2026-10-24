# Weft 0.4.53 R02 Spark 4.2 public target check

Status: **the public Weft 0.4.53 macOS arm64 wheel compiles the exact R02
Backend03 grouped query and Backend02 page/continuation requests using their
Spark 4.2.0 / Delta 4.4.1 candidate targets**. This closes the compiler target
compatibility question only; no SQL was executed and no native/runtime claim is
added.

The effect-free replay uses the original Backend03 and Backend02 request JSON
strings in [`managed-current-uc-program-parity-v0.4.50.json`](../../current-upstream-release-20261021/local-current/managed-current-uc-program-parity-v0.4.50.json)
(source file SHA-256 `dd5cef72e28b120f6a81d79d941068cbc3b099f4276dcc386c631bb06f721887`). The
original request SHA-256 values are `db32c552f3de31fc8f4f11f0b1a7edfb0645026ddbe2a72b6b3f4ea6ac2e74d5`
(Backend03) and `3206e3c16f7721699a61974e0419b015bb0728a4d6b697e734f68a3e873ae729`
(Backend02). The page compiler wraps the latter unchanged in
`weft.engine-page/0.1`, compiler `ashlar-spark4-pages`, page size 2. The
continuation uses `check_engine_page_json` with the same original request and
`after_json=["person-2"]`.

All three calls returned `compiled`, with the exact source-declared target
tuples:

| Path | Backend ID / version / target profile | Response SHA-256 |
|---|---|---|
| Backend03 grouped paths/keys/HAVING | `ashlar.databricks.paths-keys-having` / `0.4.1-count-star-having-spark4.2-candidate` / `spark4.2.0-delta4.4.1-paths-keys-having-candidate` | `be2e32908d9175c5206aa9a4a9c5383b8aa6eaee282b295bd9d9e890c5d4e7c9` |
| Backend02 SDK first page | `ashlar.databricks` / `0.2.0-local-spark4.2-pages-candidate` / `spark4.2.0-delta4.4.1-candidate` | `879af9e863ef97016106a33ec95bb80a35095340cb1aa0cb2bfb5e68c2e3cdcb` |
| Backend02 SDK continuation | same as first page; `after=["person-2"]` | `d96f63e9786f4b8d4e495708f9799edd8b9c7a66efbcbc88e0fee5eab89e98c4` |

Full response bytes are retained as deterministic gzip JSON: [Backend03](backend03-response.json.gz),
[Backend02 first page](backend02-page1-response.json.gz), and
[Backend02 continuation](backend02-continuation-response.json.gz). Their
uncompressed response hashes are listed above and compressed hashes are in
[`replay-summary.json`](replay-summary.json). The page responses preserve the
hidden Key output; the continuation response records generated continuation
filter position 0.

Reproduce from this directory with:

```sh
/private/tmp/ashlar-weft053-adoption-20261023/venv/bin/python replay.py
```

The environment imports Weft 0.4.53 from
`/private/tmp/ashlar-weft053-adoption-20261023/venv/lib/python3.12/site-packages/weft/__init__.py`.
The selected official macOS arm64 wheel SHA-256 is
`4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c`; package
and wheel provenance are recorded in the [adoption receipt](../weft053-ashlar-adoption-20261023/README.md).
The target declarations and exact Backend03/Backend02 dispatch tests are in
Weft tag commit `726a840e956e25217d6aeea2674fdfcf3f342a24`, including
`crates/weft-databricks/src/paths_keys_having_spark42.rs`,
`crates/weft-databricks/src/local_spark42.rs`,
`crates/weft-databricks/src/authored_pages.rs`, and the runtime checks in
`crates/weft-runtime/src/paths_keys.rs` and `engine_read.rs`.

The older feasibility probe's crossed-target refusal remains valid: replacing
the Backend03 Spark 4.2 profile with the Backend02 Spark 4.0 profile is rejected.
It did not test the target-matched R02 pair documented here. Neither these
compiler calls nor the prior native receipt establish a current public-wheel
native execution, publication custody, or production authority.
