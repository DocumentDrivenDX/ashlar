# B runtime dependency-lock plan (candidate; not resolved)

Status: read-only lock match verified. Runtime attempt 01 was interrupted
during offline installation and is quarantined without a receipt. No runtime
receipt, B execution, or native effect was produced. Attempt 02 is selected
but absent.

## Selected runtime boundary

Use a fresh Python 3.12.14 environment at
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv-attempt-02`. Resolve packages
only from a local wheelhouse with index/network access disabled. The current
candidate cohort is `umf_access==0.1.0.dev7`, `umf-core==0.10.0`,
`truss-toolkit==0.0.49`, and `weft-sql==0.4.52`; Weft is present for the
selected managed-page SDK boundary and is not used by B source installation.
This selected cohort does not qualify Ashlar's declared `typed-sdk` extra.

| Direct artifact | Local wheel | SHA-256 |
| --- | --- | --- |
| UMF access 0.1.0.dev7 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/umf_access-0.1.0.dev7-py3-none-any.whl` | `a78053ab8f35f55f23661de780f351f90e71299773774b62938ffb09270f3597` |
| UMF core 0.10.0 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/umf_core-0.10.0-py3-none-any.whl` | `49633924b45a3c8499d4fcc1a24c30d5a8fcb34d974ab019bbda987cb47da0f9` |
| Truss toolkit 0.0.49 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/truss_toolkit-0.0.49-py3-none-any.whl` | `7ea14b2787f02cbeec9ad99d835adbcc30c684fd5be8e0765a1d8198b50e7182` |
| Weft SQL 0.4.52 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/weft_sql-0.4.52-cp39-abi3-macosx_11_0_arm64.whl` | `598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46` |

## Lock and receipt gates

Reuse the R4 CPython 3.12.14/macOS arm64 exact lock as the selected B cohort:
`/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/wheelhouse-lock-pyspark-4.2.0-attempt-02.json`
(SHA-256
`d2686e1dbbdd5a136990b146b25375cba5a7c73e21ef30290cdfa12b3c70aa20`). Its
27 entries were checked read-only: every wheel file exists and matches its
recorded filename and SHA-256, and the four direct pins above match the B
cohort. It is the R4 superset lock and includes `pyspark`, `delta-spark`, and
`py4j`; their presence in a B venv does not authorize or imply a Spark start.
Preserve and bind the exact lock bytes; do not re-resolve or substitute a
different wheelhouse. The lock's downloaded artifacts are inputs, not a B
runtime receipt.

The B-specific receipt must bind this lock digest, Python/platform identity,
these four direct wheel digests, every resolved transitive wheel digest,
installed versions, `pip check`, the already planned focused offline SDK/API
tests, and `no_native_execution=true`, `pg_started=false`, and
`spark_started=false`. Its candidate path is
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/runtime-receipt-attempt-02.json`.
The lock and wheel hashes are selected, but the B-specific install receipt and
catalog-manifest digest remain unresolved; therefore the B config intentionally
keeps those runtime values null. Worker preflight verifies the selected lock
file digest, target/count, direct cohort identities, and that the future
B-specific runtime receipt names this lock digest. Do not create a B venv or
approval sidecar until the remaining B authority decisions and execution
review are complete.

The focused check command and the direct artifact provenance are retained in
[the compatibility check plan](b-runtime-compatibility-check-plan-20261023.md).
For the interrupted attempt-01 state and exact source/origin custody copies,
see [the partial-attempt record](b-runtime-partial-attempt-01-note-20261023.md).
