# B runtime dependency-lock and focused compatibility status

Status: read-only lock match and attempt-02 post-install receipt verified.
Attempt-01 was interrupted during offline installation and remains quarantined.
Attempt-02 installed the 27 locked wheels; read-only verification checked
installed RECORD payloads, versions, imports, and the successful pip-check
result with its nonfatal warning. The receipt is bound in B config. The 24
focused SDK/API tests subsequently passed in the preserved venv; no
Truss source installation, acceptance, or native service effect was performed. See the
[attempt-02 result note](b-runtime-partial-attempt-02-note-20261023.md).

## Selected runtime boundary

Attempt-02 uses the Python 3.12.14 environment at
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv-attempt-02`. It was
created only from the local wheelhouse with index/network access disabled. The current
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

The B-specific post-install receipt at
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/runtime-receipt-attempt-02.json`
binds this lock digest, Python/platform identity,
all 27 locked wheel digests, installed versions and RECORD payload hashes,
`pip check`, seven import results, and `no_native_execution=true`,
`pg_started=false`, and
`spark_started=false`.
The receipt SHA-256 is
`e96705451e82392657aafc956e3e85e2975e5e8b56a2658b3a93d9fd3d2f18ce`; its Truss
catalog-manifest digest is
`288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`. It records
pip-check exit 0 with a nonfatal cache warning. That install receipt accurately
records that tests had not yet run when it was issued. The separate focused
test receipt at
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/sdk-api-attempt-02-receipt.json`
has SHA-256
`6ff09683c4f7fd7a2c28c7db3234d732eaffafc9a2dd2065a5b836e95787c8f` and binds
24 passed tests, source hashes, and log/result hashes. Worker preflight verifies
the selected lock file digest, target/count, direct cohort identities, and that the runtime
receipt names this lock digest. Attempt-02 is not source or native
qualification; do not create an approval sidecar
until the remaining B authority decisions and execution review are complete.
Do not rerun installation into attempt-02; any future runtime rebuild needs a
fresh attempt path.

The focused check command and the direct artifact provenance are retained in
[the compatibility check plan](b-runtime-compatibility-check-plan-20261023.md).
For the interrupted attempt-01 state and exact source/origin custody copies,
see [the partial-attempt record](b-runtime-partial-attempt-01-note-20261023.md).
