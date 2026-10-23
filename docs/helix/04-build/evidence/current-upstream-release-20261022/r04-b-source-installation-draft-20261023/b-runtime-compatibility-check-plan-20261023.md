# B runtime compatibility check plan (not run)

Status: prepared plan only. No B source bytes were read, copied, prepared or
accepted for this plan. No package was installed, no environment was created,
and no B preflight, Truss process, PostgreSQL, Spark, Rust, UI, cloud, feed,
destination or ACK action was performed. This plan is not execution approval or
a runtime receipt.

## Purpose

Prepare the smallest offline compatibility check for the selected B package
cohort before any separately approved B installation attempt. The check covers
Ashlar's current managed-page SDK boundary with the exact public UMF Python
access wheel, UMF core 0.10.0, Truss 0.0.49, and selected Weft 0.4.52. It checks
package/version/API compatibility only; it does not check or assert B source
authority, source suitability, Truss acceptance, provider identities, or native
behavior.

## Exact direct artifact pins

| Distribution | Exact local artifact | Expected SHA-256 |
| --- | --- | --- |
| `umf_access` 0.1.0.dev7 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/umf_access-0.1.0.dev7-py3-none-any.whl` | `a78053ab8f35f55f23661de780f351f90e71299773774b62938ffb09270f3597` |
| `umf-core` 0.10.0 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/umf_core-0.10.0-py3-none-any.whl` | `49633924b45a3c8499d4fcc1a24c30d5a8fcb34d974ab019bbda987cb47da0f9` |
| `truss-toolkit` 0.0.49 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/truss_toolkit-0.0.49-py3-none-any.whl` | `7ea14b2787f02cbeec9ad99d835adbcc30c684fd5be8e0765a1d8198b50e7182` |
| `weft-sql` 0.4.52 | `/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/weft_sql-0.4.52-cp39-abi3-macosx_11_0_arm64.whl` | `598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46` |

The UMF access wheel is the public `umf-v0.5.5` 0.1.0.dev7 asset. The selected
UMF core wheel is 0.10.0 and has the existing public-wheel digest. The selected
Weft 0.4.52 artifact is the macOS arm64 wheel built from source
`32ec02f0c3fd24a97b7726500dc5992c6475908d`; its existing evidence covers the
selected profile only. This is a selected B cohort override: Ashlar's declared
`typed-sdk` extra still pins Weft 0.4.48, so this check must not be presented as
qualification of that package default. The Truss 0.0.49 wheel is the exact
package selected by the inert B proposal. These identities are inputs, not
evidence that this cohort has been installed together in a fresh B runtime.

Static wheel inspection confirmed the access wheel metadata says
`umf_access` 0.1.0.dev7 and requires Python 3.11–3.14, Pydantic 2.x >=2.13,
and typing-extensions >=4.16. The Truss wheel's optional `typed-sdk` extra
declares access dev5 and its optional `query` extra declares Weft 0.4.51;
therefore the B check must use direct, exact dev7 and 0.4.52 wheel pins, record
the deliberate selected cohort, and not claim those optional-extra ranges were
qualified by `pip check` alone.

## Required preparation before execution

The B proposal requires a new isolated Python 3.12.14 runtime and a
B-specific receipt; the existing R3 runtime receipt records access dev6 and is
historical evidence only. Reuse the exact R4 27-wheel lock at
`/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/wheelhouse-lock-pyspark-4.2.0-attempt-02.json`
(SHA-256 `d2686e1dbbdd5a136990b146b25375cba5a7c73e21ef30290cdfa12b3c70aa20`);
the B lock plan records its read-only full-file and direct-cohort comparison.
Do not re-resolve, substitute a different wheelhouse, or use the R3 virtual
environment as the B runtime.

Use only local files with package-index/network access disabled. Verify every
artifact digest before creating the isolated check environment; install the
four direct artifacts above and the separately hash-pinned dependency closure.
Capture the interpreter and platform, wheel names and hashes, installed
distribution versions, `pip check` output, test command/exit code, and exact
result hashes in a new private B runtime receipt. Keep the receipt outside the
repository and bind its absolute path and digest into later B pins only after
review.

## Focused offline checks to run after the dependency lock is frozen

Run from the repository with the fresh B check environment's Python 3.12.14.
The command below uses only existing offline Python tests. Do not set
`ASHLAR_UMF_CORE010_SOURCE`; that opts into a separate producer/checkout path
which is outside this check.

```sh
PYTHONPATH=src:tests /<approved-private-B-runtime>/venv/bin/python -m unittest -v \
  tests.test_managed_current_page_target \
  tests.test_sdk_async_bridge \
  tests.test_truss_b_install_worker.TrussBInstallWorkerTests.test_adapter_smoke_with_actual_truss_feedconsumer_and_cursor_types
```

The managed-page target test exercises the selected Weft compiler artifact
through Ashlar's UMF access API. The async bridge tests cover the existing
`umf_access` SDK boundary. The selected B test constructs the actual public
Truss `FeedConsumer` and `FeedCursor` types through a mocked assembly; it does
not open a Truss host or register a consumer. Do not broaden this command to
run the B source preflight or the source-preparation assessment. Any failure is
a compatibility failure: retain the receipt and stop; do not substitute a
fixture, alternate wheel, or package version.

## Evidence boundary and remaining owner decisions

The result, when later produced, can establish only offline package
installability/dependency consistency and the named Python API/compiler test
cases on the recorded host/runtime. It cannot establish accepted B source,
catalog/report/view readback, source epoch/namespace, consumer registration,
feed custody, ACK custody, PostgreSQL/Truss lifecycle, Spark/Delta, or hosted
runtime support.

Before a B installation is considered, an owner still must approve or replace
the exact B origin payload (`b-origin-candidate.json` currently has no approval
record), confirm the retained original source bytes are suitable for separate
B acceptance, approve the B identities/roles/request key and expected catalog
CAS, authorize an exclusive shared 5432 window after A/Arras is stopped, and
approve the unique private B paths/resource limits. A named retention custodian
and retention policy are still absent. Protected destination/receipt/ACK
authority remains a separate later gate for R04 schedule freeze. The existing
source hash is
`498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b`; the
candidate origin hash is
`2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247`. Neither
hash conveys owner approval.

## Governing evidence

- `README.md`, `b-install-attempt-proposal.json`, `b-installation-readiness.json`,
  and `source-byte-reuse-candidate-20261023.md` in this B packet.
- `../../../../02-design/contracts/CONTRACT-001-publication-boundary.md`,
  including the fresh current-core two-source and R04 slot-freeze requirements.
- `../../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md`,
  including independent source custody and complete-vector/ACK requirements.
- `../../current-upstream-release-20261023/umf-0.5.5-ashlar-access-adoption/README.md` for the public UMF access
  wheel and prior Python compatibility profile.
- `../../current-upstream-release-20261023/weft052-selected-linux/README.md` and
  `../../current-upstream-release-20261023/truss049-selected32ec-preflight/runtime-preflight.json` for the selected
  Weft 0.4.52 and Truss 0.0.49 artifact identities. Those receipts do not
  replace the future B-specific runtime receipt.
