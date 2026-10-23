# B offline runtime attempt 02 partial result

Status: installation succeeded and a later read-only post-install verification
issued a B runtime receipt. The earlier receipt gate stopped on a nonfatal pip
cache warning; the receipt records that warning and the successful pip-check
exit without rerunning pip. A separate focused test receipt then recorded 24/24
offline SDK/API checks. The install receipt proves installation/RECORD
integrity; the test receipt records the separate compatibility scope.

The fresh target was
`/private/tmp/ashlar-r04-b-install-20261023-01/runtime/venv-attempt-02`, created
from CPython 3.12.14. The R4 CPython 3.12.14/macOS arm64 lock digest was
`d2686e1dbbdd5a136990b146b25375cba5a7c73e21ef30290cdfa12b3c70aa20`; all 27
wheel file digests and wheel metadata identities were checked before
installation. No wheel contained a `.pth` member. The attempt used
`--no-index --find-links <locked-wheelhouse> --only-binary=:all: --no-deps
--require-hashes`, with zero retries. Pip exited 0 after 55.92 seconds. The
observed venv size was 722,825,958 bytes, below the 2 GiB runtime cap.

The `pip check` process exited 0 and reported `No broken requirements
found.` It prepended a cache-permission warning for
`/Users/erik/Library/Caches/pip`, which the sandbox does not permit this process
to use. The initial runner required exact stdout without a warning, so it
stopped before installed `RECORD` verification. A separate read-only verifier
then validated all 6,079 installed `RECORD` payload rows across 28
distributions (the 27 locked distributions plus pip), all recorded hashes and
sizes, package versions, and seven selected imports. It also found no installed
`.pth` files. It did not rerun pip or mutate the venv. The warning was
classified as nonfatal because pip exited 0 and emitted its dependency-success
line; the receipt preserves the complete warning and stdout.

Post-install receipt:

- Path: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/runtime-receipt-attempt-02.json`
- SHA-256: `e96705451e82392657aafc956e3e85e2975e5e8b56a2658b3a93d9fd3d2f18ce`
- Truss catalog manifest SHA-256: `288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`
- `pg_ctl` SHA-256: `59b641aabae44b43acfc34fd0bdeb9af9480338d39b25f3088d7c486fdd8b999`

The receipt is labeled `verified_post_install_read_only` and records that
SDK/API tests were not run at install-receipt creation time. The later test
receipt is labeled separately:

- Path: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/sdk-api-attempt-02-receipt.json`
- SHA-256: `6ff09683c4f7fd7a2c28c7db3234d732eaffafc9a2dd2065a5b836e95787c8f`
- Result log SHA-256: `5063673e45454ca9b23db2b94d77f76404073f94b7551a2abe886d9b8e70a265`
- Result JSON SHA-256: `a0e231c3251b7afddbb926fb9a4d63269af46e92755135c0c9fb08687c68e016`
- Result: 24 tests passed, zero failures/errors/skips; test sources were unchanged.

It records one in-process Weft compiler call through mini-racer; it started no
PostgreSQL, Spark, Truss host, feed, destination, or ACK and performed no B
source acceptance. Neither receipt establishes source authority, acceptance,
provider identity, or native service behavior.

The logs and plan are retained at:

- Preflight: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/attempt-02-preflight.json`
- Install: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/attempt-02-logs/install.log`
- Pip check: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/attempt-02-logs/pip-check.log`
- Locked requirements: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/attempt-02-locked-requirements.txt`
- Read-only verifier: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/verify-attempt-02-postinstall.py`
- Focused test result: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/sdk-api-attempt-02-r1-result.json`
- Focused test harness: `/private/tmp/ashlar-r04-b-install-20261023-01/runtime/run-sdk-api-tests-attempt-02-r1.py`

Execution approval remains null, the origin remains unapproved, and no B output
root or PGDATA was created.
