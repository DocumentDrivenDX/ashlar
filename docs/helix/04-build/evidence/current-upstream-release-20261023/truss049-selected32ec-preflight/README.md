# Truss 0.0.49 selected-Weft offline preflight

This packet preserves the Sol-reviewed offline candidate for the Truss 0.0.49
lifecycle campaign. It binds the candidate to Ashlar `11a5607e01c0a4d295db557e42c404a3b14ee085`
and source tree `a3a1c541ac89c5cc01697f503e83758d92d5d2b5`. The runtime also
pins SQL tree `3cfe250fadd42ef4b25597f0eec7d10a426ce8ff` and
`pyproject.toml` blob `170164da487d567bfa058288d32604f401125ab0`.

The selected Weft 0.4.52 asset is the locally built macOS ARM64 wheel from
source commit `32ec02f0c3fd24a97b7726500dc5992c6475908d`, SHA-256
`598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46`. Its
native extension hash is `f4be63caaacedeaa3d8af1b1f0587c561a2ff1ae0a279f4f8f7dcf836f6bb24f`.
The wheel build, packaging and compiler-only functional receipts are retained
beside the runtime and offline preflight receipts. The functional receipt
records a bounded selected test set and an earlier Ashlar commit; it is not
evidence of native SQL execution or a current-source acceptance pass.

The offline preflight completed with
`ready_pending_hash_bound_operator_approval`. It verified the current Git
objects, direct-wheel installation and RECORD payloads, the selected extension,
imports, `pip check`, the V8 probe and six UMF role proposals. Its flags report
`pg_started=false`, `spark_started=false` and `migration_apply_invoked=false`.
No native work was observed: the preflight flags are false and the attempt
directory contains no execution outputs. This is not a byte-for-byte
before/after proof that retained PGDATA stayed unchanged; no such digest
baseline was collected. A later authorized execution must cold-copy and hash
the stopped PGDATA, then verify source-before, copy and source-after digests
before PostgreSQL starts.

The historical 2026-10-22 packet at
`../../current-upstream-release-20261022/truss-049-lifecycle-ready/` remains
unchanged. This packet contains no wheel, virtualenv, PGDATA, source model,
seed request contents or read-only helper contents. The exact preparation and
preflight records retain their documented hashes and custody references.

Sol's review approves this offline preparation and its evidence only. It is
not an execution GO and does not establish source acceptance, Linux or Unity
Catalog support, R04 support, or a Weft release. Any later execution requires
a fresh hash-bound approval for the exact reviewed files in this packet.
