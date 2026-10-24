# UMF 0.10.1 published asset adoption (2026-10-24)

Public UMF release `umf-v0.5.6` is published from commit
`259bf1ed91466ff7e9782d7517c14f2112282026`. The Python distribution is
0.10.1; its UMF document/core semantics remain 0.10.0. Both platform assets selected by Ashlar's `umf-core` markers were
downloaded from that release and matched the release asset sizes and SHA-256
values. The macOS arm64 wheel was installed into a fresh CPython 3.12.14
environment; its direct URL metadata records the exact wheel digest, its
RECORD payload hashes verified, and `pip check` reported no broken
requirements. A minimal core-model validation/round-trip and the focused
`tests.test_typed_graph_source` suite passed (2 tests).

The Linux x86_64 asset was byte-verified only; it was not installed or run.
Upstream's release qualification names Ubuntu 24.04, glibc 2.39 and Python
3.11. This does not establish Ashlar compatibility on Linux, Databricks
runtime compatibility, or support for Windows, Intel macOS or other platforms.
The `cp311-abi3` wheel tag is an asset compatibility tag, not an Ashlar runtime
qualification. No Spark, Delta, Truss lifecycle, Unity Catalog, publication,
or ACK behavior was exercised by this package adoption check.

Ashlar's selected `typed-sdk` dependency marker now names only the verified
macOS arm64 and Linux x86_64 release assets. The semantic UMF model used by
Ashlar remains pinned to version 0.10.0 where specified by source documents;
the SDK package distribution is 0.10.1. Public asset and isolated install
details are in [the machine-readable receipt](install-receipt.json) and
[asset record](public-assets.json).
