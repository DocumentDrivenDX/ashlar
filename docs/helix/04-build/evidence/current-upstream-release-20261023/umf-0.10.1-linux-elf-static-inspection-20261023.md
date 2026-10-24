---
ddx:
  id: ashlar.evidence.umf-0-10-1-linux-elf-static-inspection-20261023
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-006
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
---

# UMF 0.10.1 Linux wheel: static ELF requirements

Read-only static inspection of the published asset
`umf_core-0.10.1-cp311-abi3-linux_x86_64.whl` found SHA-256
`3512e56730d1dab59d803b20301aea3ac8f6896b0dab452f767f4c3575b9ff4c`
(3,527,255 bytes), matching the public asset record. The wheel metadata declares
`cp311-abi3-linux_x86_64` and Python `>=3.11`.

Its `_rust_core.abi3.so` member is ELF64 x86-64, 9,748,672 bytes, SHA-256
`9cca9d9bb5afd48316de234df2f831b81a1de2d9563909a91ad5433e6a7467eb`.
Static `.dynamic` and `.gnu.version_r` inspection found these dependencies and
version requirements:

| Dependency | Required versions |
| --- | --- |
| `libc.so.6` | `GLIBC_2.2.5`, `2.3`, `2.3.4`, `2.14`, `2.18`, `2.25`, `2.28`, `2.33`, `2.34` |
| `libm.so.6` | `GLIBC_2.29` |
| `ld-linux-x86-64.so.2` | `GLIBC_2.3` |
| `libgcc_s.so.1` | `GCC_3.0`, `GCC_3.3`, `GCC_4.2.0` |

The extension has no `libstdc++.so.6` dependency and no `GLIBCXX_*` or
`CXXABI_*` version requirements. Thus its minimum observed glibc symbol
requirement is `GLIBC_2.34`. This is a binary requirement, not runtime
compatibility evidence.

The adoption receipt records upstream testing on Ubuntu 24.04/glibc 2.39, but
the Linux wheel was not installed or run. Ashlar's A1 plan names Databricks
Runtime 19 as a candidate and records Spark/Delta/Scala/Java versions, not the
runtime image's glibc or loader. Whether that image satisfies this wheel's
requirements remains unknown. The wheel's `linux_x86_64` tag does not establish
a manylinux compatibility floor.

The next minimal check is to inspect loader resolution and required symbol
versions for this exact extension on the selected, approved Databricks runtime,
then perform a bounded isolated install/import check under that runtime's
Python. No Linux runtime, Databricks workload, Spark, Delta, Unity Catalog,
publication or ACK was exercised here.
