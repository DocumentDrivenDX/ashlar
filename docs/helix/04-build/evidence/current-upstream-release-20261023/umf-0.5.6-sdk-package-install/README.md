# UMF SDK 0.5.6 package-manager install

**Result:** the standard SDK package-manager consumer install completed successfully on the local macOS arm64 host. This is bounded package/install evidence for the frozen candidate; it does not qualify native Spark or a hosted release matrix.

## Source and package custody

| Item | Identity |
| --- | --- |
| UMF candidate commit | `9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08` |
| Candidate tree | `ae458a8f2cebd50ed13c04916a509d801bac1664` |
| Public 0.5.5 base | `009c042545a17b49ea857eff22d0fcf3fd71ef44` |
| SDK archive | `/private/tmp/umf-public-main-0.5.5/.cache/sdk-distribution/umf-sdk.tgz` |
| SDK archive SHA-256 | `656289421dbf5231a9ea589af4f709b651666fd88db4135aaf69119205c1be42` |
| Install-check script | `scripts/test-sdk-install.ts` at the candidate checkout |
| Script SHA-256 | `ea44304fcbaa89d6df8f2b551260ebb63812d2099a1f07cf7de3367b31a0278e` |

## Install execution

The recorded command was:

```sh
TMPDIR=/private/tmp bun scripts/test-sdk-install.ts
```

It ran with Bun 1.4.3 and exited 0 in 7.8 seconds. The check installed 217 packages, including `@umf/core` 0.5.6 from the candidate tarball. Its reported checks were `installedCoreDistribution.files=1751`, `unifiedGenerator=true`, `conformanceCases=139`, `singleInstall=true`, `bundledActions=true`, and `loaderEntrypoint=true`.

The installer emitted a peer-dependency warning and, because the check uses `--ignore-scripts`, one package postinstall was skipped. Neither caused a test failure. This receipt records the successful local install; no hosted run or native Spark execution is claimed. The separate six-role source-byte correspondence check is retained in the [candidate bridge receipt](../umf-0.5.6-local-delta4-bridge/README.md).
