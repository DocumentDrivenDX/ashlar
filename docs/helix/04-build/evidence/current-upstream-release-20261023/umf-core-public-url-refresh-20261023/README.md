# UMF core 0.10.0 public asset URL refresh

Ashlar's `typed-sdk` extra now resolves `umf-core` 0.10.0 from the public
`umf-v0.5.5` release instead of `umf-v0.5.4`. The artifact digest is unchanged:
`49633924b45a3c8499d4fcc1a24c30d5a8fcb34d974ab019bbda987cb47da0f9`.
The staged wheel at `/private/tmp/umf-0.5.6-release-handoff/assets/umf_core-0.10.0-py3-none-any.whl`
was hashed locally and matched that digest. The selected access package remains
public `umf_access` 0.1.0.dev7; no unpublished UMF asset was selected.
Sol independently checked the [published v0.5.5 release asset](https://github.com/DocumentDrivenDX/umf/releases/tag/umf-v0.5.5): GitHub reports the exact wheel name, 71,258-byte size and the same SHA-256 digest at the selected URL.

The focused checks used the isolated current-cohort Python 3.12.14 runtime with
UMF core 0.10.0, access 0.1.0.dev7 and Weft 0.4.53. The command
`PYTHONPATH="$PWD/src:$PWD/tests" /private/tmp/ashlar-truss049-current-cohort-20261023/venv/bin/python -m unittest -v test_configured_typed_graph_sdk_binding test_managed_current_page_target test_sdk_async_bridge`
passed 25 tests. These exercise Ashlar's typed SDK binding and page-target
admission against the selected package cohort; they do not test Spark, Delta,
Truss lifecycle, Unity Catalog or publication/ACK.

UMF PR #47, which proposes Python SDK 0.10.1, was checked on 2026-10-23 and
remained open. This refresh does not adopt its unpublished platform wheels or
claim 0.10.1 compatibility. Re-evaluate only after public release assets are
available and their platform-specific bytes are verified.
