# UMF 0.5.5 Python access adoption

This receipt records the bounded Ashlar `typed-sdk` dependency update at Ashlar
revision `31f9fd56e6785ec3cf3fd9699d2218206faa7ee1`. Public UMF tag
`umf-v0.5.5` resolves to `009c042545a17b49ea857eff22d0fcf3fd71ef44`.

Ashlar now selects the release's `umf_access` 0.1.0.dev7 wheel by immutable
release URL and SHA-256. The public `umf_core` 0.10.0 wheel remains pinned to the
same bytes as before. Downloaded assets were checked locally:

| Artifact | SHA-256 |
| --- | --- |
| `umf_access-0.1.0.dev7-py3-none-any.whl` | `a78053ab8f35f55f23661de780f351f90e71299773774b62938ffb09270f3597` |
| `umf_core-0.10.0-py3-none-any.whl` | `49633924b45a3c8499d4fcc1a24c30d5a8fcb34d974ab019bbda987cb47da0f9` |
| Selected local macOS `weft_sql` 0.4.52 wheel used by the managed-page tests | `598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46` |

## Verification

In an isolated Python 3.12.14 environment, the exact access and core wheels
above were installed with the selected local Weft 0.4.52 wheel. The current
Weft 0.4.52 profile is used for this compatibility check because the managed
current-page target tests admit 0.4.50/0.4.52; Ashlar's declared `typed-sdk`
Weft pin remains 0.4.48.

The focused command ran 27 tests successfully in 20.938 seconds:

```sh
ASHLAR_UMF_CORE010_SOURCE=/private/tmp/ashlar-umf055-adoption/umf-source \
PYTHONPATH="$PWD/src:$PWD/tests" \
/private/tmp/ashlar-umf055-adoption/venv/bin/python -m unittest -v \
  test_managed_current_page_target test_sdk_async_bridge \
  test_managed_current_campaign test_typed_graph_admission_real010
```

The source checkout supplied to the real-core producer test was detached at
UMF commit `2ede4c879bea5db13794a6fa30e67c821306c17`; the test exercised the
actual core 0.10 producer transport. The other tests cover current managed-page
identity admission, async SDK bridge/custody refusals, and managed campaign
admission controls.

## Scope

This establishes focused Python dependency/API compatibility for the tested
profile only. It does not qualify UMF G01, Spark or Delta execution, Unity
Catalog, managed-runtime behavior, Truss acceptance/feed/ACK, or a release
artifact for Ashlar. The exact core wheel and Ashlar Weft pin were not changed.
