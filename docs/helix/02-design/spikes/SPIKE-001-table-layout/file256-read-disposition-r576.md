# Larger-file singleton comparison — R574–576

The same persistent native SQL client reads full20field carriers at private
clone0 (32 inherited64MiB-target files) and maintained2 (16 files,256MiB target).
Eight identities are selected from spaced hash prefixes00/02/04/06/08/0a/0c/0e;
each version is read for two passes with alternating order during the repeat.
Results are exact against an independent8M-node/40M-edge synthetic oracle,
including prior first/second/third change handling where applicable. All32
responses and39 exact native statements/final metrics pass independent audit.
UUID/head/metadata remain unchanged. Result cache is disabled.

| Cohort | Caller p95 ms | Engine p95 ms | Compile p95 ms | Read bytes |
| --- | ---: | ---: | ---: | ---: |
| Clone0 first |473.269334|109|239|488124161|
| Maintained2 first |372.169666|97|168|779325002|
| Clone0 repeat |418.735458|108|198|488124161|
| Maintained2 repeat |341.642084|89|142|779325002|

Every point read reports0 remote bytes; both versions read median1 file. Maintained
reads consume59.65% more bytes in this sample, despite lower observed latency.
The experiment costs2632720821 read B,0 writes/spill,22.308659375 seconds within
20GB/0/0 and180-second bounds. Nearest-rank p95 over eight queries is the maximum,
not a production-tail estimate. Version compilation and cache/order differ;
there is no isolated causal claim that file size reduced latency. Neither first
pass qualifies controlled cold-data performance.

The maintained engine cohort meets100ms in this scope, but caller342ms still
fails250ms. Prior21.108-second maintenance engine and1.504GB writes remain
charged evidence, not hidden preparation. Larger files remain a candidate rather
than a selected new default. Next compare full-field guarded mutations against
matched initial64/256 target fixtures, with identical source pins and exact
before/CDF/after preservation, recording maintenance/setup separately from the
whole publication policy. Avoid promoting a read-only or fingerprint-only win
to canonical full-guard ingest. Any improvement must remain qualified to this
2.5M-row slice and native reader3/writer7 generated-column fixture; consumer
interoperability, sustained/burst, caller/concurrent and1B/5B admission stay open.
UC Delta and deferred UMF binding remain unchanged.
