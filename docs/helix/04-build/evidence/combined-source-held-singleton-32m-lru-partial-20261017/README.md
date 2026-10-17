# Combined-source held singleton: bounded partial attempt

The installed 22ad8ac8 read used the explicit 32 MiB capture ceiling and bounded value-only LRU. Root session 63800 (tool `da642b`) terminated with `TimeoutError` at the 600-second work bound. No singleton receipt, runner result, or native-after observation exists; this is not a successful read or native/source/ACK equality proof.

The original terminal records cleanup failure: `PermissionError` during confirm-absence after session leader 50008 was reaped. A separate later root observation (tool `7c7e85`) found no members of that exact group and zero-signal lookup returned ESRCH. That later observation does not change the original cleanup result.

The bounded opening-exception diagnostic retained no records and reported no diagnostic failure. The final trace retained 96 events, dropped 11,254, and recorded three ledger calls. Its nested inclusive durations are not additive and do not establish successful admission or isolated query latency.

`actual.tar.gz` retains 24 exact control, diagnostic, terminal, and bounded stream files; `manifest.json` records their original and archive hashes. `parent-observations.json` explicitly transcribes supplied root observations rather than fabricating a raw process snapshot. `summary.json` records absent outputs and qualifications. Credentials, binaries, SDK inventories, and repeated source/receipt payloads are excluded. The unchanged 27-receipt capacity diagnosis is referenced by hash from the earlier 707 evidence; it is not a fresh native authority observation.
