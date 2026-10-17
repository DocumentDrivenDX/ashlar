# Installed configured-source publication reads

The installed `ashlar run-configured-local` command selected `operation: "read"`
against the retained installation from `configured-local-installed-20261016`.
It did not publish or replay data. The wheel was built from
`da5a574d34b529a4bf8a1775e9e3b7cf8945dec5` and installed offline without changing
native runtime dependencies.

| Selection | Result | Whole command seconds |
| --- | --- | ---: |
| Ordinal 1, object 1 | Complete original row before its later update | 22.720 |
| Ordinal 3, object 1 | Complete updated row, different from ordinal 1 | 15.018 |
| Ordinal 3, object 2 | null after its deletion | 15.690 |

The actual public reports retain full row fields and exact selected manifests.
The command checks complete source-derived carrier bags, all five original
committed phases, local progress correspondence, the exact UUID/version vector
and current native/lease/retention admission before releasing a row or absence.
The independent read implementation review confirmed that no mutation or
checkpoint advancement path is used. The source is the unchanged configured
STRING fixture with development bindings, not an accepted Truss source.

`before.json` and `after.json` are equal: all six native Delta heads, 26 journaled
native operations, three effect plans and three local progress rows remained
unchanged. Both old and current reads report the latest local checkpoint `1828`;
that does not alter the older selection's manifest checkpoint. There were no
additional ingest commands, native benchmark runs or cloud calls.

These durations include startup, fresh UMF preflight and shutdown. They are not
singleton SQL latency measurements. The profile qualifies this small local
STRING read path under a cooperative installation lease. It does not qualify
Unity Catalog, PostgreSQL pins, remote fencing or a protected producer ACK.
The operator guide is `examples/configured-local/README.md`.
