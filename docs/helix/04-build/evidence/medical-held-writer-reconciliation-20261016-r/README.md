# Installed prospective manifest capacity refusal

The installed `7104d154c8fd0a122f7285f7f70d45cd8b746674` reconciliation exited
2 after 502.958650 seconds. Its public cause was `capture-capacity`, with native
phase `begun` and status null, rather than a native deadline. No success receipt
was released. All 75 retained reconciliation stage records are preserved; no
failure-location file was produced. These records do not identify the particular
capture stream or establish a completed publication.

A subsequent observation under the actual original installation lease found the
seven original committed journal operations plus one prospective manifest
operation marked `committed`, with before-version 1 and 4,451 retained receipt
bytes. The same authorization sidecar was present. Its exact authorization SHA
is `934b3d08e760b48b3dce0a25b1547d2dd2dd9a25acd933908cb532e43a187deb`.
The retained-operation observation is SQLite metadata, not independent native
Delta receipt verification or a completed committed-attempt transition.

An independent ordinary source/ACK observation with closing found head 1, one
batch, ACK 0 and no receipt after reconciliation. Native receipt verification,
publication completion, protected ACK, settled replay and read-only reopen remain
unqualified by this attempt. Conditional exclusive-storage, complete-known-
execution and no-out-of-band-rollback assumptions remain explicit.

`manifest.json` binds 85 exact original streams and deterministic gzip copies:
original authorization/configuration, terminal attempt and stdout/stderr,
original public revalidation and request, all 75 stages, and the journal,
source/ACK and retained-operation postchecks. The [completed inspection package](../medical-held-writer-inspection-20261016-r/README.md)
separately retains the installed wheel selections, successful original
inspection/report/proposal and its before/after source observations. Those
87 streams are not duplicated here. This package contains no resumed execution.
