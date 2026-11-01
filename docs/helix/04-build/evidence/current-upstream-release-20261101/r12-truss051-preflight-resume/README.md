# R12: actual-worker effect-free preflight

R11 stopped before effect: the worker required the wrong runtime-proof filename. R12 binds `r12-runtime-proof.json` and runs the real continuation worker through `--preflight` before any output directory or PostgreSQL process can be created.

The preflight validates the current SDK 0.10.1 / Truss 0.0.51 / UMF Core 0.10.5 runtime, exact R12 scope and public API signatures, the retained R10 migration receipts and registry, stopped R10 PGDATA ownership/layout/hash, process custody, free-space bounds, seed/model inputs, role identifiers, and supervisor namespace. It prints a machine-readable result and returns before output creation, database contact, or PostgreSQL startup. The preflight is repeatable and does not consume the local standing authorization.

Seven focused tests pass, including a subprocess test that invokes the actual R12 worker with the R12 scope and confirms every configured R12 output path remains absent. The checker and validation receipt record the actual worker result. Native dispatch remains disabled pending Sol review of this exact R12 candidate.

- Scope: `r12-truss051-resume-scope.json`
- Worker: `r12_truss051_local_worker.py`
- Supervisor: `r12_truss051_supervisor.py`
- Actual-worker preflight checker: `check_r12_preflight.py`
- Focused tests: `test_r12_preflight.py`
- Validation receipt: `r12-preflight-validation.json`
- Standing authorization binding: `r12-standing-authorization-dispatch-receipt.json`

The current-main rebinding keeps the original 20-file SDK0101 commit hashes and source-custody correction checks intact. The worker additionally requires the exact reviewed manifest/recovery follow-on commits `6e40a1ac`, `3f6c3541`, `d723eeeb`, and `477fabec`, including their exact Git tree objects. The resulting integration paths are pinned to exact commit content hashes: `driver.py`, `configured_local_delta4_publication.py`, `configured_local_runtime.py`, `local_current_carrier_profile.py`, `typed_graph_admission_sdk0101.py`, and `tests/test_umf_sdk0101_pins.py`. The integration files include reviewed, committed manifest/recovery work: `configured_local_delta4_publication.py` and `configured_local_runtime.py` are pinned to commit `477fabec`; `local_current_carrier_profile.py` is pinned to `6b0bd06e`. The worker requires those exact content hashes and commit ancestry, so unrelated or unreviewed edits still fail closed. This rebinding does not change the R12 standing-authorization review state or dispatch flag.

The current-main source amendment is also pinned: R12 requires the exact trees for reviewed commits `d0c5bebf` and `7a49ab52`, and the exact `typed_graph_admission_sdk0101.py` blob at `7a49ab52`. This carries the temp-root admission fix into the preflight without changing the earlier SDK0101 or manifest/recovery lineage. Dispatch remains pending and disabled.
