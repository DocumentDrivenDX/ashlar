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
