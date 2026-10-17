# Typed SDK local Delta bulk failure paths

This correction supplies the missing `ActionConflict`, `ActionIndeterminate` and
`ActionTimedOut` imports in the existing local Delta bulk executor. Governing
behavior is [the local Delta action contract](../../../02-design/contracts/typed-sdk-delta-actions.md)
and UMF STP-086 AC3 (conflict/commit loss), with the deadline case restricted to
pre-submission admission expiry. This does not qualify managed Delta, native
cancellation, Truss authority, broader carriers or temporal behavior.

The expanded native harness uses the actual current Weft-generated bulk catalog.
A separate Delta commit after the captured preimage must raise ActionConflict
without retry or quarantine. Response loss after an actual native commit must
raise ActionIndeterminate, retain the original receipt and count markers and
quarantine admission against resubmission. The deadline test forces the actual
LocalDeltaActionAdmission expiry gate before connection acquisition; it proves
error classification with no submission, not native lock/cancellation behavior.

Static red/green: baseline Ruff F821 identifies exactly three missing exception
names; changed source and harness have zero F821 findings. Syntax and whitespace
checks pass. `preconnection-timeout.json` records execution through an actual
compiled generated bulk method with zero connection calls.

`prior-stale-build/` retains the first 22-pass native execution and its packaging
limitation. The pre-existing build tree contributed a stale package file absent
current source; that wheel is not final source-qualified evidence. The entire
old build tree and wheel remain preserved in task scratch. A clean wheel was
rebuilt, installed independently and checked against all 410 current package
payloads; `clean-wheel-identity.json` binds their exact hashes. Clean-wheel native
replay passed all 22 checks on the second attempt, exit 0. Owned PostgreSQL and
Spark stopped successfully. `source-pins.json`, `verification.json` and
`original-bulk-program.json` retain this exact source-qualified final cohort.
Neither native attempt failed; the first wheel was rejected for packaging closure.

Temporal qualification remains **HELD**.
