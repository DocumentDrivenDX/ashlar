# Combined-source continuation

The original retained run has ordinals 0 and 1 completed, ordinal 2 retained,
and ordinals 3–7 unprepared. A bounded attempt with one local worker and one
Delta snapshot/shuffle partition stopped at 120.011 seconds, produced no result,
and left the original ledger state and transaction digests unchanged. Its owned
Python and Spark processes were stopped. No new publication or ACK is claimed.

The continuation now observes all completed-prefix descriptors in one owner-scoped
native ledger pass while preserving each stored-publisher request, artifact,
descriptor and identity check. Pending publication and final admission remain
unchanged. Two basic controls cover one-pass orchestration and closing inventory
drift; 17 new/affected tests pass. Module boundaries and independent review pass.
This implementation has not been rerun natively and has no measured speedup claim.
