# Combined-source ordinal 2 timeout

The human-authorized E continuation exited 1 after 185.147 seconds under its unchanged 180-second native bound. Its inner terminal reports `TimeoutError`, no worker status and incomplete execution. No final receipt was produced.

The public observer retained 11 records: writer acquisition, ledger opening, run identity, producer admission and prior-prefix native admission each completed; required closing began without an end record. These labels locate completed observed intervals, not the timeout's cause or an unobserved publication/ACK outcome.

Original cleanup reported `PermissionError` during `confirm-absence`, with session leader 65128 reaped but its numeric group still recorded as remaining. A later read-only observation found no matching numeric rows for that group. This separate later observation does not convert the failed original cleanup into confirmed success.

[The safe summary](summary.json) preserves those exact records and hashes of private inputs. Configuration, credentials and raw logs remain private. Current source/publication state, durable ACK outcome, timeout cause and completed closing remain unknown; no unchanged-state or successful-continuation claim is made. No retry was performed as part of retention.

A separate bounded read-only local ledger observation records ordinals 0/1 completed with descriptors, ordinal 2 retained without a descriptor, and 3–7 unprepared. This local observation is unauthoritative: it performs no native/source/ACK validation, proves no unchanged-effect outcome and does not authorize replay.
