# Installed medical inspection lease lifecycle diagnosis

One read-only inspection from the exact installed
`5f3daa63f7d2a175aede8e1aa9478f20a0af1eb1` wheel exited with status 2 after
254.338 seconds, before the unchanged 300-second native deadline. The captured
native phase was `ended`. All 76 preflight stage records are complete pairs;
composition and runtime closing completed. No successful observation, proposal
or final receipt was released, and no prospective submission was attempted.

The new private diagnostic identified a `PermissionError` at
`original_medical_local_publication._operate` line 386, through
`medical_journal_inspection.renew` line 156 to
`medical_installation.renew` line 59. These exact installed source locations
identify an inspection renewal after its actual installation lease was released.
The correction must keep held renewal inside the lease interval and final
selection-only renewal outside it; a released lease must continue to refuse.

Ordinary source reader and protected ACK SELECT observations, each with closing,
found source head 1, one batch, protected ACK 0 and no receipt both before and
after inspection. They are read-only observations, not publication authority.
No paid Databricks work was started.

`manifest.json` binds every retained original and compressed byte stream,
including installed wheel facts, the bounded failure location and all stage
records. No exception messages, credentials, SQL or source values are present
in the failure-location record. This refused run does not qualify publication,
reconciliation or durable ACK completion.
