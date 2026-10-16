# Installed original-medical commit inspection

The exact `a53f1199cf5ff54c9bcb344fa7f9eae46101f6ea` installed wheel ran one
read-only inspection against the original retained installation and PostgreSQL
source. The worker exited with status 2 after 253.959 seconds, before its
300-second native deadline. No successful commit observation, proposal or receipt
was released. This run does not qualify publication or durable ACK completion.

All 74 retained preflight records are complete pairs. Composition closing
completed in 8.815 seconds and runtime closing in 0.361 seconds. These completed
closings do not identify the primary refusal. The original worker suppressed its
exception location; a code review separately found plain JSON serialization of
native Delta history timestamps in the inspection path. Regression evidence for
that defect must not be presented as recovery of this run's missing exception.

Independent ordinary reader and protected ACK SELECT observations before and
after the run both found source head 1, one batch, ACK position 0 and no receipt.
They include closing checks and do not grant source mutation authority. No
prospective manifest reconciliation or new paid Databricks run was started.

`manifest.json` binds each exact retained original byte stream and its gzip
archive. Installation facts bind the wheel and installed files. Stage records
exclude source values, SQL and credentials. The JVM/Python diagnostic samples
remain private and are not needed to substantiate this refusal result.
