# Concrete held Spark provider

`SparkCurrentQueryBackend` implements bounded positional query capture, unchanged
compiler SQL/parameter execution, complete pinned source-frame construction and
mandatory native refinement before public projection. Supply an explicit Spark
session, authority owner and separate `PathCaptureConfig` result/source limits,
then pass it to `execute_databricks_current_query` with the same selected mode.
The caller owns Spark lifetime; the authority context owns opening and closing
caller, policy, schema/UUID, source/action and publication-cut admission.

Six focused unit controls cover unchanged parameters and repeated output names,
mandatory refinement and bad reports, schema/capacity refusal, missing checker,
cancellation/closing and foreign or closed capture use. The retained `run.py`
and `summary.json` additionally exercise the actual public Weft 0.4.35 ARM64
CQP/checker through this provider in two controls (0.005 seconds): matching public
count returns and wrong count refuses even with adapter refinement disabled.
Both paths close. These controls use inert Spark/authority and supplied source
scans; they qualify ABI composition, not native SQL or source/UC authority.
The separate [three-row Delta observation](../current-source-capture-20261015/README.md)
qualifies actual local source capture. No combined native managed-UC claim follows.

The Spark session receives the emitted SQL without repair. Missing installed
checker, incomplete source capture, changed binding, wrong native result, capacity
overflow or failed closing releases no public result. Private contributor carriers
remain internal and result columns retain original positions and repeated names.
