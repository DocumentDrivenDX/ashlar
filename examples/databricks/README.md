# Databricks notebooks (experimental)

Four notebook entry points for the managed (Databricks serverless) publication path:
`initialize.py`, `publish.py`, `verify.py` and `runtime_probe.py`. Each reads a
`task_parameters_json` widget and calls into `ashlar_host.managed_a1_serverless_tasks`.

**Experimental and not tested against a workspace.** CI has no Databricks workspace, so
nothing here has been run by CI. The Python task code these notebooks call is covered by
mocked unit tests only. The live-trial runner that used to generate the task parameters
was removed; you must write the parameters yourself.
