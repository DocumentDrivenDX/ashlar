# Databricks notebook source
# SPDX-License-Identifier: MIT OR Apache-2.0
import json

dbutils.widgets.text("task_parameters_json", "")
parameters = json.loads(dbutils.widgets.get("task_parameters_json"))
from ashlar_host.managed_a1_serverless_tasks import verifier_task

result = verifier_task(spark, parameters)
dbutils.notebook.exit(json.dumps(result, sort_keys=True, separators=(",", ":")))
