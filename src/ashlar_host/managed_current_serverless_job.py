# SPDX-License-Identifier: MIT OR Apache-2.0
"""Build and inspect an inert, one-task serverless Jobs definition.

This module makes no Jobs API calls. The caller supplies the selected notebook,
serverless environment version, and immutable Workspace/Volume dependencies.
See the Jobs API and serverless dependency constraints:
https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/jobs
https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies
"""
from dataclasses import dataclass
import math
import re
from typing import Optional


_MAX_SECONDS = 900
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z")
_TRIAL_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,63}\Z")


def _require(value, message):
    if not value:
        raise ValueError(message)


def _workspace_path(value, *, suffix=None):
    if type(value) is not str or "\0" in value:
        return False
    if not (value.startswith("/Workspace/") or value.startswith("/Volumes/")):
        return False
    if suffix is not None and not value.endswith(suffix):
        return False
    return True


def _dependency_path(value):
    if _workspace_path(value, suffix=".whl"):
        return True
    if type(value) is str and value.startswith("-r "):
        value = value[3:]
    if not _workspace_path(value, suffix=".txt"):
        return False
    filename = value.rsplit("/", 1)[-1]
    return filename == "requirements.txt" or filename.startswith("requirements-")


@dataclass(frozen=True)
class ManagedCurrentServerlessJobSpec:
    """Caller-selected inputs for an unscheduled single notebook task.

    `dependencies` are absolute Workspace/Volume paths to immutable wheels or
    requirements files. A requirements file may select architecture-specific
    wheel URLs with pip environment markers. Native wheel architecture coverage
    remains an external admission fact; a path alone does not prove it.
    """

    name: str
    trial_id: str
    notebook_path: str
    environment_version: int
    dependencies: tuple[str, ...]
    timeout_seconds: int = 900
    estimated_total_cost_usd: float = 0.0
    cleanup_reserve_usd: float = 0.0
    budget_policy_id: Optional[str] = None
    preferred_budget_exception: Optional[str] = None

    def build_settings(self):
        _require(type(self.name) is str and self.name.strip() and "\0" not in self.name,
                 "A stable job name is required")
        _require(type(self.trial_id) is str and _TRIAL_ID.fullmatch(self.trial_id),
                 "A unique managed trial ID is required for usage reconciliation")
        _require(_workspace_path(self.notebook_path),
                 "Notebook must resolve to an absolute Workspace path")
        _require(type(self.environment_version) is int and self.environment_version > 0,
                 "A read-back-confirmed serverless environment version is required")
        _require(type(self.dependencies) is tuple and self.dependencies
                 and all(_dependency_path(path) for path in self.dependencies)
                 and len(set(self.dependencies)) == len(self.dependencies),
                 "Dependencies must be unique absolute Workspace/Volume wheel or requirements paths")
        _require(type(self.timeout_seconds) is int and 1 <= self.timeout_seconds <= _MAX_SECONDS,
                 "Job and task timeout must be between 1 and 900 seconds")
        _require(type(self.estimated_total_cost_usd) in (int, float)
                 and math.isfinite(self.estimated_total_cost_usd)
                 and self.estimated_total_cost_usd > 0
                 and type(self.cleanup_reserve_usd) in (int, float)
                 and math.isfinite(self.cleanup_reserve_usd)
                 and self.cleanup_reserve_usd > 0
                 and self.estimated_total_cost_usd + self.cleanup_reserve_usd <= 100,
                 "Estimated task cost plus cleanup reserve must fit the $100 operational ceiling")
        _require(self.budget_policy_id is None or
                 type(self.budget_policy_id) is str and _UUID.fullmatch(self.budget_policy_id),
                 "Budget policy ID must be a UUID when supplied")
        if self.estimated_total_cost_usd + self.cleanup_reserve_usd > 10:
            _require(type(self.preferred_budget_exception) is str
                     and self.preferred_budget_exception.strip()
                     and len(self.preferred_budget_exception.encode("utf-8")) <= 1024,
                     "An estimate above the preferred $10 budget needs an owner rationale")
        else:
            _require(self.preferred_budget_exception is None,
                     "Preferred-budget rationale must be omitted at or below $10")
        settings = {
            "name": self.name,
            "tags": {"project": "ashlar", "purpose": "managed-current-a1",
                     "ashlar_trial_id": self.trial_id},
            "max_concurrent_runs": 1,
            "max_retries": 0,
            "retry_on_timeout": False,
            "timeout_seconds": self.timeout_seconds,
            "tasks": [{
                "task_key": "managed_current_trial",
                "timeout_seconds": self.timeout_seconds,
                "max_retries": 0,
                "retry_on_timeout": False,
                "notebook_task": {"notebook_path": self.notebook_path},
                "environment_key": "managed_current_trial_env",
                # Serverless auto-optimization can add retries beyond the
                # retry policy. Disabling it preserves at-most-once semantics.
                "disable_auto_optimization": True,
            }],
            "environments": [{
                "environment_key": "managed_current_trial_env",
                "spec": {
                    "environment_version": str(self.environment_version),
                    "dependencies": list(self.dependencies),
                },
            }],
        }
        if self.budget_policy_id is not None:
            settings["budget_policy_id"] = self.budget_policy_id
        return settings


def admit_serverless_job_readback(job, spec):
    """Fail closed unless Jobs GET readback matches the generated safety shape.

    This checks API configuration only. It does not prove the workspace runtime,
    wheel bytes/architecture, source authority, actual spend, or hard invoice cap.
    """
    _require(type(job) is dict and type(spec) is ManagedCurrentServerlessJobSpec,
             "Jobs readback and original job specification are required")
    expected = spec.build_settings()
    settings = job.get("settings")
    _require(type(settings) is dict, "Jobs GET response omitted settings")
    _require(settings.get("name") == expected["name"]
             and settings.get("max_concurrent_runs") == 1
             and settings.get("max_retries", 0) == 0
             and settings.get("retry_on_timeout", False) is False
             and settings.get("timeout_seconds") == expected["timeout_seconds"]
             and type(settings.get("tags")) is dict
             and settings["tags"].get("ashlar_trial_id") == spec.trial_id,
             "Jobs readback does not enforce the bounded single-run policy")
    _require("schedule" not in settings and "trigger" not in settings
             and "triggers" not in settings,
             "Managed current trial job must be unscheduled and untriggered")
    _require("job_clusters" not in settings and "compute" not in settings,
             "Serverless task must not declare classic job compute")
    tasks = settings.get("tasks")
    _require(type(tasks) is list and len(tasks) == 1,
             "Managed current trial must contain exactly one task")
    task = tasks[0]
    expected_task = expected["tasks"][0]
    _require(type(task) is dict and task.get("task_key") == expected_task["task_key"]
             and task.get("notebook_task") == expected_task["notebook_task"]
             and task.get("environment_key") == expected_task["environment_key"]
             and task.get("timeout_seconds") == expected_task["timeout_seconds"]
             and task.get("max_retries", 0) == 0
             and task.get("retry_on_timeout", False) is False
             and task.get("disable_auto_optimization") is True
             and "existing_cluster_id" not in task and "job_cluster_key" not in task
             and "new_cluster" not in task and "sql_warehouse_id" not in task,
             "Task readback differs from the selected serverless notebook task")
    _require(settings.get("environments") == expected["environments"],
             "Notebook-scoped selected dependency environment differs from Jobs readback")
    if spec.budget_policy_id is not None:
        _require(settings.get("budget_policy_id") == spec.budget_policy_id
                 and job.get("effective_budget_policy_id") == spec.budget_policy_id,
                 "Selected budget attribution policy is not effective on this job")
    _require(type(job.get("job_id")) is int and job["job_id"] > 0,
             "Jobs readback omitted a valid job ID for usage reconciliation")
    return {"jobId": job.get("job_id"), "trialId": spec.trial_id,
            "taskKey": expected_task["task_key"], "taskCount": 1,
            "timeoutSeconds": expected["timeout_seconds"],
            "dependencyPaths": list(spec.dependencies),
            "budgetPolicyId": job.get("effective_budget_policy_id")}
