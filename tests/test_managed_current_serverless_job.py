# SPDX-License-Identifier: MIT OR Apache-2.0
"""Declarative managed-current Jobs admission checks; no API or compute calls."""
import unittest

from ashlar_host.managed_current_serverless_job import (
    ManagedCurrentServerlessJobSpec,
    admit_serverless_job_readback,
)


def _spec(**changes):
    values = {
        "name": "managed-current-a1-candidate",
        "notebook_path": "/Workspace/Shared/managed-current-trial",
        "environment_version": 4,
        "dependencies": ("/Volumes/client_dev/trial/wheels/ashlar.whl",),
        "timeout_seconds": 900,
        "estimated_total_cost_usd": 8.0,
        "cleanup_reserve_usd": 1.0,
        "budget_policy_id": "550e8400-e29b-41d4-a716-446655440000",
    }
    values.update(changes)
    return ManagedCurrentServerlessJobSpec(**values)


def _readback(spec):
    settings = spec.build_settings()
    return {"job_id": 123, "settings": settings,
            "effective_budget_policy_id": spec.budget_policy_id}


class ManagedCurrentServerlessJobTests(unittest.TestCase):
    def test_builds_single_unscheduled_serverless_notebook_job(self):
        spec = _spec()
        settings = spec.build_settings()
        self.assertEqual(settings["max_concurrent_runs"], 1)
        self.assertEqual(settings["max_retries"], 0)
        self.assertIs(settings["retry_on_timeout"], False)
        self.assertEqual(settings["timeout_seconds"], 900)
        self.assertIs(settings["disable_auto_optimization"], True)
        self.assertEqual(len(settings["tasks"]), 1)
        self.assertEqual(settings["tasks"][0]["timeout_seconds"], 900)
        self.assertNotIn("schedule", settings)
        self.assertNotIn("job_clusters", settings)
        admitted = admit_serverless_job_readback(_readback(spec), spec)
        self.assertEqual(admitted["jobId"], 123)
        self.assertEqual(admitted["dependencyPaths"], list(spec.dependencies))

        # Jobs GET omits default-valued zero retries and false timeout retry.
        job = _readback(spec)
        del job["settings"]["max_retries"]
        del job["settings"]["retry_on_timeout"]
        self.assertEqual(admit_serverless_job_readback(job, spec)["taskCount"], 1)

    def test_rejects_timeout_cost_and_unscoped_dependency_inputs(self):
        for spec in (
            _spec(timeout_seconds=901),
            _spec(estimated_total_cost_usd=90.0, cleanup_reserve_usd=11.0),
            _spec(dependencies=("dbfs:/FileStore/ashlar.whl",)),
        ):
            with self.subTest(spec=spec):
                with self.assertRaises(ValueError):
                    spec.build_settings()

    def test_readback_rejects_schedule_retry_classic_compute_and_drift(self):
        spec = _spec()
        mutations = (
            lambda settings: settings.update(schedule={"quartz_cron_expression": "* * * * *"}),
            lambda settings: settings.update(max_retries=1),
            lambda settings: settings["tasks"][0].update(existing_cluster_id="cluster"),
            lambda settings: settings["tasks"][0].update(timeout_seconds=901),
            lambda settings: settings["environments"][0]["spec"].update(
                dependencies=["/Volumes/client_dev/other.whl"]),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                job = _readback(spec)
                mutate(job["settings"])
                with self.assertRaises(ValueError):
                    admit_serverless_job_readback(job, spec)

    def test_cost_over_preferred_budget_requires_policy_and_readback(self):
        with self.assertRaises(ValueError):
            _spec(estimated_total_cost_usd=9.0, cleanup_reserve_usd=2.0).build_settings()
        spec = _spec(estimated_total_cost_usd=9.0, cleanup_reserve_usd=2.0,
                     budget_policy_id="550e8400-e29b-41d4-a716-446655440000",
                     preferred_budget_exception="Owner accepted the >$10 estimate")
        settings = spec.build_settings()
        job = {"job_id": 123, "settings": settings,
               "effective_budget_policy_id": spec.budget_policy_id}
        self.assertEqual(admit_serverless_job_readback(job, spec)["budgetPolicyId"],
                         spec.budget_policy_id)
        job["effective_budget_policy_id"] = "other-policy"
        with self.assertRaises(ValueError):
            admit_serverless_job_readback(job, spec)


if __name__ == "__main__":
    unittest.main()
