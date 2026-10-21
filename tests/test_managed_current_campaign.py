# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert campaign routing and namespace bootstrap tests."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_campaign import run_managed_current_campaign
from ashlar_host.managed_current_trial import (
    ManagedCurrentTrialConfig, run_managed_current_trial_page,
)


def _config():
    environment = {
        "runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
        "sparkBuildText": "Spark build test", "runtimeTag": None,
        "principal": "owner@example.test", "catalog": "client_dev",
        "schema": "trial_campaign", "protocol": "trial-reader/0.1",
    }
    return ManagedCurrentTrialConfig(
        catalog="client_dev", schema="trial_campaign", stream="typed-graph-feed",
        predecessor="catalog-page-1", schedule_sha256="a" * 64, schedule_step=2,
        stage_id="positive-page", schema_revisions_json='{"typed-graph":"1"}',
        materialized_at="1792606778180666", expected_environment=environment,
        maximum_task_seconds=600, estimated_total_dbu=2.0, authorized_total_dbu=3.0,
        estimated_total_cost_usd=2.0, reserve_dbu_for_cleanup=1.0,
        reserve_usd_for_cleanup=1.0, cost_estimate_includes_startup_driver_and_cleanup=True,
        create_schema=False,
    )


class ManagedCurrentCampaignTests(unittest.TestCase):
    def test_campaign_refuses_unadmitted_oracle_before_publication(self):
        from test_managed_current_query import _source_bundle

        class Compiler:
            @staticmethod
            def compile_json(_raw): return "{}"
            @staticmethod
            def compile_engine_page_json(*_args): return "{}"

        class EmptyParameters:
            @staticmethod
            def model_dump(mode="python"): return {}

        with patch("ashlar_host.managed_current_campaign.run_managed_current_trial_page") as publish:
            with self.assertRaises(ValueError):
                run_managed_current_campaign(
                    object(), config=_config(), binding=_source_bundle(),
                    protocol_profile=object(), source_oracle_input={"source_oracle": {}},
                    compiler=Compiler(), page_parameters=EmptyParameters,
                    page_result=SimpleNamespace, grouped_sql="GROUPED",
                    negative_sql="NEGATIVE", page_sql="PAGE", run_id=123,
                    task_receipt_id="456")
        publish.assert_not_called()

    def test_campaign_runs_publisher_then_four_exact_oracle_cases(self):
        from test_managed_current_query import _oracle, _source_bundle

        config = _config()
        binding = _source_bundle()
        oracle = _oracle(binding)
        publication = {
            "profile": "ashlar-managed-current-task-result/0.2",
            "publicationProfile": config.publication_profile,
            "runtimeIdentity": {"opening": config.expected_environment,
                                "closing": config.expected_environment,
                                "privateDatabricksBuildIdentity": "unknown"},
            "receipt": {"weftBindingInputs": {"manifest": {}, "manifestUuid": "id",
                "dataRoles": {}}},
        }

        class Compiler:
            @staticmethod
            def compile_json(_raw): return "{}"
            @staticmethod
            def compile_engine_page_json(*_args): return "{}"

        class EmptyParameters:
            @staticmethod
            def model_dump(mode="python"): return {}

        class TargetOwner:
            def __init__(self, *_args, **_kwargs): self.opened = False
            def open_existing(self): self.opened = True
            def closing(self): return None

        class ReadOwner:
            def __init__(self, *_args, **_kwargs): self.cases = []
            def run_backend03_case(self, _compiler, request, *, case, expected_rows, **_kwargs):
                self.cases.append((case, request["sql"], expected_rows))
                return {"case": case, "status": "passed", "rows": expected_rows}
            def backend02_target(self, *_args, **_kwargs): return object()
            def run_backend02_two_pages(self, *_args, **_kwargs):
                return {"page1": {"case": "backend02-page1", "status": "passed"},
                        "continuation": {"case": "backend02-continuation", "status": "passed"}}
            def _renew(self): return None

        with patch("ashlar_host.managed_current_campaign.observe_managed_weft_050",
                   return_value=({"version": "0.4.50", "wheelSha256": "5" * 64,
                                 "nativeModuleSha256": "7" * 64, "recordSha256": "4" * 64}, Compiler())), \
             patch("ashlar_host.managed_current_campaign.run_managed_current_trial_page",
                   return_value={"profile": "ashlar-managed-current-trial-step/0.1",
                       "scheduleSha256": config.schedule_sha256, "scheduleStep": config.schedule_step,
                       "stageId": config.stage_id, "task": publication}), \
             patch("ashlar_host.managed_current_publication.ManagedCurrentTargetOwner", TargetOwner), \
             patch("ashlar_host.managed_current_query.ManagedCurrentReadOwner", ReadOwner), \
             patch("ashlar_host.managed_current_query.admit_managed_current_source_oracle",
                   return_value=oracle), \
             patch("ashlar_host.managed_current_weft_binding.managed_current_count_star_request",
                   side_effect=lambda _bundle, _inputs, sql: {"sql": sql}), \
             patch("ashlar_host.managed_current_weft_binding.managed_current_sdk_pages_request",
                   return_value={"sql": "SELECT p.person_key,p.person_name FROM Person p"}), \
             patch("umf_access.engine_catalog.EngineRead.build", return_value=object()) as build:
            result = run_managed_current_campaign(
                object(), config=config, binding=binding, protocol_profile=object(),
                source_oracle_input={"oracle": oracle}, compiler=Compiler(),
                page_parameters=EmptyParameters,
                page_result=SimpleNamespace, grouped_sql="GROUPED", negative_sql="NEGATIVE",
                page_sql="PAGE", run_id=123, task_receipt_id="456")
        self.assertEqual(set(result["cases"]), {
            "backend03-grouped-having", "backend03-negative-having",
            "backend02-page1", "backend02-continuation"})
        self.assertEqual(result["cases"]["backend03-grouped-having"]["rows"],
                         [["person-1", "2", "1"]])
        self.assertEqual(result["cases"]["backend03-negative-having"]["rows"], [])
        build.assert_called_once()

    def test_trial_checks_runtime_before_create_then_selects_new_schema(self):
        environment = _config().expected_environment
        preflight = dict(environment, catalog="hive_metastore", schema="default")
        protocol = ReaderProtocolProfile("trial-reader/0.1", (1,), (2,), ())
        binding = {"profile": "ashlar-managed-current-core-binding/0.1",
                   "status": "preinstallation-source-bundle"}
        config = _config()
        bootstrap_expected = dict(environment, sparkVersion=None,
                                  sparkBuildText=None, runtimeTag=None)
        config = ManagedCurrentTrialConfig(**{**config.__dict__,
            "create_schema": True, "expected_environment": bootstrap_expected})
        events = []

        class Frame:
            @staticmethod
            def collect(): return []

        class Spark:
            @staticmethod
            def sql(statement): events.append(("sql", statement)); return Frame()

        fake_publication = {"profile": "ashlar-managed-current-task-result/0.2",
            "publicationProfile": config.publication_profile,
            "receipt": {"profile": "ashlar-managed-current-destination-receipt/0.1"},
            "runtimeIdentity": {"opening": environment, "closing": environment,
                                "privateDatabricksBuildIdentity": "unknown"}}
        with patch("ashlar_host.managed_current_core.restore_managed_current_admission",
                   side_effect=lambda *_args: events.append(("admit", None))), \
             patch("ashlar_host.managed_current_publication.observe_managed_current_environment",
                   side_effect=[preflight, environment, environment]), \
             patch("ashlar_host.managed_current_trial._schema_exists", side_effect=[False, True]), \
             patch("ashlar_host.managed_current_publication.run_managed_current_publication_task",
                   return_value=fake_publication) as publish:
            result = run_managed_current_trial_page(
                Spark(), config=config, binding=binding, protocol_profile=protocol)
        commands = [item[1] for item in events if item[0] == "sql"]
        self.assertLess(commands.index("CREATE SCHEMA `client_dev`.`trial_campaign`"),
                        commands.index("USE CATALOG `client_dev`"))
        self.assertEqual(result["task"], fake_publication)
        self.assertEqual(publish.call_args.kwargs["expected_environment"], environment)


if __name__ == "__main__":
    unittest.main()
