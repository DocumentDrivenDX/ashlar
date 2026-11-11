# SPDX-License-Identifier: MIT OR Apache-2.0
"""pytest plumbing for the unittest-style suite.

* puts ``src``, ``tools`` and ``tests`` on ``sys.path`` (and ``PYTHONPATH`` for
  tests that spawn subprocesses);
* applies the ``slow``, ``spark`` and ``stack`` markers by module name;
* turns a missing *optional* third-party dependency into a skip instead of an
  error, so a bare ``pip install -e .[dev]`` run is green.
"""
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
_PATHS = [str(ROOT), str(ROOT / "src"), str(ROOT / "tools"), str(ROOT / "tests")]
for _p in reversed(_PATHS):
    if _p not in sys.path:
        sys.path.insert(0, _p)
_existing = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
os.environ["PYTHONPATH"] = os.pathsep.join(_PATHS + [p for p in _existing if p not in _PATHS])

# Modules measured above ~30 s each. They run in the nightly job.
SLOW_MODULES = frozenset("""
test_ashlar_host test_commerce_graphframes_installed test_configured_local_delta4_authority
test_configured_local_graph_release test_configured_typed_graph_projection_oracle
test_domain_pack_inventory test_evolution_operator test_evolution_run test_evolution_run_lineage
test_evolution_singleton test_evolution_verification test_managed_current_publication
test_managed_current_query test_managed_current_receipt_read test_managed_current_serverless_job
test_managed_current_spark_receipt_read test_medical_effect_proof test_medical_historical_oracle
test_medical_preflight_timing test_medical_publication_observation test_medical_publication_sources
test_medical_read_sources test_medical_recovery_preflight test_module_boundaries
test_onelake_release_upload test_original_commerce_local_setup test_original_medical_admission
test_original_medical_local_publication test_original_medical_local_setup
test_original_medical_runtime
""".split())

# Modules that start a local Spark/Delta/GraphFrames session. They run in the nightly job.
SPARK_MODULES = frozenset("""
test_delta_receipt test_read_only_transport_bounds test_graphframes_export test_graphframes_local
test_graphframes_release test_graphframes_pack_scenarios test_commerce_graphframes
test_commerce_graphframes_installed test_commerce_graphframes_scenarios test_pinned_delta_graphframes
test_local_delta_custody test_local_delta_effect_plan test_local_graph_refresh
test_local_current_delta4_umf test_local_current_xxhash64_profile test_managed_current_publication
test_pack_release_graphframes test_local_weft_typed_spark4
""".split())

# Tests that need a released Truss installed (the `stack` smoke job). Each refuses with
# "Exactly one selected Truss distribution required" when none is installed.
STACK_TESTS = frozenset("""
test_managed_current_core::test_048_bundle_round_trips_exact_release_provenance_without_new_carrier_profile
test_managed_current_core::test_049_bundle_round_trips_exact_release_provenance_without_changing_physical_binding
test_managed_current_core::test_050_profile_binds_exact_public_wheel_through_core_and_weft_restore
test_managed_current_weft_binding::test_048_source_version_and_runtime_pin_survive_weft_binding
test_managed_current_weft_binding::test_049_source_tuple_survives_weft_binding_with_existing_physical_profile
test_managed_current_weft_binding::test_050_source_tuple_survives_distinct_weft_binding_profile
test_truss_current_catalog::test_fake_045_evidence_refuses_when_another_runtime_is_installed
test_truss_graph_admission::test_current_empty_interval_admits_coverage_without_entity_meaning
""".split())


# Tests whose fixtures pin macOS paths (/private/tmp, ~/.orbstack); they skip where those are absent.
MACOS_PATH_TESTS = frozenset("""
test_truss_local_publication_recovery::test_ack_exception_is_reconciled_once_without_resubmission
test_truss_local_publication_recovery::test_existing_completion_marker_refuses_before_source_custody
test_truss_local_publication_recovery::test_malformed_reconciliation_checkpoint_fails_closed
test_truss_local_publication_recovery::test_native_readback_marker_and_ack_are_strictly_ordered
test_truss_local_publication_recovery::test_readback_refusal_prevents_marker_and_source_ack
test_truss_local_publication_recovery::test_recovery_decodes_retained_archive_with_archive_digest
test_truss_local_publication_recovery::test_unmatched_ack_reconciliation_is_durable_non_success
test_truss_record_group_zero_edges::test_nonempty_records_with_zero_edges_is_admitted
test_managed_a1_source_bootstrap::test_config_uses_explicit_operator_checkout_from_installed_package_path
test_managed_a1_source_bootstrap::test_private_config_is_closed_and_pins_exact_model
test_managed_a1_source_bootstrap::test_public_group_preflight_receives_exact_typed_scalar_carriers
test_managed_a1_recovery_source_bootstrap::test_orderly_cleanup_skips_ipc_when_ack_already_closed_channel
test_original_commerce_private_fixture_selection::test_exact_original_scope_and_isolated_ordinary_ddl
""".split())


def _truss_installed():
    from importlib import metadata
    for name in ("umf-truss", "truss-toolkit"):
        try:
            metadata.distribution(name)
            return True
        except metadata.PackageNotFoundError:
            pass
    return False


# Third-party modules whose absence skips a test instead of failing it.
OPTIONAL_MODULES = frozenset("""
duckdb umf_core umf_access umf_weft weft truss pydantic py_mini_racer mini_racer pyspark py4j
delta graphframes gremlin_python neo4j jsonschema referencing aiohttp databricks psycopg
opentelemetry
""".split())


def pytest_collection_modifyitems(config, items):
    for item in items:
        module = Path(str(item.fspath)).stem
        if module in SLOW_MODULES:
            item.add_marker(pytest.mark.slow)
        if module in SPARK_MODULES:
            item.add_marker(pytest.mark.spark)
        if f"{module}::{item.name}" in STACK_TESTS:
            item.add_marker(pytest.mark.stack)
            if not _truss_installed():
                item.add_marker(pytest.mark.skip(reason="needs a released Truss installed (stack job)"))
        if f"{module}::{item.name}" in MACOS_PATH_TESTS and not Path("/private/tmp").is_dir():
            item.add_marker(pytest.mark.skip(reason="fixture pins macOS paths (/private/tmp)"))
        if module == "test_otel_supervision" and sys.version_info[:2] != (3, 11):
            item.add_marker(pytest.mark.skip(
                reason="the OpenTelemetry worker profile is pinned to Python 3.11 (diagnostics extra)"))


def _missing_optional(exc):
    """Return the missing optional top-level module name, or None."""
    while exc is not None:
        if isinstance(exc, ImportError):
            name = (getattr(exc, "name", None) or "").split(".")[0]
            if name in OPTIONAL_MODULES:
                return name
        exc = exc.__cause__ or exc.__context__
    return None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed and call.excinfo is not None:
        name = _missing_optional(call.excinfo.value)
        if name:
            report.outcome = "skipped"
            report.longrepr = (str(item.fspath), 0, f"Skipped: optional dependency {name!r} is not installed")


@pytest.hookimpl(hookwrapper=True)
def pytest_make_collect_report(collector):
    outcome = yield
    report = outcome.get_result()
    if report.failed and getattr(collector, "fspath", None) is not None:
        text = str(report.longrepr)
        for name in OPTIONAL_MODULES:
            if f"No module named '{name}" in text:
                report.outcome = "skipped"
                report.longrepr = (str(collector.fspath), 0, f"Skipped: optional dependency {name!r} is not installed")
                break
