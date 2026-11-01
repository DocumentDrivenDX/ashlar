# SPDX-License-Identifier: MIT OR Apache-2.0
"""One bounded publication plus the four selected managed-current Weft cases.

This is an injected-task orchestrator. It creates no compute, acquires no
credentials, reads no Truss pages, and performs no source ACK. The external
holder retains the result, reopens its destination receipt, and owns ACK.
"""
from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import platform
import time
from pathlib import Path

from .managed_current_trial import ManagedCurrentTrialConfig, run_managed_current_trial_page
from .managed_current_core import CARRIER_PROFILE_XXHASH64

PROFILE = "ashlar-managed-current-functional-campaign/0.2"
WEFT_SOURCE_COMMIT = "2f62e1148d1b2030558550e4f6f2ffe518bac62d"
WEFT_VERSION = "0.4.56"
WEFT_LINUX_WHEELS = {
    "x86_64": ("https://github.com/DocumentDrivenDX/weft/releases/download/v0.4.56/"
               "umf_weft-0.4.56-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
               "32f4c38d9a614dd4c3a367f8b24d5cd4ba41ec80c2a4a2e3423e2082a294dbdd"),
    "aarch64": ("https://github.com/DocumentDrivenDX/weft/releases/download/v0.4.56/"
                "umf_weft-0.4.56-cp39-abi3-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
                "2de2156e283c34e45154f5d9fbc83a3eb1dd43fad497d787509d66cc96cfbe5a"),
}
_CASES = ("backend03-grouped-having", "backend03-negative-having",
          "backend02-page1", "backend02-continuation")


def _require(value, reason="Managed current functional campaign refused"):
    if not value:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _hash_file(path, maximum):
    candidate = Path(path)
    _require(candidate.is_absolute() and not candidate.is_symlink())
    raw = candidate.read_bytes()
    _require(0 < len(raw) <= maximum)
    return _sha(raw)


def observe_managed_weft_056():
    """Verify installed compiler metadata against the selected Linux wheel pin."""
    try:
        _require(platform.system() == "Linux" and platform.machine() in WEFT_LINUX_WHEELS,
                 "Selected Weft 0.4.56 campaign supports Linux x86_64/aarch64 only")
        expected_url, expected_hash = WEFT_LINUX_WHEELS[platform.machine()]
        distribution = importlib.metadata.distribution("umf-weft")
        module = importlib.import_module("weft")
        direct_url = distribution.read_text("direct_url.json")
        direct = json.loads(direct_url) if type(direct_url) is str else None
        archive = direct.get("archive_info", {}) if type(direct) is dict else {}
        wheel_hash = archive.get("hashes", {}).get("sha256")
        if wheel_hash is None and type(archive.get("hash")) is str:
            algorithm, separator, digest = archive["hash"].partition("=")
            wheel_hash = digest if algorithm == "sha256" and separator else None
        wheel_url = direct.get("url") if type(direct) is dict else None
        record_name = next(name for name in (str(item) for item in distribution.files or ())
                           if name.endswith(".dist-info/RECORD"))
        native_path = Path(module.__file__).resolve().parent / "weft.abi3.so"
        record_path = Path(distribution.locate_file(record_name)).resolve()
        observed = {
            "version": distribution.version,
            "wheelSha256": wheel_hash,
            "platformMachine": platform.machine(),
            "wheelUrl": wheel_url,
            "nativeModuleSha256": _hash_file(native_path, 64 * 1024 * 1024),
            "recordSha256": _hash_file(record_path, 2 * 1024 * 1024),
        }
        _require(distribution.version == WEFT_VERSION
                 and wheel_url == expected_url
                 and wheel_hash == expected_hash
                 and all(callable(getattr(module, name, None)) for name in (
                     "compile_json", "compile_engine_page_json",
                     "compile_paths_keys_json",
                     "check_engine_page_json", "check_engine_read_json")),
                 "Installed Weft 0.4.56 Linux release URL, wheel hash, or API differs")
        return observed, module
    except Exception as error:
        if isinstance(error, ValueError):
            raise
        raise ValueError("Selected installed Weft 0.4.56 Linux wheel could not be observed") from None


def run_managed_current_campaign(spark, *, config, binding, protocol_profile,
                                 source_oracle_input, compiler, page_parameters,
                                 page_result, grouped_sql, negative_sql, page_sql,
                                 run_id, task_receipt_id):
    """Publish one already delivered positive page, then execute all four cases.

    The source oracle is admitted before publication can create the new schema.
    The resulting case artifacts contain exact compiler JSON and result bags;
    continuation bytes remain in the page owner and are only hashed.
    """
    from .managed_current_query import (
        ManagedCurrentReadOwner, admit_managed_current_source_oracle,
        managed_current_grouped_paths, managed_current_person_page,
    )
    from .managed_current_publication import ManagedCurrentTargetOwner
    from .managed_current_weft_binding import (
        managed_current_count_star_request, managed_current_sdk_pages_request,
    )

    _require(type(config) is ManagedCurrentTrialConfig)
    config.validate()
    _require(config.create_schema is False and config.schedule_step == 2
             and config.carrier_profile == CARRIER_PROFILE_XXHASH64,
             "Functional Weft campaign requires the frozen positive graph page after catalog publication")
    started = time.monotonic()
    _require(type(binding) is dict and type(source_oracle_input) is dict
             and type(run_id) in (int, str)
             and (type(run_id) is int and run_id > 0
                  or type(run_id) is str and run_id.isdecimal())
             and type(task_receipt_id) is str and task_receipt_id.isdecimal())
    _require(all(type(sql) is str and 0 < len(sql.encode("utf-8")) <= 65536
                 and "\0" not in sql for sql in (grouped_sql, negative_sql, page_sql)))
    _require(type(page_parameters) is type and type(page_result) is type
             and callable(getattr(compiler, "compile_json", None))
             and callable(getattr(compiler, "compile_engine_page_json", None)))

    # Bind all expected rows to the actual retained Truss page and frozen
    # schedule before the publication helper can create or mutate UC objects.
    oracle = admit_managed_current_source_oracle(
        source_oracle_input, source_bundle=binding,
        schedule_sha256=config.schedule_sha256, schedule_step=config.schedule_step)
    selected_weft, compiler = observe_managed_weft_056()
    _require(config.maximum_task_seconds - int(time.monotonic() - started) > 0,
             "Campaign task deadline exhausted before publication")
    publication_result = run_managed_current_trial_page(
        spark, config=config, binding=binding, protocol_profile=protocol_profile)
    task_result = publication_result.get("task")
    _require(publication_result.get("profile") == "ashlar-managed-current-trial-step/0.1"
             and publication_result.get("scheduleSha256") == config.schedule_sha256
             and publication_result.get("scheduleStep") == config.schedule_step
             and publication_result.get("stageId") == config.stage_id
             and type(task_result) is dict
             and task_result.get("profile") == "ashlar-managed-current-task-result/0.2"
             and task_result.get("publicationProfile") == config.publication_profile)
    receipt = task_result.get("receipt")
    runtime_identity = task_result.get("runtimeIdentity")
    _require(type(receipt) is dict and type(runtime_identity) is dict
             and runtime_identity.get("opening") == runtime_identity.get("closing")
             and (runtime_identity["opening"].get("sparkBuildText")
                  or runtime_identity["opening"].get("runtimeTag")))
    observed_environment = runtime_identity["opening"]
    _require(all(observed_environment.get(key) == config.expected_environment.get(key)
                 for key in ("runtimeName", "principal", "protocol"))
             and all(config.expected_environment.get(key) is None
                     or observed_environment.get(key) == config.expected_environment[key]
                     for key in ("sparkVersion", "sparkBuildText", "runtimeTag"))
             and observed_environment.get("catalog") == config.catalog
             and observed_environment.get("schema") == config.schema,
             "Observed task runtime must match every supplied runtime pin and selected destination")

    target_owner = ManagedCurrentTargetOwner(
        spark, catalog=config.catalog, schema=config.schema,
        protocol_profile=protocol_profile, carrier_profile=config.carrier_profile)
    target_owner.open_existing()
    positive_rows = managed_current_grouped_paths(oracle, threshold=0)
    negative_rows = managed_current_grouped_paths(oracle, threshold=2)
    first_page = managed_current_person_page(oracle, page_size=2)
    _require(len(positive_rows) == 1 and positive_rows[0][1:] == ["2", "1"]
             and negative_rows == [] and len(first_page["rows"]) == 2
             and first_page["hasMore"] is True,
             "Frozen positive-page oracle must prove two parallel paths, one terminal, no >2 result, and 2+1 Persons")
    second_page = managed_current_person_page(oracle, after=first_page["nextAfter"], page_size=2)
    _require(second_page["hasMore"] is False and len(second_page["rows"]) == 1)
    weft_inputs = receipt.get("weftBindingInputs")
    _require(type(weft_inputs) is dict)
    grouped_request = managed_current_count_star_request(binding, weft_inputs, grouped_sql)
    negative_request = managed_current_count_star_request(binding, weft_inputs, negative_sql)
    page_request = managed_current_sdk_pages_request(binding, weft_inputs, page_sql, {})

    owner = ManagedCurrentReadOwner(
        spark, target_owner=target_owner, receipt=receipt,
        runtime_identity=runtime_identity, expected_environment=observed_environment,
        source_bundle=binding, oracle=oracle, expected_rows=[],
        schedule_sha256=config.schedule_sha256, schedule_step=config.schedule_step)
    common = {"source_commit": WEFT_SOURCE_COMMIT,
              "linux_wheel_sha256": selected_weft["wheelSha256"],
              "linux_native_module_sha256": selected_weft["nativeModuleSha256"],
              "linux_record_sha256": selected_weft["recordSha256"],
              "oracle": oracle, "run_id": run_id,
              "task_receipt_id": task_receipt_id}
    def remaining():
        value = config.maximum_task_seconds - int(time.monotonic() - started)
        _require(value > 0, "Campaign task deadline exhausted")
        return min(90, value)

    cases = {}
    cases["backend03-grouped-having"] = owner.run_backend03_case(
        compiler, grouped_request, case="backend03-grouped-having",
        expected_rows=positive_rows, timeout_seconds=remaining(), **common)
    cases["backend03-negative-having"] = owner.run_backend03_case(
        compiler, negative_request, case="backend03-negative-having",
        expected_rows=negative_rows, timeout_seconds=remaining(), **common)

    from umf_access.engine_catalog import EngineRead
    page_entry = EngineRead.build(compiler, page_request, parameters=page_parameters,
        result=page_result, method="ashlar-spark4-pages", page_size=2)
    parameters = page_parameters()
    _require(parameters.model_dump(mode="python") == {})
    page_target = owner.backend02_target(compiler, page_entry, timeout_seconds=remaining())
    page_common = {key: value for key, value in common.items() if key != "oracle"}
    sdk_cases = owner.run_backend02_two_pages(
        page_target, page_entry, parameters, oracle=oracle,
        expected_first=first_page["rows"], expected_second=second_page["rows"],
        timeout_seconds=remaining(), **page_common)
    cases["backend02-page1"] = sdk_cases["page1"]
    cases["backend02-continuation"] = sdk_cases["continuation"]
    # Each read path renews the manifest pin, all six installation roles,
    # owners, runtime and protocol before and after releasing its result.
    target_owner.closing()
    _require(set(cases) == set(_CASES))

    answer = {"profile": PROFILE, "carrierProfile": config.carrier_profile,
              "lookupHashProfile": target_owner.carriers.lookup_hash_profile,
              "publication": publication_result,
              "weftRuntime": selected_weft, "cases": cases}
    encoded = _canonical(answer).encode("utf-8")
    _require(len(encoded) <= 4 * 1024 * 1024,
             "Four-case campaign receipt exceeds the bounded notebook result")
    return answer
