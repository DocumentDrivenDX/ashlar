# SPDX-License-Identifier: MIT OR Apache-2.0
import copy
import hashlib
from importlib.metadata import version as package_version
import json
import unittest
from contextlib import nullcontext
from datetime import timedelta

from umf_access.engine_catalog import EngineRead
from umf_access.types import StrictModel

from ashlar.access import AshlarTarget
from ashlar_host.managed_current_core import bind_current_core
from ashlar_host.managed_current_weft_binding import managed_current_sdk_pages_request
from test_managed_current_core import _current_admission
from test_managed_current_weft_binding import (
    _inputs, _managed_config, _xxhash_config,
)


class Parameters(StrictModel):
    pass


class Person(StrictModel):
    person_key: str
    person_name: str


class ArtifactView(EngineRead):
    """Expose a narrowly mutated view of one real compiler artifact."""

    def __init__(self, source, mutate):
        object.__setattr__(self, "_source", source)
        object.__setattr__(self, "_mutate", mutate)

    def artifact(self):
        value = copy.deepcopy(self._source.artifact())
        self._mutate(value)
        return value


class ManagedCurrentPageTargetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import weft

        cls.weft = weft
        cls.weft_version = package_version("umf-weft")
        bundle = bind_current_core(_current_admission(), _xxhash_config())
        request = managed_current_sdk_pages_request(
            bundle, _inputs(bundle),
            "SELECT p.person_key,p.person_name FROM Person p", {})
        packet = {
            "version": "weft.engine-page/0.1",
            "compiler": "ashlar-spark4-pages",
            "request": request,
            "pageSize": 2,
        }
        original = weft.compile_engine_page_json(json.dumps(
            packet, ensure_ascii=False, separators=(",", ":")))
        parsed = json.loads(original)
        if parsed.get("status") != "compiled":
            if any(item.get("code") == "WFT-ENGINE-PAGE"
                   for item in parsed.get("diagnostics", [])):
                if cls.weft_version not in {"0.4.50", "0.4.52"}:
                    raise unittest.SkipTest(
                        "installed Weft compiler does not emit the managed .4 page target")
                raise AssertionError(
                    f"Weft {cls.weft_version} failed to emit the managed .4 page target: "
                    f"{parsed}")
            raise AssertionError(f"Weft page compilation failed: {parsed}")
        cls.entry = EngineRead.from_original(
            original, parameters=Parameters, result=Person)
        cls.request = request

    def make_target(self, entry=None, *, profiles=None, binding_sha256=None):
        return AshlarTarget(
            compiler=self.weft,
            compiler_method="ashlar-spark4-pages",
            executor=object(), backend=object(), pins=object(), vector=object(),
            policy=object(), context=object(),
            supported_profiles=profiles or ["ashlar-delta/0.4-xxhash64"],
            supported_revisions={},
            binding_sha256=binding_sha256 or self.request["target"]["bindingSha256"],
            platform=(), default_timeout=timedelta(seconds=10),
            deadline=lambda _timeout: nullcontext(),
            catalog_entries=(entry or self.entry,),
        )

    def test_actual_compiler_page_artifact_accepts_exact_xxhash64_target(self):
        program = self.entry.artifact()
        self.assertEqual(program["compiler"], "ashlar-spark4-pages")
        self.assertEqual(program["request"]["target"]["backendId"],
                         "ashlar.databricks.managed-uc-spark-xxhash64")
        self.assertEqual(program["artifact"]["backend"], {
            "backendId": "ashlar.databricks.managed-uc-spark-xxhash64",
            "backendVersion": "0.2.0-managed-current-core-xxhash64-pages-candidate",
            "interfaceVersion": "weft-backend/0.2.7",
            "targetProfile": "ashlar-managed-current-core/0.2-xxhash64",
        })
        target = self.make_target()
        self.assertEqual(json.loads(target.native_target_json)["backendId"],
                         "ashlar.databricks.managed-uc-spark-xxhash64")

    def test_actual_compiler_page_artifact_keeps_legacy_managed_target(self):
        bundle = bind_current_core(_current_admission(), _managed_config())
        request = managed_current_sdk_pages_request(
            bundle, _inputs(bundle),
            "SELECT p.person_key,p.person_name FROM Person p", {})
        packet = {
            "version": "weft.engine-page/0.1",
            "compiler": "ashlar-spark4-pages",
            "request": request,
            "pageSize": 2,
        }
        original = self.weft.compile_engine_page_json(json.dumps(
            packet, ensure_ascii=False, separators=(",", ":")))
        if json.loads(original).get("status") != "compiled":
            self.fail("selected Weft compiler must retain the existing managed page target")
        entry = EngineRead.from_original(
            original, parameters=Parameters, result=Person)
        target = AshlarTarget(
            compiler=self.weft, compiler_method="ashlar-spark4-pages",
            executor=object(), backend=object(), pins=object(), vector=object(),
            policy=object(), context=object(), supported_profiles=["ashlar-delta/0.3"],
            supported_revisions={}, binding_sha256=request["target"]["bindingSha256"],
            platform=(), default_timeout=timedelta(seconds=10),
            deadline=lambda _timeout: nullcontext(), catalog_entries=(entry,))
        self.assertEqual(json.loads(target.native_target_json)["backendId"],
                         "ashlar.databricks.managed-uc-spark")

    def test_wrong_managed_target_identity_fields_refuse(self):
        for field, value in (
            ("backendId", "ashlar.databricks.managed-uc-spark"),
            ("backendVersion", "0.2.0-managed-current-core-xxhash64-candidate"),
            ("targetProfile", "ashlar-managed-current-core/0.1"),
        ):
            def mutate(program, field=field, value=value):
                program["request"]["target"][field] = value

            with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "Complete original Ashlar page binding required"):
                self.make_target(ArtifactView(self.entry, mutate))

    def test_carrier_and_hash_binding_must_match_target_profile(self):
        self.assertRaisesRegex(
            ValueError, "Managed page target and physical carrier profile differ",
            self.make_target, profiles=["ashlar-delta/0.3"])

        def mutate_binding(program):
            target = program["request"]["target"]
            binding = json.loads(target["bindingJson"])
            binding["layoutRevision"] = "ashlar-delta/0.3"
            target["bindingJson"] = json.dumps(
                binding, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            target["bindingSha256"] = hashlib.sha256(
                target["bindingJson"].encode()).hexdigest()

        changed = ArtifactView(self.entry, mutate_binding)
        changed_digest = changed.artifact()["request"]["target"]["bindingSha256"]
        with self.assertRaisesRegex(
                ValueError, "Managed page target and physical carrier profile differ"):
            self.make_target(changed, binding_sha256=changed_digest)


if __name__ == "__main__":
    unittest.main()
