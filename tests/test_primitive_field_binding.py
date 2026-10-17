"""Actual public Field checks preserve incomplete Record evidence, no ingestion port."""

import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from test_typed_source_policy import BINDINGS, change, request, source

from ashlar.primitive_field_binding import PrimitiveFieldBinding
from ashlar.typed_source_policy import RECORD_CHECK_PIN, TypedSourceError

ROOT = Path(__file__).resolve().parents[1]


class PrimitiveFieldBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        directory = Path(cls.temp.name)
        model = json.loads(source())
        model["vocabularies"] = {"umf.access": {"version": "0.1.0"}}
        model["modules"][0]["extensions"] = {"umf.access": {"views": []}}
        model["modules"][0]["elements"][-1]["nullability"] = "required"
        cls.model = json.dumps(model, separators=(",", ":")).encode()
        document, query = directory / "model.json", directory / "request.json"
        document.write_bytes(cls.model)
        query.write_text(
            json.dumps(
                {"records": [request(change('{"21":1,"22":-0.00,"23":true,"24":"x"}'))]}
            )
        )
        observed = subprocess.run(
            [
                os.environ.get("ASHLAR_BUN", "bun"),
                str(ROOT / "tools/check_primitive_fields.ts"),
                os.environ.get(
                    "ASHLAR_UMF_SOURCE", "/private/tmp/sdk-a1-umf-c45c72-source"
                ),
                str(document),
                str(query),
            ],
            capture_output=True,
            check=False,
        )
        if observed.returncode:
            raise AssertionError(observed.stderr.decode())
        cls.receipt = observed.stdout

    def binding(self, receipt=None, source_bytes=None, bindings=None):
        return PrimitiveFieldBinding(
            self.model if source_bytes is None else source_bytes,
            self.receipt if receipt is None else receipt,
            BINDINGS if bindings is None else bindings,
            source_system="typed-source",
            schema_revision="1",
            trusted_producer_revision=RECORD_CHECK_PIN,
        )

    def test_original_incomplete_record_retained_no_ingestion_callable(self):
        binding = self.binding()
        report = json.loads(binding.receipt)
        self.assertFalse(report["records"][0]["result"]["validation"]["complete"])
        self.assertEqual(
            report["records"][0]["result"]["validation"]["diagnostics"][0]["code"],
            "UNKNOWN_EXTENSION",
        )
        self.assertTrue(
            all(f["validation"]["complete"] for f in report["records"][0]["fields"])
        )
        self.assertEqual(binding.source, self.model)
        self.assertFalse(callable(binding))
        self.assertFalse(hasattr(binding, "request"))

    def test_faulted_original_custody_and_fields_refuse(self):
        for fault in [
            "sha",
            "field-invalid",
            "field-incomplete",
            "field-identity",
            "record-meaning",
            "missing",
            "duplicate",
        ]:
            report = json.loads(self.receipt)
            if fault == "sha":
                report["sourceSha256"] = "0" * 64
            elif fault == "field-invalid":
                report["records"][0]["fields"][0]["validation"]["valid"] = False
            elif fault == "field-incomplete":
                report["records"][0]["fields"][0]["validation"]["complete"] = False
            elif fault == "field-identity":
                report["records"][0]["fields"][0]["field"]["element"] = "other"
            elif fault == "record-meaning":
                report["records"][0]["result"]["validation"]["diagnostics"][0][
                    "code"
                ] = "UNKNOWN_CONSTRAINT"
            elif fault == "missing":
                report["records"][0]["fields"].pop()
            else:
                report["records"].append(copy.deepcopy(report["records"][0]))
            with self.subTest(fault=fault), self.assertRaises(TypedSourceError):
                self.binding(json.dumps(report).encode())
        bindings = copy.deepcopy(BINDINGS)
        bindings[17]["fields"]["021"] = bindings[17]["fields"].pop("21")
        with self.assertRaises(TypedSourceError):
            self.binding(bindings=bindings)

    def test_original_field_request_profile_refuses_broader_semantics(self):
        from ashlar.primitive_binding import primitive_compile_request
        from ashlar.publication import Descriptor
        from ashlar.schema import SchemaIntake
        from ashlar.weft_binding import LAYOUT_SHA256, WeftBindingError

        directory = Path(self.temp.name)
        artifact = subprocess.run(
            [
                os.environ.get("ASHLAR_BUN", "bun"),
                str(ROOT / "tools/inspect_umf.ts"),
                os.environ.get(
                    "ASHLAR_UMF_SOURCE", "/private/tmp/sdk-a1-umf-c45c72-source"
                ),
                RECORD_CHECK_PIN,
                str(directory / "model.json"),
            ],
            capture_output=True,
            check=True,
        ).stdout
        intake = SchemaIntake.read(
            artifact, "1", trusted_validator_revision=RECORD_CHECK_PIN
        )
        descriptor = Descriptor(
            "compiler-only",
            "ashlar-delta/0.3",
            {"spark_catalog.owned.object_current": 0},
            {"owned": "1"},
            {},
            {},
            {},
        )

        def build(sql):
            return primitive_compile_request(
                sql,
                intake,
                self.binding(),
                descriptor,
                schema_alias="owned",
                table_uuids={
                    "spark_catalog.owned.object_current": "11111111-1111-1111-1111-111111111111"
                },
                manifest_uuid="22222222-2222-2222-2222-222222222222",
                layout_sha256=LAYOUT_SHA256,
                backend_version="0.1.0-local-spark4-fields-candidate",
                target_profile="spark4.0.1-delta4.0.0-fields-candidate",
            )

        result = build(
            "SELECT r.quantity AS value FROM m.Item r ORDER BY r.note LIMIT 10"
        )
        self.assertEqual(result["modules"][0]["documentJson"].encode(), self.model)
        self.assertEqual(result["parameters"], {})
        self.assertEqual(result["interfaceVersion"], "weft-compile/0.2.0")
        for sql in [
            "SELECT r.quantity FROM m.Item r WHERE r.quantity=1 ORDER BY r.note LIMIT 10",
            "SELECT COUNT(*) AS value FROM m.Item r ORDER BY r.note LIMIT 10",
            "SELECT RELATED_KEYS(r.edge,2) AS value FROM m.Item r ORDER BY r.note LIMIT 10",
            "SELECT r.quantity FROM m.Item r ORDER BY r.note",
        ]:
            with self.subTest(sql=sql), self.assertRaises(WeftBindingError):
                build(sql)

    def test_unknown_known_vocabulary_content_still_refuses(self):
        directory = Path(self.temp.name)
        model = json.loads(self.model)
        model["modules"][0]["extensions"]["umf.access"]["unknownMeaning"] = True
        source_bytes = json.dumps(model, separators=(",", ":")).encode()
        document = directory / "unknown-access-model.json"
        document.write_bytes(source_bytes)
        produced = subprocess.run(
            [
                os.environ.get("ASHLAR_BUN", "bun"),
                str(ROOT / "tools/check_primitive_fields.ts"),
                os.environ.get(
                    "ASHLAR_UMF_SOURCE", "/private/tmp/sdk-a1-umf-c45c72-source"
                ),
                str(document),
                str(directory / "request.json"),
            ],
            capture_output=True,
            check=True,
        )
        # Original public Field checks are complete. A known vocabulary name
        # does not authorize unknown payload semantics at the native binding.
        self.assertTrue(
            all(
                f["validation"]["complete"]
                for f in json.loads(produced.stdout)["records"][0]["fields"]
            )
        )
        with self.assertRaises(TypedSourceError):
            self.binding(produced.stdout, source_bytes=source_bytes)
