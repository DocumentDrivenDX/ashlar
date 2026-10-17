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
