"""Actual pinned public producer -> owner allocation -> original query request."""

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from test_typed_source_policy import BINDINGS, change, request, source

from ashlar.primitive_binding import decode_primitive_column, primitive_compile_request
from ashlar.publication import Descriptor
from ashlar.schema import SchemaIntake
from ashlar.typed_source_policy import RECORD_CHECK_PIN, TypedSourcePolicy
from ashlar.weft_binding import LAYOUT_SHA256, WeftBindingError

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(
    os.environ.get("ASHLAR_UMF_SOURCE", "/private/tmp/sdk-a1-umf-c45c72-source")
)


class PrimitiveBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not SOURCE.exists():
            raise AssertionError("Actual pinned original UMF producer required")
        cls.temp = tempfile.TemporaryDirectory(prefix="ashlar-primitive-binding-")
        cls.addClassCleanup(cls.temp.cleanup)
        directory = Path(cls.temp.name)
        authored = json.loads(source())
        # This independently authored query fixture declares names explicitly.
        for element in authored["modules"][0]["elements"]:
            element["name"] = element["id"]
        cls.model = json.dumps(authored, separators=(",", ":")).encode()
        document = directory / "model.json"
        document.write_bytes(cls.model)
        query = directory / "request.json"
        query.write_text(
            json.dumps({"records": [request(change('{"21":1,"22":-0.00,"23":true}'))]})
        )
        bun = os.environ.get("ASHLAR_BUN", "bun")
        receipt = subprocess.run(
            [
                bun,
                str(ROOT / "tools/check_typed_umf_records.ts"),
                str(SOURCE),
                str(document),
                str(query),
            ],
            check=True,
            capture_output=True,
        ).stdout
        cls.evidence = (
            Path(os.environ["ASHLAR_A1_EVIDENCE_DIR"])
            if "ASHLAR_A1_EVIDENCE_DIR" in os.environ
            else None
        )
        if cls.evidence is not None:
            cls.evidence.mkdir(parents=True, exist_ok=False)
            (cls.evidence / "model.json").write_bytes(cls.model)
            (cls.evidence / "original-record-receipt.json").write_bytes(receipt)
        cls.policy = TypedSourcePolicy(
            cls.model,
            receipt,
            BINDINGS,
            source_system="typed-source",
            schema_revision="1",
            trusted_producer_revision=RECORD_CHECK_PIN,
        )
        artifact = subprocess.run(
            [
                bun,
                str(ROOT / "tools/inspect_umf.ts"),
                str(SOURCE),
                RECORD_CHECK_PIN,
                str(document),
            ],
            check=True,
            capture_output=True,
        ).stdout
        if cls.evidence is not None:
            (cls.evidence / "original-intake.json").write_bytes(artifact)
        cls.intake = SchemaIntake.read(
            artifact, "original-model-1", trusted_validator_revision=RECORD_CHECK_PIN
        )
        cls.descriptor = Descriptor(
            "primitive-fixture",
            "ashlar-delta/0.3",
            {"dev.fixture.object_current": 0},
            {"typed": "original-model-1"},
            {},
            {},
            {},
        )
        cls.options = {
            "schema_alias": "typed",
            "table_uuids": {
                "dev.fixture.object_current": "11111111-1111-1111-1111-111111111111"
            },
            "manifest_uuid": "22222222-2222-2222-2222-222222222222",
            "layout_sha256": LAYOUT_SHA256,
            "backend_version": "0.1.0-local-spark4-candidate",
            "target_profile": "spark4.0.1-delta4.0.0-candidate",
        }

    def build(self, **changes):
        return primitive_compile_request(
            'SELECT i.quantity FROM "urn:typed:".Item i',
            self.intake,
            self.policy,
            self.descriptor,
            **(self.options | changes),
        )

    def test_retains_exact_original_source_facets_allocations_and_native_vector(self):
        result = self.build()
        self.assertEqual(result["modules"][0]["documentJson"].encode(), self.model)
        native = json.loads(result["target"]["bindingJson"])
        self.assertEqual(
            hashlib.sha256(result["target"]["bindingJson"].encode()).hexdigest(),
            result["target"]["bindingSha256"],
        )
        self.assertEqual(
            native["modelPins"][0]["sha256"], hashlib.sha256(self.model).hexdigest()
        )
        self.assertEqual(
            [p["home"]["propertyId"] for p in native["records"][0]["properties"]],
            ["21", "22", "23", "24"],
        )
        self.assertEqual(native["publication"]["tables"][0]["version"], 0)
        self.assertTrue(result["options"]["allowCandidate"])

    def test_actual_original_compiler_owns_direct_primitive_descriptors(self):
        from decimal import Decimal

        import weft

        for name, token, expected in (
            ("quantity", "9007199254740993", 9007199254740993),
            ("amount", "-0.00", Decimal("-0.00")),
            ("active", "true", True),
        ):
            request_json = primitive_compile_request(
                f'SELECT i.{name} FROM "urn:typed:".Item i',
                self.intake,
                self.policy,
                self.descriptor,
                **self.options,
            )
            original = json.loads(
                weft.compile_engine_read_json(
                    json.dumps(
                        {
                            "version": "weft.engine-read/0.1",
                            "compiler": "ashlar-spark4",
                            "request": request_json,
                        }
                    )
                )
            )
            self.assertEqual(original["status"], "compiled", original)
            if self.evidence is not None:
                (self.evidence / f"{name}-request.json").write_text(
                    json.dumps(request_json, indent=2)
                )
                (self.evidence / f"{name}-original-engine-program.json").write_text(
                    json.dumps(original, indent=2)
                )

            artifact = original["artifact"]
            cell = decode_primitive_column(
                artifact["columns"][0], token, artifact, self.policy, self.intake
            )
            self.assertEqual(cell.value, expected)
            self.assertEqual(cell.original, token)
            from ashlar.publication import ResolutionError

            with self.assertRaises(ResolutionError):
                decode_primitive_column(
                    artifact["columns"][0], None, artifact, self.policy, self.intake
                )

            forged = json.loads(json.dumps(artifact))
            forged["columns"][0]["representation"]["logicalType"]["facets"] = {
                "invented": True
            }
            from ashlar.publication import ResolutionError

            with self.assertRaises(ResolutionError):
                decode_primitive_column(
                    forged["columns"][0], token, forged, self.policy, self.intake
                )

    def test_false_profile_uuid_and_version_claims_refuse(self):
        for changes in (
            {"backend_version": "qualified"},
            {"table_uuids": {}},
            {"manifest_uuid": "not-native"},
            {"schema_alias": "foreign"},
        ):
            with self.subTest(changes=changes), self.assertRaises(WeftBindingError):
                self.build(**changes)
        self.descriptor = replace(
            self.descriptor, versions={"dev.fixture.object_current": True}
        )
        with self.assertRaises(WeftBindingError):
            self.build()
