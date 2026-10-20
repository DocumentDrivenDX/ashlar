# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original compiler custody for reduced stream pages; no native authority claim."""

import copy
import hashlib
import json
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

from pydantic import ConfigDict

from ashlar.access import AshlarTarget
from ashlar.publication import ResolutionError
from umf_access.dsl._nodes import GeneratedSource
from umf_access.engine_catalog import EngineRead
from umf_access.native_program import ProgramBuildError, canonical_json
from umf_access.pages import ReadObligationUnmet
from umf_access.related_keys import make_related_result_model
from umf_access.types import RUNTIME_IDENTITY, StrictModel


class Parameters(StrictModel):
    pass


class Result(StrictModel):
    id: str


class StreamPageSizeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import weft

        cls.compiler = weft
        cls.original = json.loads(
            (Path(__file__).parent / "fixtures/sdk-engine-related-original.json").read_text()
        )
        cls.request = copy.deepcopy(cls.original["request"])
        cls.request["interfaceVersion"] = "weft-compile/0.2.7"
        cls.request["target"]["backendVersion"] = "0.1.0-local-spark4-pages-candidate"
        cls.request["sql"] = 'SELECT r.id AS id FROM sales."order" r'

    def test_fixture_is_officially_valid_current_core(self):
        from umf import Document, validate_document

        source = self.original["request"]["modules"][0]
        self.assertEqual(source["pin"]["umfVersion"], "0.10.0")
        document = Document.model_validate(json.loads(source["documentJson"]))
        report = validate_document(document.to_dict())
        self.assertTrue(report.valid, report.diagnostics)

    def target(self, compiler=None):
        target = object.__new__(AshlarTarget)
        target.compiler = self.compiler if compiler is None else compiler
        target.compiler_method = "ashlar-spark4-pages"
        return target

    def entry(self, related=False):
        request = copy.deepcopy(self.request)
        navigation = {}
        if related:
            request["sql"] = "SELECT r.customerId AS id FROM sales.customer r"
            source = {**request, "sql": 'SELECT r.id AS id FROM sales."order" r WHERE r.status = \'open\' LIMIT 3'}
            navigation = {"related_source": source, "relationship": {"module": "sales", "id": "customers", "direction": "forward"}}
        entry = EngineRead.build(self.compiler, request, parameters=Parameters, result=Result,
            method="ashlar-spark4-pages", page_size=4, **navigation)
        return replace(entry, context_json='{"owner":"unchanged-catalog-context"}')

    def test_actual_original_plain_and_related_recompile_and_continuation(self):
        for related in (False, True):
            with self.subTest(related=related):
                entry = self.entry(related)
                original_bytes = entry.artifact_json
                smaller = self.target()._stream_page_entry(entry, 1)
                original, current = entry.artifact(), smaller.artifact()
                self.assertEqual(current["pageSize"], 1)
                self.assertEqual(current["request"], original["request"])
                self.assertEqual(current["outputs"], original["outputs"])
                self.assertEqual(smaller.context_json, entry.context_json)
                self.assertIs(smaller.parameters, entry.parameters)
                self.assertIs(smaller.result, entry.result)
                self.assertEqual(entry.artifact_json, original_bytes)
                if related:
                    expected = copy.deepcopy(original["relatedRequest"])
                    expected["pageSize"] = 1
                    self.assertEqual(current["relatedRequest"], expected)
                checked = json.loads(self.compiler.check_engine_page_json(
                    smaller.originals(), canonical_json(current["request"]), '["o-1"]'))
                self.assertEqual(checked["status"], "compiled", checked)
                self.assertEqual(checked["page"]["pageSize"], 1)
                self.assertEqual(checked["outputs"], original["outputs"])
                self.assertEqual(checked["artifact"]["bindingSha256"], original["artifact"]["bindingSha256"])

    def test_actual_original_related_key_transport_remains_exact(self):
        request = copy.deepcopy(self.request)
        request["sql"] = self.original["request"]["sql"]
        original = EngineRead.compile_original(self.compiler, request,
            method="ashlar-spark4-pages", page_size=4)
        program = json.loads(original)
        decoder = program["outputs"][1]["decoder"]
        pin = program["artifact"]["modelPins"][0]
        source = GeneratedSource(pin["documentId"], pin["umfVersion"], pin["sha256"],
            pin["sha256"], "umf-access-runtime/0.1")

        class Key(StrictModel):
            UMF_KEY: ClassVar[bool] = True
            UMF_SOURCE: ClassVar[GeneratedSource] = source
            KEY_IDENTITY: ClassVar[dict] = {"documentId": source.document_id,
                "coreVersion": source.core_version, "module": "sales", "element": "customer", "key": "pk"}
            UMF_FIELDS: ClassVar[tuple] = (("sales", "customerId", "customerId"),)
            customerId: str

        class Row(StrictModel):
            model_config = ConfigDict(strict=True)
            UMF_SOURCE: ClassVar[GeneratedSource] = source
            UMF_BUNDLE_INPUT_DIGEST: ClassVar[str] = source.input_digest
            UMF_RUNTIME_ABI: ClassVar[tuple] = RUNTIME_IDENTITY
            id: str

        result = make_related_result_model("OriginalRelatedRow", Row, Key, "related", decoder, Row)
        entry = EngineRead.from_original(original, parameters=Parameters, result=result)
        smaller = self.target()._stream_page_entry(entry, 2)
        self.assertIs(smaller.result, result)
        self.assertEqual(smaller.artifact()["outputs"], entry.artifact()["outputs"])
        self.assertEqual(smaller.artifact()["outputs"][1]["decoder"], decoder)

    def test_bound_and_missing_original_compiler_refuse(self):
        entry = self.entry()
        self.assertIs(self.target()._stream_page_entry(entry, 4), entry)
        for size in (True, 0, -1, 5, 1000, 1.0):
            with self.subTest(size=size), self.assertRaises(ValueError):
                self.target()._stream_page_entry(entry, size)
        with self.assertRaises(ReadObligationUnmet):
            self.target(SimpleNamespace())._stream_page_entry(entry, 1)
        target = self.target()
        target.compiler_method = "ashlar-dbsql-20261016-pages"
        with self.assertRaises(ResolutionError):
            target._stream_page_entry(entry, 1)

    def test_original_refusal_and_context_drift_are_not_repaired(self):
        entry = self.entry()

        class Refusing:
            def compile_engine_page_json(self, request):
                packet = json.loads(request)
                packet["request"]["target"]["bindingSha256"] = "0" * 64
                return StreamPageSizeTests.compiler.compile_engine_page_json(canonical_json(packet))

        with self.assertRaises(ProgramBuildError):
            self.target(Refusing())._stream_page_entry(entry, 1)
        self.assertEqual(hashlib.sha256(entry.artifact_json.encode()).hexdigest(),
            hashlib.sha256(self.entry().artifact_json.encode()).hexdigest())
