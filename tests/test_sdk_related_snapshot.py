import unittest
from dataclasses import dataclass

from ashlar.publication import ResolutionError
from ashlar.weft_query import _completed_decode
from ashlar.weft_related_keys_decode import DecodedRelatedKeys


class RelatedNativeSnapshotTests(unittest.TestCase):
    def test_original_occurrences_order_and_bytes_are_preserved(self):
        original = DecodedRelatedKeys(
            b'{"items":[["b"],["b"],["a"]],"truncated":true}',
            (("b",), ("b",), ("a",)),
            True,
        )
        copied = _completed_decode(original)
        self.assertEqual(copied, original)
        self.assertIsNot(copied, original)
        self.assertEqual(copied.items, (("b",), ("b",), ("a",)))

    def test_malformed_or_opaque_carriers_refuse(self):
        @dataclass(frozen=True)
        class Lookalike:
            original: bytes
            items: tuple
            truncated: bool

        for value in (
            Lookalike(b"{}", (), False),
            DecodedRelatedKeys(b"{}", (["x"],), False),
            DecodedRelatedKeys(b"{}", ((1,),), False),
            DecodedRelatedKeys(b"{}", (), 1),
            DecodedRelatedKeys(b"x" * (8 * 1024 * 1024 + 1), (), False),
        ):
            with (
                self.subTest(value_type=type(value)),
                self.assertRaises(ResolutionError),
            ):
                _completed_decode(value)

    def test_original_native_snapshot_consumes_shared_cell_budget(self):
        one = DecodedRelatedKeys(b"x" * (1024 * 1024), (("x",),), False)
        self.assertEqual(len(_completed_decode([one] * 8)), 8)
        with self.assertRaises(ResolutionError):
            _completed_decode([one] * 9)


class RelationshipAdmissionControls(unittest.TestCase):
    def fixture(self):
        import copy
        import json
        from pathlib import Path

        from test_weft_query import WeftQueryTests

        helper = WeftQueryTests()
        request, artifact, pins, executor, policy = helper.setup()
        original = json.loads(
            (
                Path(__file__).parent / "fixtures/sdk-engine-related-original.json"
            ).read_text()
        )
        guard = next(
            o
            for o in original["artifact"]["obligations"]
            if o["id"] == "ashlar.candidate.relationshipIntegrity"
        )
        artifact["obligations"].append(copy.deepcopy(guard))
        return helper, request, artifact, pins, executor, policy, guard

    def test_missing_owner_refuses_before_native_sql(self):
        helper, q, a, p, e, policy, _ = self.fixture()
        with self.assertRaisesRegex(ResolutionError, "relationship.*authority"):
            helper.read(q, a, p, e, policy)
        self.assertEqual(e.calls, [])
        self.assertFalse(p.active)

    def test_original_checks_run_while_held_and_owner_can_withhold(self):
        helper, q, a, p, e, policy, guard = self.fixture()
        admissions = []

        def admit(received, artifact, context):
            self.assertTrue(p.active)
            self.assertEqual(received, guard["parameters"])
            admissions.append(received)

        policy.admit_relationship_integrity = admit
        helper.read(q, a, p, e, policy)
        self.assertGreaterEqual(len(admissions), 2)
        business = next(i for i, (sql, _) in enumerate(e.calls) if sql == a["sql"])
        relationship_sql = [check["sql"] for check in guard["parameters"]["checks"]]
        for sql in relationship_sql:
            self.assertIn(sql, [query for query, _ in e.calls[:business]])
        self.assertFalse(p.active)
        helper, q, a, p, e, policy, _ = self.fixture()

        def deny(*args):
            raise PermissionError("full target input withheld")

        policy.admit_relationship_integrity = deny
        with self.assertRaises(PermissionError):
            helper.read(q, a, p, e, policy)
        self.assertEqual(e.calls, [])
        self.assertFalse(p.active)
