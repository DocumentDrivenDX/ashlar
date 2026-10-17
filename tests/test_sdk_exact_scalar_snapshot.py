"""Original non-temporal decoder custody, independent of native qualification."""

import unittest
from decimal import Decimal
from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict

from ashlar.access import _Policy
from ashlar.publication import ResolutionError
from ashlar.weft_decode import ExactScalar, decode_exact_scalar
from ashlar.weft_query import _completed_decode


class ExactScalarSnapshotTests(unittest.TestCase):
    def test_actual_original_decoder_tokens_survive_owned_snapshot(self):
        for logical, carrier, decoder, token in (
            (
                {
                    "family": "integer",
                    "facets": {"integerWidth": {"bits": 64, "signed": True}},
                    "nullable": False,
                },
                "text",
                "exact-integer",
                "9007199254740993",
            ),
            (
                {
                    "family": "decimal",
                    "facets": {"precision": 18, "scale": 2},
                    "nullable": False,
                },
                "text",
                "exact-decimal",
                "-0.00",
            ),
            (
                {"family": "boolean", "facets": {}, "nullable": False},
                "boolean",
                "boolean",
                False,
            ),
        ):
            representation = {
                "kind": "scalar",
                "logicalType": logical,
                "carrier": carrier,
                "decoder": decoder,
            }
            original = decode_exact_scalar(representation, token)
            policy = object.__new__(_Policy)
            policy._time = lambda: None
            policy.original = SimpleNamespace(
                decode_result=lambda *args, decoded=original: decoded
            )
            observed = policy.decode_result({"representation": representation})
            self.assertIs(observed, original)
            detached = _completed_decode(((observed,),))[0][0]
            self.assertIs(type(detached), ExactScalar)
            self.assertIsNot(detached, original)
            self.assertEqual(detached.original, token)
            self.assertEqual(detached.value, original.value)
            if isinstance(detached.value, Decimal):
                self.assertEqual(detached.value.as_tuple(), original.value.as_tuple())

    def test_typed_transport_unwraps_only_after_original_custody_is_retained(self):
        class Result(BaseModel):
            model_config = ConfigDict(strict=True)
            number: int

        rows = ((ExactScalar(9007199254740993, "9007199254740993"),),)
        observed = []
        policy = object.__new__(_Policy)
        policy._time = lambda: None
        policy.model = Result
        policy.page_metadata = None
        policy.total_outputs = None
        policy.original = SimpleNamespace(
            authorize_result=lambda value, *args: observed.append(value)
        )
        policy.authorize_result(rows, None, None)
        self.assertIs(observed[0], rows)
        self.assertIs(type(observed[0][0][0]), ExactScalar)
        self.assertEqual(policy.validated[0].number, 9007199254740993)

    def test_opaque_and_nonfinite_carriers_refuse(self):
        for value in (
            SimpleNamespace(value=1, original="1"),
            ExactScalar(Decimal("NaN"), "NaN"),
            ExactScalar([], "1"),
        ):
            with self.subTest(value=value), self.assertRaises(ResolutionError):
                _completed_decode(value)

    def test_forged_token_correspondence_refuses_before_buffer(self):
        representation = {
            "kind": "scalar",
            "logicalType": {
                "family": "integer",
                "facets": {"integerWidth": {"bits": 64, "signed": True}},
                "nullable": False,
            },
            "carrier": "text",
            "decoder": "exact-integer",
        }
        policy = object.__new__(_Policy)
        policy._time = lambda: None
        policy.original = SimpleNamespace(
            decode_result=lambda *args: ExactScalar(2, "1")
        )
        with self.assertRaises(ResolutionError):
            policy.decode_result({"representation": representation})
