# SPDX-License-Identifier: MIT OR Apache-2.0
"""Effect-free admission controls for the public Weft 0.4.56 `.4` consumer route."""
import hashlib
import json
import unittest
from contextlib import nullcontext
from unittest.mock import patch

from ashlar_host.managed_current_weft_056 import (
    PROFILE, run_managed_current_weft_056_backend03,
)
from ashlar.weft_056_distribution import Weft056Distribution
from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64, bind_current_core
from ashlar_host.managed_current_weft_binding import (
    XXHASH64_BACKEND_ID, XXHASH64_BACKEND03_VERSION, XXHASH64_TARGET_PROFILE,
    managed_current_count_star_request,
)
from ashlar_host.managed_current_query import ManagedCurrentReadOwner
from test_managed_current_weft_binding import _inputs, _xxhash_config
from test_managed_current_core import _current_admission


def _response(request):
    artifact = {"backend": {
        "backendId": XXHASH64_BACKEND_ID,
        "backendVersion": XXHASH64_BACKEND03_VERSION,
        "interfaceVersion": "weft-backend/0.3.0",
        "targetProfile": XXHASH64_TARGET_PROFILE,
    }, "sql": "SELECT exact_compiler_sql"}
    raw = json.dumps({"status": "compiled", "artifact": artifact},
                     separators=(",", ":")).encode()
    return artifact, raw


class ManagedCurrentWeft056Tests(unittest.TestCase):
    def setUp(self):
        bundle = bind_current_core(_current_admission(), _xxhash_config())
        self.sql = ("SELECT p.person_key, COUNT(*) AS paths, "
                    "COUNT_DISTINCT_PATH_TARGETS(x) AS targets FROM Person p "
                    "CROSS JOIN EXPAND_PATHS(p.knows, located) AS x "
                    "GROUP BY p.person_key HAVING COUNT(*) > 1 ORDER BY p.person_key")
        self.request = managed_current_count_star_request(
            bundle, _inputs(bundle), self.sql)
        self.owner = object.__new__(ManagedCurrentReadOwner)
        self.owner.carrier_profile = CARRIER_PROFILE_XXHASH64
        self.compiler = object.__new__(Weft056Distribution)

    def test_admission_delegates_unchanged_request_and_retains_exact_response(self):
        artifact, raw = _response(self.request)
        outcome = {"request": self.request, "artifact": artifact, "rows": [["P", "2", "1"]],
                   "compilerResponseBytes": raw,
                   "compilerResponseSha256": hashlib.sha256(raw).hexdigest(),
                   "compilerSqlSha256": hashlib.sha256(artifact["sql"].encode()).hexdigest(),
                   "runtimeIdentity": {"opening": {}, "closing": {}},
                   "publicationReceipt": {"manifest": {}}}
        with patch.object(Weft056Distribution, "_verify", return_value=True), \
             patch.object(ManagedCurrentReadOwner, "run_backend03", return_value=outcome) as run:
            admitted = run_managed_current_weft_056_backend03(
                self.owner, self.compiler, self.request,
                reviewed_request_sha256=hashlib.sha256(json.dumps(
                    self.request, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
                reviewed_response_sha256=hashlib.sha256(raw).hexdigest())
        run.assert_called_once_with(self.compiler, self.request, timeout_seconds=90,
                                    retain_response=True,
                                    admit_compiled=run.call_args.kwargs["admit_compiled"])
        self.assertEqual(admitted["profile"], PROFILE)
        self.assertEqual(admitted["requestSql"], self.sql)
        self.assertEqual(admitted["compilerResponseBytes"], raw)
        self.assertEqual(admitted["compilerResponseSha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(admitted["rows"], [["P", "2", "1"]])

    def test_old_carrier_and_crossed_target_refuse_before_owner_call(self):
        compiler = object.__new__(Weft056Distribution)
        legacy = json.loads(json.dumps(self.request))
        target = legacy["target"]
        binding = json.loads(target["bindingJson"])
        binding["layoutRevision"] = "ashlar-delta/0.3"
        target["bindingJson"] = json.dumps(binding, separators=(",", ":"), sort_keys=True)
        target["bindingSha256"] = hashlib.sha256(target["bindingJson"].encode()).hexdigest()
        with patch.object(Weft056Distribution, "_verify", return_value=True), \
             patch.object(ManagedCurrentReadOwner, "run_backend03") as run:
            with self.assertRaises(ValueError):
                run_managed_current_weft_056_backend03(
                    self.owner, compiler, legacy,
                    reviewed_request_sha256="0" * 64,
                    reviewed_response_sha256="0" * 64)
        run.assert_not_called()

    def test_malformed_compiler_response_refuses_before_native_read(self):
        malformed = {"status": "compiled", "artifact": {"backend": {
            "backendId": XXHASH64_BACKEND_ID,
            "backendVersion": XXHASH64_BACKEND03_VERSION,
            "interfaceVersion": "weft-backend/0.3.0",
            "targetProfile": XXHASH64_TARGET_PROFILE,
        }, "sql": ""}}
        raw = json.dumps(malformed, separators=(",", ":"))
        self.owner.backend = object()
        self.owner.vector = object()
        self.owner.context = object()
        self.owner.receipt = {"manifest": {"schema_revisions_json": "{}"}}
        self.owner.runtime_identity = {"opening": {}, "closing": {}}
        with patch.object(Weft056Distribution, "_verify", return_value=True), \
             patch.object(Weft056Distribution, "compile_json", return_value=raw), \
             patch.object(ManagedCurrentReadOwner, "deadline", return_value=nullcontext()), \
             patch("ashlar.weft_query.read_weft") as read:
            with self.assertRaises(ValueError):
                run_managed_current_weft_056_backend03(
                    self.owner, self.compiler, self.request,
                    reviewed_request_sha256=hashlib.sha256(json.dumps(
                        self.request, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
                    reviewed_response_sha256=hashlib.sha256(raw.encode()).hexdigest())
        read.assert_not_called()

    def test_reviewed_response_digest_mismatch_prevents_native_read(self):
        artifact, raw = _response(self.request)
        self.owner.backend = object()
        self.owner.vector = object()
        self.owner.context = object()
        self.owner.receipt = {"manifest": {"schema_revisions_json": "{}"}}
        self.owner.runtime_identity = {"opening": {}, "closing": {}}
        request_digest = hashlib.sha256(json.dumps(
            self.request, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        with patch.object(Weft056Distribution, "_verify", return_value=True), \
             patch.object(Weft056Distribution, "compile_json", return_value=raw.decode()), \
             patch.object(ManagedCurrentReadOwner, "deadline", return_value=nullcontext()), \
             patch("ashlar.weft_query.read_weft") as read:
            with self.assertRaises(ValueError):
                run_managed_current_weft_056_backend03(
                    self.owner, self.compiler, self.request,
                    reviewed_request_sha256=request_digest,
                    reviewed_response_sha256="0" * 64)
        read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
