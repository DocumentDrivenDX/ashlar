# SPDX-License-Identifier: MIT OR Apache-2.0
import base64
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from ashlar_host.truss_group_request_freeze import (
    PROFILE, TrussGroupRequestFreezeError, freeze_public_truss_group_request,
)


class State(Enum):
    ACTIVE = "active"


@dataclass(frozen=True)
class Carrier:
    name: str
    payload: bytes
    state: State


class TestTrussGroupRequestFreeze(unittest.TestCase):
    def test_fsync_record_binds_invocation_and_public_group_carriers(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve() / "attempt"
            directory.mkdir(mode=0o700)
            os.chmod(directory, 0o700)
            path = directory / "a1-request.json"
            invocation = b'{"profile":"selected-original","values":{"quantity":null}}'
            receipt = freeze_public_truss_group_request(path,
                original_invocation_bytes=invocation, schedule_sha256="a" * 64,
                source="A", slot="A1",
                semantic=Carrier("semantic", b"semantic-bytes", State.ACTIVE),
                request=Carrier("request", b"request-bytes", State.ACTIVE),
                documents=(Carrier("model", b"accepted-model", State.ACTIVE),))
            value = json.loads(path.read_bytes())
            binding = value.pop("binding_sha256")
            self.assertEqual(hashlib.sha256(json.dumps(value, ensure_ascii=False,
                sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest(), binding)
            self.assertEqual(value["profile"], PROFILE)
            self.assertEqual(base64.b64decode(value["invocation_bytes_base64"]), invocation)
            self.assertEqual(value["request_sha256"], receipt["request_sha256"])
            request_bytes = base64.b64decode(value["request_bytes_base64"])
            self.assertEqual(value["request_sha256"],
                             hashlib.sha256(request_bytes).hexdigest())
            self.assertEqual(json.loads(request_bytes)["name"], "request")

    def test_refuses_other_source_and_existing_attempt_without_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve() / "attempt"
            directory.mkdir(mode=0o700)
            os.chmod(directory, 0o700)
            path = directory / "request.json"
            kwargs = dict(original_invocation_bytes=b"{}", schedule_sha256="b" * 64,
                source="A", slot="B1", semantic={}, request={}, documents=())
            with self.assertRaises(TrussGroupRequestFreezeError):
                freeze_public_truss_group_request(path, **kwargs)
            self.assertFalse(path.exists())
            path.write_bytes(b"preserve")
            kwargs["slot"] = "A1"
            with self.assertRaises(TrussGroupRequestFreezeError):
                freeze_public_truss_group_request(path, **kwargs)
            self.assertEqual(path.read_bytes(), b"preserve")


if __name__ == "__main__":
    unittest.main()
