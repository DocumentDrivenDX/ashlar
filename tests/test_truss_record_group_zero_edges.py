"""Records-only groups remain one public atomic Truss group."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ashlar_host.truss_record_group import preflight


class RecordsOnlyGroupPreflightTests(unittest.TestCase):
    def test_nonempty_records_with_zero_edges_is_admitted(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temp:
            root = Path(temp)
            bun = root / "bun"
            model = root / "model.json"
            origin = root / "origin.json"
            config = root / "request.json"
            output = root / "result.json"
            bun.write_bytes(b"selected-bun-placeholder")
            model.write_text(json.dumps({"umf": "0.10.0", "id": "model", "modules": []}))
            origin.write_text('{"operator":"test"}')
            selected = {
                "profile": "ashlar-truss-record-group/0.1",
                "connection_uri": "host=/tmp dbname=test user=test",
                "bun": str(bun), "document_path": str(model),
                "document_revision": "r1", "document_id": "model",
                "source_version": "0.10.0", "origin_path": str(origin),
                "request_key": "r61-two-record-continuation",
                "records": [
                    {"alias": "p1", "module_id": "m", "record_id": "Person",
                     "values": {"person_key": {"kind": "string", "text": "one"},
                                "person_name": {"kind": "string", "text": "One"}}},
                    {"alias": "p2", "module_id": "m", "record_id": "Person",
                     "values": {"person_key": {"kind": "string", "text": "two"},
                                "person_name": {"kind": "string", "text": "Two"}}},
                ],
                "edges": [],
            }
            config.write_text(json.dumps(selected))
            parsed, _, _, _, _, _ = preflight(config, output)
            self.assertEqual(len(parsed.records), 2)
            self.assertEqual(parsed.edges, ())


if __name__ == "__main__":
    unittest.main()
