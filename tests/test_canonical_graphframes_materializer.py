# SPDX-License-Identifier: MIT OR Apache-2.0
import os
from pathlib import Path
import tempfile
import unittest

from ashlar_host.canonical_graphframes_materializer import (
    EXPECTED_PROTOCOL, _acquire_lock, _frozen_source, _private_root, _protocol,
    _spark_type_names, _verify_detail_layout,
)
from ashlar_host.pinned_delta_graphframes import _quote_path


class CanonicalGraphFramesMaterializerTests(unittest.TestCase):
    def test_frozen_input_is_five_nodes_and_four_edges(self):
        document = _frozen_source()
        self.assertEqual(len(document["tables"]["object_current"]), 5)
        self.assertEqual(len(document["tables"]["edge_current"]), 4)

    def test_protocol_gate_requires_exact_writer_seven_features(self):
        detail = {"minReaderVersion": 1, "minWriterVersion": 7,
                  "tableFeatures": EXPECTED_PROTOCOL["writerFeatures"]}
        _protocol(detail)
        for key, value in (("minWriterVersion", 2), ("tableFeatures", ["clustering"])):
            changed = dict(detail)
            changed[key] = value
            with self.assertRaises(PermissionError):
                _protocol(changed)

    def test_umf_uppercase_types_map_to_spark_signature(self):
        fields = (("source_system", "STRING", False),
                  ("id", "BIGINT", False),
                  ("published_at", "TIMESTAMP", False))
        self.assertEqual(_spark_type_names(fields), fields)
        with self.assertRaises(ValueError):
            _spark_type_names((("id", "long", False),))

    def test_detail_must_expose_checked_clustering_and_stats_layout(self):
        layout = {"clusterBy": ["lookup_hash"],
                  "properties": {"delta.dataSkippingStatsColumns":
                                 "lookup_hash,source_system,type_id,id"}}
        detail = {"clusteringColumns": ["lookup_hash"], "properties": {
            "delta.dataSkippingStatsColumns": "lookup_hash,source_system,type_id,id"}}
        _verify_detail_layout(detail, layout)
        for changed in ({"clusteringColumns": [], "properties": detail["properties"]},
                        {"clusteringColumns": ["lookup_hash"], "properties": {}}):
            with self.assertRaises(PermissionError):
                _verify_detail_layout(changed, layout)

    def test_fixture_lock_is_nonblocking_and_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            fd = _acquire_lock(Path(directory))
            try:
                with self.assertRaises(PermissionError):
                    _acquire_lock(Path(directory))
            finally:
                os.close(fd)

    def test_private_root_must_be_canonical_and_nonsymlink(self):
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            self.assertEqual(_private_root(directory), Path(directory).resolve())
            alias = Path(directory).parent / (Path(directory).name + "-alias")
            alias.symlink_to(directory)
            try:
                with self.assertRaises(PermissionError):
                    _private_root(alias)
            finally:
                alias.unlink()

    def test_path_quote_rejects_sql_delimiters_and_controls(self):
        for path in ("/tmp/a`b", "/tmp/a\nb", "/tmp/a\x00b"):
            with self.assertRaises(ValueError):
                _quote_path(path)


if __name__ == "__main__":
    unittest.main()
