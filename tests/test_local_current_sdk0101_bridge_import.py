"""Effect-free SDK0101 bridge resolution and table-creator import smoke."""
import unittest

from ashlar_host.local_current_carrier_profile import (
    _sdk0101_bridge_type, create_local_current_xxhash64_sdk0101_tables,
)
from ashlar_host.local_current_delta4_umf_sdk0101 import LocalUmfDelta4Sdk010Bridge


class Sdk0101BridgeImportTests(unittest.TestCase):
    def test_exact_bridge_class_and_table_creator_import_without_spark(self):
        self.assertIs(_sdk0101_bridge_type(), LocalUmfDelta4Sdk010Bridge)
        self.assertTrue(callable(create_local_current_xxhash64_sdk0101_tables))


if __name__ == "__main__":
    unittest.main()
