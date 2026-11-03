# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected current third-party pin metadata stays aligned across owners."""
import json
from pathlib import Path
import unittest

from ashlar_host.managed_sql_read import DATABRICKS_SDK_VERSION
from ashlar_host.otel_profile import SDK_VERSIONS


class SelectedDependencyPinTests(unittest.TestCase):
    def test_pyproject_and_runtime_profile_match_selected_current_pins(self):
        root = Path(__file__).resolve().parents[1]
        pyproject = (root / "pyproject.toml").read_text()
        self.assertIn(f'databricks-sdk=={DATABRICKS_SDK_VERSION}', pyproject)
        for name, version in SDK_VERSIONS.items():
            self.assertIn(f'"{name}=={version}; python_version == \'3.11\'"',
                          pyproject)
        self.assertIn('"pyspark==4.2.0"', pyproject)
        self.assertIn('"delta-spark==4.4.1"', pyproject)

    def test_diagnostic_schema_tracks_selected_otel_api_version(self):
        root = Path(__file__).resolve().parents[1]
        schema = json.loads((root / "docs/helix/02-design/contracts/schemas/"
            "diagnostic-event-v0.1.schema.json").read_text())
        self.assertEqual(schema["$defs"]["resource"]["properties"][
            "telemetry.sdk.version"]["const"], SDK_VERSIONS["opentelemetry-api"])


if __name__ == "__main__":
    unittest.main()
