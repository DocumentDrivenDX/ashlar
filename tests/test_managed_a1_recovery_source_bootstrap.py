import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tools.managed_a1_recovery_source_bootstrap import RetainedA0SourceHold


class RetainedA0SourceHoldCloseTests(unittest.TestCase):
    def test_orderly_cleanup_skips_ipc_when_ack_already_closed_channel(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            worker_config = root / "worker-config.json"
            worker_config.write_bytes(b"private worker config")
            source_config = json.dumps({"root": str(root)}, sort_keys=True,
                separators=(",", ":")).encode()
            owner = Mock()
            owner._closed = True
            owner.operation_timeout_seconds = 30
            owner.process_group_id = 12345
            owner.close_before_ack.side_effect = AssertionError("closed owner must not receive IPC")
            owner.channel.settimeout.side_effect = OSError("closed channel")
            adapter = RetainedA0SourceHold(owner)
            adapter.worker_config_path = worker_config

            with patch("tools.managed_a1_recovery_source_bootstrap._decode_config",
                       return_value=({}, source_config, {})), \
                 patch("tools.managed_a1_recovery_source_bootstrap._wait_worker_group_absent"), \
                 patch("tools.managed_a1_recovery_source_bootstrap._require_existing_pgdata"), \
                 patch("tools.managed_a1_recovery_source_bootstrap._remove_worker_config") as remove:
                result = adapter.close(deadline=10**12, orderly=True)

            self.assertIsNone(result)
            owner.close_before_ack.assert_not_called()
            owner.channel.settimeout.assert_not_called()
            remove.assert_called_once_with(worker_config)


if __name__ == "__main__":
    unittest.main()
