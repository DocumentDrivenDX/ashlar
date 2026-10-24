#!/usr/bin/env python3
"""Print the digest that Sol must review before packet preparation/execution."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from packet_common import packet_tree_digest  # noqa: E402

print(packet_tree_digest())
