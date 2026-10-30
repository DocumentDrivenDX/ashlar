#!/usr/bin/env python3
"""Print the SHA-256 identity Sol reviews before a one-shot local attempt."""
from pathlib import Path
import sys

PACKET = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET))
from packet_common import packet_digest  # noqa: E402

print(packet_digest())
