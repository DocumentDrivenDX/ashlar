"""Compatibility entrypoint for the installed exact original source converter.

Development carrier IDs and original source bytes remain unchanged. ROOT is
retained for checkout-only callers; installed code uses explicit caller paths.
"""
from pathlib import Path
from ashlar.supply_chain_source import (
    SOURCE_SHA, GRAPH_SHA, encode, build_transaction, write_candidate,
)
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/supply-chain/upstream'
