"""Convenience imports for the explicit original historical medical converter."""
from pathlib import Path
from ashlar.medical_source import SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA,encode,build_transaction,write_candidate
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/medical'
