"""Repository convenience imports for the packaged original archaeology source converter."""
from pathlib import Path
from ashlar.archaeology_source import SOURCE_SHA, GRAPH_SHA, encode, build_transaction, write_candidate
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/archaeology/upstream'
