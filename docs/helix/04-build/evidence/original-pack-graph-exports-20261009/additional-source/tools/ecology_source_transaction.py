"""Repository convenience imports for the packaged original ecology source converter."""
from pathlib import Path
from ashlar.ecology_source import SOURCE_SHA, GRAPH_SHA, encode, build_transaction, write_candidate
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/ecology/upstream'
