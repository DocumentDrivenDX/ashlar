"""Compatibility entrypoint for the installed independent original oracle."""
from pathlib import Path
import json
from ashlar_host.archaeology_oracle import original_oracle,SOURCE_SHA,GRAPH_SHA
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/archaeology/upstream'
if __name__=='__main__':print(json.dumps(original_oracle((ROOT/'ontology.json').read_bytes(),(ROOT/'graph/fixture.json').read_bytes()),ensure_ascii=False,indent=2))
