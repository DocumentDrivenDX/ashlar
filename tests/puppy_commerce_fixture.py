# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual public original admission construction, without a producer/native rerun."""
import json,tempfile,base64
from pathlib import Path
from ashlar.commerce_source import build_transaction
from ashlar_host.commerce_admission import CommerceAdmission
ROOT=Path(__file__).resolve().parents[1]
def admission(test):
    temporary=tempfile.TemporaryDirectory();test.addCleanup(temporary.cleanup);root=Path(temporary.name)
    source=ROOT/'examples/domain-packs/commerce/upstream';model=(source/'ontology.json').read_bytes();graph=(source/'graph/fixture.json').read_bytes()
    release=json.loads((ROOT/'docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json').read_bytes())
    original=json.loads(release['publication']['validation_report_json'])['source_admission']
    binding_bytes=base64.b64decode(original['original_bindings_base64'],validate=True)
    batch,bindings=build_transaction(model,graph,source_system=release['nodes'][0]['source_system'])
    paths=[root/n for n in ('model.json','graph.json','bindings.json','public.json')]
    for p,raw in zip(paths,[model,graph,binding_bytes,(ROOT/'docs/helix/04-build/evidence/commerce-public-dataset-20261009.json').read_bytes()]):p.write_bytes(raw)
    return CommerceAdmission(batch,bindings,*paths)
