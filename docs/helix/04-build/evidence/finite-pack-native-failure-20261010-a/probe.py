"""Exactly one explicitly selected original pack publication; no queries."""
import sys
if sys.path[0]!='/private/tmp':raise ValueError('Explicit native probe selection required')
del sys.path[0]
from pathlib import Path
import hashlib,json
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.finite_dataset import FiniteDatasetConfig
from ashlar_host.finite_native import FinitePackNativeConfig,publish_finite_pack_native
from ashlar_host.lifecycle import owned_context
if len(sys.argv)!=2 or sys.argv[1]not in ('archaeology','ecology'):raise ValueError('Closed original pack command required')
name=sys.argv[1];definition=FinitePackDefinition(name);original=Path('/Users/erik/Projects/ashlar/examples/domain-packs')/name/'upstream'
output=Path('/private/tmp/ashlar-finite-pack-native-20261010-a-'+name)
config=FinitePackNativeConfig(output,Path('/private/tmp/ashlar-supply-chain-finite-native-jars-f'),original/'pack.json',original/'ontology.json',original/'graph/fixture.json',
 FiniteDatasetConfig(definition,Path('/private/tmp/ashlar-umf-compact-clean-07357ead'),Path('/opt/homebrew/Cellar/bun/1.4.2/bin/bun'),Path('/usr/bin/git'),5,1048576,33554432),
 {'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608})
result=publish_finite_pack_native(config)
facts=definition.facts();expected={'object_current':facts['objects'],'edge_current':facts['edges'],'tombstone':0,'whole_source_history':facts['objects']+facts['edges']}
if {r:len(rows)for r,rows in result['native_projection'].items()}!=expected or len(result['original_phase_records'])!=5 or result['original_phase_records'][-1]['phase']!='committed':raise ValueError('Complete actual original native capture required')
receipt=json.loads((output/'public-dataset.json').read_bytes())
if len(receipt['controls'])!=3 or any(c['receipt']['datasetValidation']['valid']is not False for c in receipt['controls']):raise ValueError('All original public negative controls required')
model=(original/'ontology.json').read_bytes();graph=(original/'graph/fixture.json').read_bytes();pack=(original/'pack.json').read_bytes()
raw=json.dumps({'cases':definition.cases(model,graph,pack),'oracle':definition.oracle(model,graph)},separators=(',',':')).encode()
if len(raw)>1048576:raise ValueError('Whole original oracle/case bound')
with owned_context((output/'original-cases-oracle.json').open('xb'))as stream:
 if stream.write(raw)!=len(raw):raise ValueError('Whole original oracle write required')
print(json.dumps({'pack':name,'report':str(output/'report.json'),'report_sha256':hashlib.sha256((output/'report.json').read_bytes()).hexdigest(),'rows':expected,'phase_count':5,'qualification':'Fresh local original finite publication, exact replay and read-only committed reopen; SQL cases retained only, no query/remote/UC/ACK claim.'}))
