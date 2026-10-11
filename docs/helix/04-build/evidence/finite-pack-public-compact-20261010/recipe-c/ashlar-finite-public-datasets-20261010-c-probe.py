"""Actual shared public dataset producer only; no source or native authority."""
import sys
if sys.path[0]!='/private/tmp':raise ValueError('Explicit probe directory required')
del sys.path[0]
from pathlib import Path
import hashlib,json
from ashlar_host.finite_pack import FinitePackDefinition,UMF_REVISION
from ashlar_host.finite_dataset import FiniteDatasetConfig,HeldFiniteDataset
from ashlar_host.lifecycle import owned_context
def write_artifact(path,raw):
    with owned_context(path.open('xb'))as stream:
        if stream.write(raw)!=len(raw):raise ValueError('Whole original artifact required')
ROOT=Path('/private/tmp/ashlar-finite-public-datasets-20261010-c')
ROOT.mkdir(mode=0o700)
observations=[]
for name in ('archaeology','ecology'):
    definition=FinitePackDefinition(name)
    original=Path('/Users/erik/Projects/ashlar/examples/domain-packs')/name/'upstream'
    model,graph,pack=[(original/n).read_bytes()for n in ('ontology.json','graph/fixture.json','pack.json')]
    facts=definition.inputs(model,graph,pack);cases=definition.cases(model,graph,pack);oracle=definition.oracle(model,graph)
    config=FiniteDatasetConfig(definition,Path('/private/tmp/ashlar-umf-compact-clean-07357ead'),Path('/opt/homebrew/Cellar/bun/1.4.2/bin/bun'),Path('/usr/bin/git'),5,1048576,33554432)
    output=ROOT/(name+'-public-dataset.json')
    producer=HeldFiniteDataset(config,original/'ontology.json',original/'graph/fixture.json',output)
    raw=producer.receipt(model,graph);value=json.loads(raw);receipt=value['receipt'];authored=json.loads(graph)
    if value['profile']!='ashlar-finite-pack-public-dataset/0.2'or value['pack']!=name or value['umfRevision']!=UMF_REVISION or value['sourceSha256']!=facts['model_sha256']or value['graphSha256']!=facts['graph_sha256']:raise ValueError('Original selected public receipt identity required')
    if receipt['operation']!='validate-core-dataset-values-compact'or receipt['version']!='1.0.0'or receipt['provenance']!='unverified'or receipt['source']!=json.loads(model):raise ValueError('Whole original compact profile required')
    fragments=[r['result']for r in receipt['records']]+[k['result']for k in receipt['keys']]+[e['targetKey']for e in receipt['relationships']]
    if any(f.get('sourceRef')!='#/source'or 'source'in f for f in fragments):raise ValueError('Sole original shared source references required')
    if receipt['datasetValidation']['valid']is not True or receipt['datasetValidation']['complete']is not True or receipt['datasetValidation']['diagnostics']!=[] or any(r['result']['validation']['valid']is not True for r in receipt['records']):raise ValueError('Actual complete public dataset validation required')
    if len(receipt['records'])!=facts['objects']or len(receipt['keys'])!=facts['objects']or {r['instanceId']for r in receipt['records']}!={o['key']for o in authored['objects']}:raise ValueError('Complete original Record/key inventory differs')
    if len(receipt['relationships'])!=facts['edges']or {r['instanceId']:(r['sourceInstanceId'],r['targetInstanceId'])for r in receipt['relationships']}!={e['key']:(e['source'],e['target'])for e in authored['edges']}:raise ValueError('Complete original occurrence/endpoint inventory differs')
    if len(value['originalNulls'])!=facts['present_nulls']or len(value['controls'])!=3 or any(c['receipt']['datasetValidation']['valid']is not False for c in value['controls']):raise ValueError('Original null inventory and actual negative controls required')
    pure=json.dumps({'cases':cases,'oracle':oracle},ensure_ascii=True,separators=(',',':')).encode()
    if len(pure)>1048576:raise ValueError('Complete original oracle artifact bound')
    artifact=ROOT/(name+'-original-cases-oracle.json')
    write_artifact(artifact,pure)
    if producer.receipt(model,graph)!=raw:raise ValueError('Closing original emitted receipt differs')
    observations.append({'pack':name,'facts':facts,'receipt':str(output),'receipt_bytes':len(raw),'receipt_sha256':hashlib.sha256(raw).hexdigest(),'source_stdout':producer.stdout.decode(),'source_stderr':producer.stderr.decode(),'original_cases_oracle':str(artifact),'oracle_sha256':hashlib.sha256(pure).hexdigest(),'qualification':'Actual public finite dataset validation and pure independent original query bags only; original SQL retained, not compiled or executed. No source/native/writer/publication/ACK authority.'})
report={'source_revision':'83528d6249cb86a883ed2b36652fc546ae6eae85','umf_revision':UMF_REVISION,'observations':observations,'scope':'Existing Bun public UMF dataset operation only; no Spark/Delta/PG/install/cloud/benchmark. Original declaredUMF0.8 and whole unknown receipt content retained.'}
raw=json.dumps(report,indent=2).encode()
if len(raw)>1048576:raise ValueError('Complete report bound')
write_artifact(ROOT/'report.json',raw)
print(json.dumps({'report':str(ROOT/'report.json'),'packs':2,'scope':report['scope']}))
