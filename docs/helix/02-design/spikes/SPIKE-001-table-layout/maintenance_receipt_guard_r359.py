"""Offline controlled-lineage receipt guard; not a lock or publication writer."""
import copy, hashlib, json
from pathlib import Path
B=Path(__file__).resolve().parent

def require(ok, reason):
    if not ok: raise ValueError(reason)

def check(a):
    require(a['before_detail']['id']==a['uuid']==a['after_detail']['id'], 'table UUID')
    lo,hi=a['before_version'],a['after_version']
    require(type(lo) is int and type(hi) is int and hi>lo, 'version interval')
    require([int(c['version']) for c in a['commits']]==list(range(hi,lo,-1)), 'complete ordered commit interval')
    require(all(c['operation']=='OPTIMIZE' and c['queryHistoryStatementId']==a['optimize_statement_id'] and int(c['readVersion'])==lo for c in a['commits']), 'owned maintenance custody')
    for key in ['minReaderVersion','minWriterVersion','tableFeatures','partitionColumns','clusteringColumns']:
        require(a['before_detail'][key]==a['after_detail'][key], 'physical profile changed: '+key)
    before,after=a['checks']['before'],a['checks']['after']
    require(len(before)==len(after)==400 and [int(r[0]) for r in before]==list(range(400)) and [int(r[0]) for r in after]==list(range(400)), 'complete group coverage')
    require(before==after and sum(int(r[1]) for r in before)==39980000, 'full carrier digest preservation')
    require(all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<a['bounds']['wall_s'], 'resource budget')
    require(a['audit']['complete_carrier_rows']==39980000 and a['audit']['uncached_full_carrier_groups']==400, 'audit scope')
    return {'eligibility':'controlled fixture receipt only','versions':list(range(lo+1,hi+1)), 'selected_version':hi, 'logical_progress':'must remain unchanged; not present in this receipt', 'production_manifest':'refused: no real fencing, schema revision or active-pin registry evidence'}

def main():
    source=B/'out/native/ashlar_lc_prefix_maintain_r350/audited-summary.json';a=json.loads(source.read_text())
    for name,digest in a['audit']['source_sha256'].items(): require(hashlib.sha256((source.parent/name).read_bytes()).hexdigest()==digest,'native source hash')
    controls=[]
    def refuse(name, mutate):
        changed=copy.deepcopy(a);mutate(changed)
        try: check(changed)
        except ValueError as e: controls.append({'control':name,'result':'refused','reason':str(e)})
        else: raise AssertionError('Accepted corrupted receipt: '+name)
    refuse('replaced table UUID',lambda x:x['after_detail'].update(id='replacement'))
    refuse('intervening MERGE',lambda x:x['commits'][0].update(operation='MERGE'))
    refuse('unknown OPTIMIZE writer',lambda x:x['commits'][0].update(queryHistoryStatementId='unknown'))
    refuse('missing zero-file commit',lambda x:x['commits'].pop(0))
    refuse('duplicate commit',lambda x:x['commits'].append(copy.deepcopy(x['commits'][0])))
    refuse('reversed ledger',lambda x:x['commits'].reverse())
    refuse('different base read version',lambda x:x['commits'][0].update(readVersion='4'))
    refuse('changed carrier token digest',lambda x:x['checks']['after'][0].__setitem__(2,'0'*64))
    refuse('missing group on both sides',lambda x:[x['checks'][k].pop() for k in ['before','after']])
    refuse('duplicated group on both sides',lambda x:[x['checks'][k].__setitem__(1,copy.deepcopy(x['checks'][k][0])) for k in ['before','after']])
    refuse('reader protocol drift',lambda x:x['after_detail'].update(minReaderVersion='99'))
    refuse('read budget exceeded',lambda x:x['costs'].update(read_bytes=x['bounds']['read_bytes']+1))
    result={'format':'ashlar-maintenance-receipt-controls/1','source':str(source.relative_to(B)),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'positive':check(a),'negative_controls':controls,'scope':'Local alterations of saved native evidence; no native fault injection, schema proof, publication, scheduler or writer-fence implementation.'}
    (B/'out/maintenance-receipt-controls-r359.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'positive':result['positive'],'refusals':len(controls)},indent=2))
if __name__=='__main__': main()
