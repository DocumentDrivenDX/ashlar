"""Fixed evidence composition only; general publisher admission is not implemented."""
import hashlib,json
from pathlib import Path
from changed_structure_guard import guard
B=Path(__file__).resolve().parent
PROFILE_SHA256='2428cc0075116ae2138fc602b3e0b06289465459acf1352425257e0b7abaab47'

def prove(profile,records,histories,commits):
    assert hashlib.sha256(json.dumps(profile,sort_keys=True,separators=(',',':')).encode()).hexdigest()==PROFILE_SHA256
    proofs={}
    for label,digest in profile['owned_query_sha256'].items():
        matching=[r for r in records if r['label']==label];assert len(matching)==1
        r=matching[0];assert hashlib.sha256(r['sql'].encode()).hexdigest()==digest
        assert r['response']['status']['state']=='SUCCEEDED'
        h=histories[r['statement_id']];assert h['is_final'] and h['status']=='FINISHED'
        assert h['query_text'].endswith(r['sql'])
        if label.startswith(('intended-','output-parity-','adjacency-reuse-')):assert r['response']['result']['data_array']==[['0']]
        if label.startswith('input-membership-'):assert r['response']['result']['data_array']==[['100000','100000']]
        if label.startswith('global-identities-'):assert r['response']['result']['data_array']==[['20000000','20000000','100000']]
        proofs[label]=r['statement_id']
    # The reviewed profile pins old17/new18, original immutable stage0 and adjacency1.
    assert profile['old_edge_version']==17 and profile['new_edge_version']==18
    assert profile['members']==100000 and profile['stage_version']==0
    assert profile['adjacency_version']==1
    guard(commits,17,18,proofs['apply-r123-b1'],100000)
    return {'structural_reuse':True,'old_edge_version':17,'new_edge_version':18,'adjacency_version':1,'proof_query_ids':proofs}

def load_evidence():
    records=[];hist={}
    for run in ('r121','r123'):
        p=B/f'out/native/ashlar_isolation_{run}'
        records.extend(json.loads(l) for l in (p/'statements.jsonl').read_text().splitlines())
        hist.update({h['query_id']:h for h in json.loads((p/'query-history.json').read_text())})
    profile=json.loads((B/'composed_structure_profile_r126.json').read_text())
    s=json.loads((B/'out/native/ashlar_isolation_r123/audited-summary.json').read_text())
    return profile,records,hist,s['batches'][0]['commits']
if __name__=='__main__':
    result=prove(*load_evidence())
    result['qualification']='Fixed reviewed fixture query hashes, trusted local evidence and native history binding. Baseline full20M structural proof plus intended/output full-carrier checks and closed owned MERGE lineage imply reuse. Not a generic SQL verifier, producer authority or concurrent fence. Independent r123 full20M oracle also passed; no performance saving measured here.'
    out=B/'out/composed-structure-r126.json';out.write_text(json.dumps(result,indent=2)+'\n');print('Recorded structural reuse composition passed')
