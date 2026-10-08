"""Bounded read-only native differential for append allocation; no table writes."""
import json
import hashlib
import time
from pathlib import Path
from append_profile_r636 import AppendWorkload, PROFILE, OLD_NODES, OLD_EDGES, NEW_NODES, NEW_EDGES
from scale_mixed_sql_r222 import carrier_sql, literal
from scale_mixed_roles_sql_r224 import role_sql
from persistent_sql import Client
from publication_history import collect_history, HistoryPending

BASE = Path(__file__).resolve().parent
OUT = BASE/'out/native/ashlar_append_native_r637'

def append_sql(kind, start, end):
    old = OLD_NODES if kind == 'node' else OLD_EDGES
    if end <= old:
        return carrier_sql(kind, OLD_NODES, OLD_EDGES, start, end)
    if start < old:
        raise ValueError('Split historical/new intervals explicitly')
    fields = list(AppendWorkload().carrier(kind,start))
    replacements = {'schema_revision':literal(PROFILE), 'source_epoch':literal(PROFILE), 'apply_batch_id':literal('append-calibration')}
    if kind == 'node':
        replacements['id'] = 'id+40000000'
        replacements['logical_key_json'] = "to_json(named_struct('synthetic_native_tuple',array(cast(type_id AS STRING),cast(id+40000000 AS STRING))))"
    else:
        for side in ['source','target']:
            f = side+'_id'
            replacements[f] = f'CASE WHEN {f}>8000000 THEN {f}+40000000 ELSE {f} END'
    inner = 'SELECT '+','.join(replacements.get(f,f)+' AS '+f for f in fields if f!='lookup_hash')+' FROM ('+carrier_sql(kind, NEW_NODES, NEW_EDGES,start,end)+')'
    typ = 'type_id' if kind=='node' else 'rel_type_id'
    return f"SELECT *,sha2(to_json(named_struct('source_system',source_system,'{typ}',{typ},'id',id)),256) lookup_hash FROM ({inner})"

def transport(value):
    if value is None: return None
    if type(value) is bool: return str(value).lower()
    return str(value)

def run():
    assert not OUT.exists(), 'Existing native handles must be inspected, never replayed'
    w = AppendWorkload()
    c = Client(OUT,observation_timeout=180,cancel_after=60)
    started = time.monotonic()
    c.sql('timeout','SET STATEMENT_TIMEOUT=60')
    c.sql('cache','SET use_cached_result=false')
    checks=[]
    for kind,old,total in [('node',OLD_NODES,NEW_NODES),('edge',OLD_EDGES,NEW_EDGES)]:
        for start in [0,old-32,old,total-32]:
            end=start+32
            for role in (['object_current','source_record','property_journal'] if kind=='node' else ['edge_current','source_record','property_journal','adjacency_forward']):
                expected=[r for i in range(start,end) for name,r in w.roles(kind,i) if name==role]
                fields=list(expected[0])
                historical=end<=old
                nodes,edges=(OLD_NODES,OLD_EDGES) if historical else (NEW_NODES,NEW_EDGES)
                q=role_sql(role,kind,nodes,edges,start,end,_carrier_query=append_sql(kind,start,end))
                projection=','.join((literal('2026-10-07T00:00:00Z') if f in ['published_at','received_at'] else 'cast('+f+' AS STRING)')+' AS '+f for f in fields)
                rows=c.sql(f'{kind}-{start}-{role}','SELECT '+projection+' FROM ('+q+')')
                actual=sorted(json.dumps(r,ensure_ascii=False) for r in rows)
                target=sorted(json.dumps([transport(r[f]) for f in fields],ensure_ascii=False) for r in expected)
                assert actual==target,(kind,start,role)
                checks.append({'kind':kind,'start':start,'end':end,'role':role,'rows':len(rows),'sha256':hashlib.sha256('\n'.join(actual).encode()).hexdigest()})
    for attempt in range(12):
        try:
            history=collect_history(c.w,c.records,OUT/'shared-history.json')
            break
        except HistoryPending:
            if attempt==11: raise
            time.sleep(2)
    costs={k:sum(q['metrics'][k] for q in history.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
    assert costs['write_remote_bytes']==0 and costs['spill_to_disk_bytes']==0
    result={'state':'Scoped independent native append parity passes every sampled role field', 'checks':checks,'costs':costs,'wall_s':time.monotonic()-started,'statements':len(c.records),'qualification':'256 carrier boundary/residue sample with exact raw envelopes, lexical property tokens and typed adjacency. Read-only SQL generator differential, not full growth, current publication reconciliation, scale admission or performance proof.'}
    (OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    (OUT/'live-statement.json').unlink(missing_ok=True)
    print(json.dumps(result,indent=2))

if __name__=='__main__': run()
