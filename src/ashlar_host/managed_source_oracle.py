"""Original configured-source carrier oracle and retained clock correspondence.

No native reads, publisher SQL plans, snapshot-row oracle or authority grants.
"""
import base64,datetime,hashlib,json,re
from dataclasses import dataclass
from ashlar.source import jsonl_batches
from ashlar.source_config import JsonlConfiguration
from ashlar.typed_source_config import TypedJsonlConfiguration
from ashlar.staging import batch_row
from .managed_carriers import ManagedCarriers
from .managed_sql_read import decode_managed_sql_result

CLOCK_OPERATION='native-example:materialization-clock/1'
CLOCK_SQL='SELECT cast(unix_micros(current_timestamp()) AS STRING) AS now_us, :workload AS workload'

@dataclass(frozen=True)
class RetainedMaterializationClock:
    microseconds:str
    actor_id:str
    warehouse_id:str

def retained_materialization_clock(submissions,workload):
    if type(workload)is not str or re.fullmatch('[0-9a-f]{64}',workload)is None:raise ValueError('Original workload required')
    selected=[r for r in submissions if r[0]==CLOCK_OPERATION]
    if len(selected)!=1:raise ValueError('One original materialization clock required')
    operation,raw,digest,handle,response=selected[0]
    if type(raw)is not str or len(raw.encode())>262144 or hashlib.sha256(raw.encode()).hexdigest()!=digest or type(handle)is not str or not 0<len(handle)<=1024 or type(response)is not str or len(response.encode())>262144:raise ValueError('Original clock submission custody required')
    from ashlar.schema import decode_retained_json
    request=decode_retained_json(raw.encode());body=request['body']
    if (set(request)!={'authority','body'}or type(request['authority'])is not str or not request['authority']
            or type(body.get('row_limit'))is not int or type(body.get('warehouse_id'))is not str or re.fullmatch('[0-9a-f]{16}',body['warehouse_id'])is None
            or body!={'warehouse_id':body['warehouse_id'],'statement':CLOCK_SQL,'parameters':[{'name':'workload','type':'STRING','value':workload}],
                'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000}):raise ValueError('Original clock statement/workload required')
    observed=decode_retained_json(response.encode())
    if observed.get('statement_id')!=handle:raise ValueError('Original clock response handle differs')
    result=decode_managed_sql_result(observed,maximum_rows=1,maximum_bytes=65536)
    if result.columns!=(('now_us','STRING'),('workload','STRING'))or len(result.rows)!=1 or result.rows[0]['workload']!=workload:raise ValueError('Original complete clock result required')
    token=result.rows[0]['now_us']
    if type(token)is not str or re.fullmatch('0|[1-9][0-9]*',token)is None or len(token)>19 or int(token)>2**63-1:raise ValueError('Original signed64 epoch microseconds required')
    try:datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(token))
    except OverflowError:raise ValueError('Original clock outside supported UTC range')from None
    return RetainedMaterializationClock(token,request['authority'],body['warehouse_id'])

@dataclass(frozen=True)
class ConfiguredCarrierRows:
    original_json:str
    def rows(self):return json.loads(self.original_json)

def configured_prefix_carrier_rows(configuration,batches,*,materialization_clock_us,carriers,maximum_rows=1000,maximum_bytes=1048576):
    if (type(configuration)not in (JsonlConfiguration,TypedJsonlConfiguration)or type(carriers)is not ManagedCarriers or type(batches)is not tuple or not 1<=len(batches)<=4
            or type(maximum_rows)is not int or not 1<=maximum_rows<=10000 or type(maximum_bytes)is not int or not 1<=maximum_bytes<=16777216):raise ValueError('Explicit original configured prefix and bounds required')
    from .evolution_cli import InvocationFile
    originals_file=InvocationFile.read(configuration.source,262144)
    configuration.verify();carriers.__post_init__()
    originals=tuple(jsonl_batches(originals_file.raw.splitlines(keepends=True),feed=configuration.feed,epoch=configuration.epoch))
    if originals[:len(batches)]!=batches:raise ValueError('Original complete configured prefix differs')
    if type(materialization_clock_us)is not str or re.fullmatch('0|[1-9][0-9]*',materialization_clock_us)is None or len(materialization_clock_us)>19 or int(materialization_clock_us)>2**63-1:raise ValueError('Exact original epoch microseconds required')
    try:instant=datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(materialization_clock_us))
    except OverflowError:raise ValueError('Original clock outside supported UTC range')from None
    for batch in batches:batch_row(batch)
    columns={role:carriers.ordered_columns(role)for role in ('object_current','edge_current','tombstone','whole_source_history')}
    expected=_original_inventory(batches,columns,materialized_at=instant.isoformat())
    if any(len(rows)>maximum_rows for rows in expected.values()):raise ValueError('Complete original carrier row bound exceeded')
    raw=json.dumps(expected,ensure_ascii=False,separators=(',',':'),allow_nan=False)
    if len(raw.encode())>maximum_bytes:raise ValueError('Complete original carrier byte bound exceeded')
    originals_file.renew(262144);configuration.verify();return ConfiguredCarrierRows(raw)

def _original_inventory(batches, columns, *, materialized_at):
    # Independent source-to-carrier expectations: do not call graph_sql_plan or
    # consume generated SQL rows as an oracle.
    instant=datetime.datetime.fromisoformat(materialized_at)
    if instant.tzinfo is None or instant.utcoffset()!=datetime.timedelta(0):raise ValueError('Explicit original UTC materialization clock required')
    elapsed=instant-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
    stamp=str((elapsed.days*86400+elapsed.seconds)*1000000+elapsed.microseconds)
    current={};history=[];deletes=[];deliveries={}
    for batch in batches:
        for record in batch.records:
            delivery=(batch.feed,batch.epoch,record.delivery_id)
            if delivery in deliveries:
                if deliveries[delivery]!=record.raw:raise ValueError('Original source delivery conflict')
                continue
            deliveries[delivery]=record.raw
            event=json.loads(record.raw);kind=event['entity_kind'];key=(kind,event['source_system'],event['type_id'],event['id'])
            identity={'source_system':event['source_system'],'type_id' if kind=='object' else 'rel_type_id':int(event['type_id']),'id':int(event['id'])}
            row={name:None for name,typ in columns['object_current' if kind=='object' else 'edge_current']}
            row.update({k:str(v) for k,v in identity.items()})
            row.update(schema_revision=event['schema_revision'],entity_version=event['entity_version'],props_json=event['props_json'],retained_json=event['retained_json'],
                       source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=json.dumps({'profile':batch.profile,'offset':batch.cursor_after},separators=(',',':')),
                       source_delivery_id=record.delivery_id,published_at=stamp,lookup_hash=hashlib.sha256(json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),apply_batch_id=batch.batch_id)
            if kind=='object':row['logical_key_json']='[]'
            else:
                a,b=event['endpoints'];row.update(source_type=a['type_id'],source_id=a['id'],target_type=b['type_id'],target_id=b['id'])
            if event['operation']=='delete':
                current.pop(key)
                delete={name:None for name,typ in columns['tombstone']}
                delete.update(source_system=event['source_system'],entity_kind=kind,type_id=event['type_id'],id=event['id'],entity_version=event['entity_version'],source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=row['source_cursor_json'],source_delivery_id=record.delivery_id)
                deletes.append(delete)
            else:current[key]=row
            def typed_key(k,t,i):return {'source':event['source_system'],'kind':k,'type_id':int(t),'id':int(i)}
            state={'key':typed_key(kind,event['type_id'],event['id']),'version':int(event['entity_version']),'schema_revision':event['schema_revision'],'props_json':event['props_json'],'retained_json':event['retained_json'],
                   'endpoints':None if kind=='object' else [typed_key('object',r['type_id'],r['id']) for r in event['endpoints']]}
            change={'feed':batch.feed,'epoch':batch.epoch,'delivery_id':record.delivery_id,'raw_digest':record.sha256,'operation':event['operation'],'state':state}
            history.append({'feed':batch.feed,'epoch':batch.epoch,'delivery_id':record.delivery_id,'digest':record.sha256,'change_json':json.dumps(change,separators=(',',':')),'raw_base64':base64.b64encode(record.raw).decode()})
    expected={'object_current':[r for k,r in current.items() if k[0]=='object'],'edge_current':[r for k,r in current.items() if k[0]=='edge'], 'tombstone':deletes,'whole_source_history':history}
    return expected
