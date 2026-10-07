"""Owned synthetic raw-wire validation; no generic producer fencing claim."""
from dataclasses import dataclass
from property_apply_queries import COLS,table,integer,token,lit
from wire_json import encode

def origin_sql(stage):
    table(stage)
    return f'SELECT source_feed,source_epoch,count(*) FROM {stage} VERSION AS OF 0 GROUP BY source_feed,source_epoch'

@dataclass(frozen=True)
class OriginProof:
    stage:str
    feed:str
    epoch:str
    members:int
    query_id:str

def verified_origin(stage,members,record):
    table(stage);integer(members,1)
    if record.get('sql')!=origin_sql(stage):raise ValueError('Exact owned immutable stage query required')
    if record.get('response',{}).get('status',{}).get('state')!='SUCCEEDED':raise ValueError('Successful native result required')
    qid=record.get('statement_id')
    if not isinstance(qid,str) or not qid:raise ValueError('Native query identity required')
    rows=record.get('response',{}).get('result',{}).get('data_array',[])
    if not isinstance(rows,list) or len(rows)!=1 or not isinstance(rows[0],list) or len(rows[0])!=3:raise ValueError('One complete origin group required')
    feed,epoch,count=rows[0]
    if not isinstance(feed,str) or not feed or not isinstance(epoch,str) or not epoch:raise ValueError('Nonempty native string origins required')
    if count!=str(members):raise ValueError('Exact expected stage membership required')
    return OriginProof(stage,feed,epoch,members,qid)

def raw_validation(stage,raw,raw_version,batch,proof):
    table(stage);table(raw);integer(raw_version);token(batch)
    if type(proof) is not OriginProof or proof.stage!=stage:raise ValueError('Matching owned stage origin proof required')
    # OriginProof construction is internal to the trusted controller. It is not an authentication boundary.
    wire=encode('named_struct('+','.join("'"+col+"',s."+col for col in list(COLS)+['old_json'])+')')
    actual=f"(SELECT * FROM {raw} VERSION AS OF {raw_version} WHERE apply_batch_id={lit(batch)} AND source_feed={lit(proof.feed)} AND source_epoch={lit(proof.epoch)})"
    return f"""SELECT count(*) FROM {stage} VERSION AS OF 0 s LEFT JOIN {actual} r
 ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
 WHERE r.delivery_id IS NULL OR NOT(s.schema_revision <=> r.schema_revision) OR NOT(s.apply_batch_id <=> r.apply_batch_id)
 OR NOT(r.record_kind <=> 'synthetic-scheduled-wide-edge') OR r.received_at IS NULL
 OR NOT(s.source_cursor_json <=> r.source_cursor_json) OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
 OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
 OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({wire},'UTF-8')))"""
