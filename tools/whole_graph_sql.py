"""Host SQL plans for the explicit whole-entity profile; no property-feed alias.

Trusted complete prior state and mandatory schema admission produce a new state
and ordered effects. Persist these exact statements before execution. Separate
writer/source fencing and native prior/parity/UUID validation are mandatory.
"""
from dataclasses import asdict
import base64
import datetime
import hashlib
import json
from ashlar.apply import plan_apply
from ashlar.native import _quoted
from ashlar.whole_entity import changes_from_batch

TABLES={'object_current','edge_current','tombstone','whole_source_history'}


def graph_sql_plan(prior, batch, tables, *, materialized_at, schema_policy, schema_transition_policy=None):
    if set(tables)!=TABLES or len(set(tables.values()))!=4:
        raise ValueError('Four distinct complete graph target names required')
    quoted={k:_quoted(v) for k,v in tables.items()}
    stamp=datetime.datetime.fromisoformat(materialized_at)
    if stamp.tzinfo is None or stamp.utcoffset()!=datetime.timedelta(0):
        raise ValueError('Explicit UTC materialization metadata clock required')
    changes=changes_from_batch(batch)
    after=plan_apply(prior,changes,schema_policy=schema_policy,schema_transition_policy=schema_transition_policy)
    fresh=[x for x in changes if (x.feed,x.epoch,x.delivery_id) not in prior.deliveries]
    steps=[]
    def insert(table,rows,types):
        if not rows:return
        fields=list(rows[0]);definition='ARRAY<STRUCT<'+','.join(k+':STRING' for k in fields)+'>>'
        casts=','.join('cast(r.'+k+' AS '+types.get(k,'STRING')+') AS '+k for k in fields)
        steps.append({'statement':'INSERT INTO '+quoted[table]+' ('+','.join(fields)+') SELECT '+casts+" FROM (SELECT explode(from_json(:rows,'"+definition+"')) r)",
                      'parameters':{'rows':json.dumps(rows,separators=(',',':'))}})
    numeric={k:'BIGINT' for k in ['type_id','rel_type_id','id','entity_version','source_type','source_id','target_type','target_id']}
    for table,kind,typed in [('object_current','object','type_id'),('edge_current','edge','rel_type_id')]:
        touched=sorted({x.state.key for x in fresh if x.state.key.kind==kind})
        if not touched:continue
        keys=[{'source_system':key.source,'type_id':str(key.type_id),'id':str(key.id)} for key in touched]
        steps.append({'statement':'MERGE INTO '+quoted[table]+" t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<source_system:STRING,type_id:STRING,id:STRING>>')) k)) s ON t.source_system=s.source_system AND t."+typed+'=cast(s.type_id AS BIGINT) AND t.id=cast(s.id AS BIGINT) WHEN MATCHED THEN DELETE',
                      'parameters':{'keys':json.dumps(keys,separators=(',',':'))}})
        rows=[]
        for key in touched:
            if key not in after.current:continue
            entity=after.current[key];change=next(x for x in reversed(fresh) if x.state.key==key)
            identity={'source_system':key.source,typed:key.type_id,'id':key.id}
            row={k:str(v) for k,v in identity.items()}
            row.update(schema_revision=entity.schema_revision,entity_version=str(entity.version),props_json=entity.props_json,retained_json=entity.retained_json,
                       source_feed=change.feed,source_epoch=change.epoch,source_cursor_json=json.dumps({'profile':batch.profile,'offset':batch.cursor_after},separators=(',',':')),
                       source_delivery_id=change.delivery_id,published_at=materialized_at,
                       lookup_hash=hashlib.sha256(json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),apply_batch_id=batch.batch_id)
            if kind=='object':row['logical_key_json']='[]'
            else:
                a,b=entity.endpoints
                row.update(source_type=str(a.type_id),source_id=str(a.id),target_type=str(b.type_id),target_id=str(b.id))
            rows.append(row)
        insert(table,rows,dict(numeric,published_at='TIMESTAMP'))
    original={r.delivery_id:r for r in batch.records}
    history=[{'feed':x.feed,'epoch':x.epoch,'delivery_id':x.delivery_id,'digest':x.raw_digest,
              'change_json':json.dumps(asdict(x),separators=(',',':')),
              'raw_base64':base64.b64encode(original[x.delivery_id].raw).decode()} for x in fresh]
    insert('whole_source_history',history,{})
    deletes=[{'source_system':x.state.key.source,'entity_kind':x.state.key.kind,'type_id':str(x.state.key.type_id),'id':str(x.state.key.id),
              'entity_version':str(x.state.version),'source_feed':x.feed,'source_epoch':x.epoch,
              'source_cursor_json':json.dumps({'profile':batch.profile,'offset':batch.cursor_after},separators=(',',':')),'source_delivery_id':x.delivery_id} for x in fresh if x.operation=='delete']
    insert('tombstone',deletes,numeric)
    return after,steps
