"""Exact multiset comparison for the scoped synthetic property journal."""
from property_apply_queries import table,integer,token,lit
COLS=('source_system','entity_kind','type_id','id','property_id','entity_version','operation','old_present','old_json','new_present','new_json','schema_revision','source_feed','source_epoch','source_position','event_ordinal','source_time_text','published_at','apply_batch_id','source_cursor_json','source_delivery_id')
TEXTS={'source_system','entity_kind','operation','old_json','new_json','schema_revision','source_feed','source_epoch','source_time_text','apply_batch_id','source_cursor_json','source_delivery_id'}
def expected(stage):
 table(stage)
 return f"""SELECT source_system,'edge' entity_kind,rel_type_id type_id,id,cast(105 AS BIGINT) property_id,
 entity_version,'set' operation,true old_present,old_json,true new_present,concat('"',get_json_object(props_json,'$.105'),'"') new_json,
 schema_revision,source_feed,source_epoch,source_position,cast(0 AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,
 published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM {stage} VERSION AS OF 0"""
def actual(journal,version,batch):
 table(journal);integer(version);token(batch)
 return f'SELECT * FROM {journal} VERSION AS OF {version} WHERE apply_batch_id={lit(batch)}'
def symmetric(left,right):return f'SELECT count(*) FROM (({left} EXCEPT ALL {right}) UNION ALL ({right} EXCEPT ALL {left}))'
def multiset(left,right):
 # Caller supplies owned relational expressions; this is not a public arbitrary SQL API.
 fields=','.join(f"hex(encode({col},'UTF-8')) AS {col}" if col in TEXTS else col for col in COLS)
 keys=','.join(COLS)
 return f"""SELECT count(*) FROM (SELECT {keys},sum(__delta) balance FROM
 ((SELECT {fields},cast(1 AS BIGINT) __delta FROM ({left})) UNION ALL
 (SELECT {fields},cast(-1 AS BIGINT) __delta FROM ({right})))
 GROUP BY {keys} HAVING sum(__delta)<>0)"""
