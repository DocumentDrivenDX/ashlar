"""Deterministic synthetic 4M-node/20M-edge physical sensitivity workload.
Full 0.3 current carriers; history/source tables deliberately out of scope.
"""
NODES=4_000_000
EDGES=20_000_000
UPDATES=200_000
SEED=104729

def select(kind, count, updated=False, entropy=False, truss_shape=False):
    if truss_shape and count>(NODES if kind=='object' else EDGES):
        raise ValueError('Declared shared-allocation/relationship profile exceeds its 4M/20M scope')
    typ='type_id' if kind=='object' else 'rel_type_id'
    identity=f"named_struct('source_system',source_system,'{typ}',{typ},'id',id)"
    props="""CASE pmod(id,8)
      WHEN 0 THEN '{}'
      WHEN 1 THEN '{"101":null}'
      WHEN 2 THEN '{"102":9007199254740993,"103":1.2300000000000000001}'
      WHEN 3 THEN '{"104":"2026-10-06T09:30:00.123456789-04:00"}'
      ELSE concat('{"105":"',sha2(cast(id AS STRING),256),
        repeat(sha2(concat('payload:',cast(id AS STRING)),256),cast(2+pmod(id,30) AS INT)),
        '","106":',cast(pmod(id,100) AS STRING),'}') END"""
    if entropy:
        props=props.replace("repeat(sha2(concat('payload:',cast(id AS STRING)),256),cast(2+pmod(id,30) AS INT))", "concat_ws('',transform(sequence(0,cast(63+pmod(id,65) AS INT)), block -> sha2(concat('entropy-r86:',cast(id AS STRING),':',cast(block AS STRING)),256)))")
    if updated: props="concat('{\"107\":true',CASE WHEN ("+props+")='{}' THEN '}' ELSE concat(',',substring(("+props+"),2)) END)"
    keys=f"cast(pmod(id,32)+1 AS BIGINT) {typ}"
    src=f"(pmod(id*17,{NODES//10})*10+pmod(id-1,10)+1)"
    dst=f"(pmod(id*31+11,{NODES//10})*10+pmod(id-1,10)+1)"
    if truss_shape and kind=='edge':
        src=f"(pmod(id-1,{NODES})+1)"
        hop=f"(cast(floor((id-{NODES}-1)/{NODES}) AS BIGINT)+1)"
        dst=f"(pmod({src}-1+{hop}*10,{NODES})+1)"
        keys=f"cast(1000+(pmod({src},32)+1)*5+{hop}-1 AS BIGINT) {typ}"
    endpoints='' if kind=='object' else f",cast(pmod({src},32)+1 AS BIGINT) source_type,cast({src} AS BIGINT) source_id,cast(pmod({dst},32)+1 AS BIGINT) target_type,cast({dst} AS BIGINT) target_id"

    logical="to_json(array(id)) logical_key_json," if kind=='object' else ''
    structural='cast(NULL AS BIGINT) root_id,' if kind=='object' else "cast(NULL AS STRING) order_key,"
    native='source_system,'+typ+',id,'+logical
    if kind=='edge':native+='source_type,source_id,target_type,target_id,'
    ids='pmod(id*104729,20000000)+1' if updated else 'id+1'
    if truss_shape and kind=='edge':ids=f"({ids})+{NODES}"
    return f"""SELECT {native}'synthetic-r1' schema_revision,cast({1 if updated else 0} AS BIGINT) entity_version,
      {props} props_json,concat('{{\"unknown\":{{\"ordinal\":',cast(id AS STRING),',\"nested\":[null,true,\"opaque\"]}}}}') retained_json,
      {structural}'synthetic' source_feed,'scale-1' source_epoch,cast(NULL AS BIGINT) source_position,
      timestamp '2026-10-06 00:00:00' published_at,sha2(to_json({identity}),256) lookup_hash,
      '{'update' if updated else 'baseline'}' apply_batch_id,
      concat('{{\"xid\":\"9007199254740993\",\"seq\":\"',cast(id AS STRING),'\"}}') source_cursor_json,
      concat('{kind}:',cast(id AS STRING)) source_delivery_id
      FROM (SELECT id,CASE WHEN pmod(id,10)=0 THEN 'other' ELSE 'pilot' END source_system,{keys}{endpoints}
      FROM (SELECT {ids} id FROM range({count})))"""

def parity_sql(table,kind,count,entropy=False,truss_shape=False):
    """Exact per-field outer comparison; caller first proves unique ID cardinality.

    This synthetic shared-allocation fixture has globally unique native IDs.
    It is not a generic profile's identity join or production uniqueness check.
    """
    cols=['source_system',('type_id' if kind=='object' else 'rel_type_id'),'id']+(['logical_key_json'] if kind=='object' else ['source_type','source_id','target_type','target_id'])+['schema_revision','entity_version','props_json','retained_json',('root_id' if kind=='object' else 'order_key'),'source_feed','source_epoch','source_position','published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
    text_cols={'source_system','logical_key_json','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
    tests=[f"NOT (hex(encode(a.{col},'UTF-8')) <=> hex(encode(b.{col},'UTF-8')))" if col in text_cols else f'NOT (a.{col} <=> b.{col})' for col in cols]
    return f'SELECT count(*) FROM {table} a FULL OUTER JOIN ({select(kind,count,entropy=entropy,truss_shape=truss_shape)}) b ON a.id=b.id WHERE a.id IS NULL OR b.id IS NULL OR '+ ' OR '.join(tests)
