"""Owned SQL for the bounded synthetic ASCII property105 apply profile only."""
import re
from dataclasses import dataclass
COLS=('source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position','published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id')
TEXTS={'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
def integer(value,minimum=0):
    if type(value) is not int or not minimum<=value<=9223372036854775807:raise ValueError('Signed64 integer required')
    return value
def table(value):
    if not isinstance(value,str) or not re.fullmatch(r'[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*',value):raise ValueError('Explicit three-part ASCII table identifier required')
    return value
def token(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_.:-]+',value):raise ValueError('Synthetic profile token required')
    return value
def lit(value):return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
def eq(a,b,col):return f"hex(encode({a},'UTF-8')) <=> hex(encode({b},'UTF-8'))" if col in TEXTS else f'{a} <=> {b}'
@dataclass(frozen=True)
class PropertyApply:
    canonical:str
    stage:str
    old_version:int
    previous_entity_version:int
    batch:str
    predecessor:str
    epoch:str
    xid:int
    eligibility_placement:str='matched'
    input_ranges:int=None
    scope_predecessor:bool=False
    def __post_init__(self):
        if type(self.scope_predecessor) is not bool:raise ValueError('Explicit boolean required')
        table(self.canonical);table(self.stage)
        if self.canonical==self.stage:raise ValueError('Stage must be distinct from canonical')
        integer(self.old_version);integer(self.previous_entity_version);integer(self.previous_entity_version+1)
        integer(self.xid,1)
        if self.input_ranges is not None:
            integer(self.input_ranges,1)
            if self.input_ranges>64:raise ValueError('Owned range screen maximum64')
        for value in (self.batch,self.predecessor,self.epoch):token(value)
        if self.batch==self.predecessor:raise ValueError('New batch identity required')
        if self.eligibility_placement not in ('matched','on'):raise ValueError('Owned eligibility placement required')
    def intended(self):
        E,stage,old_v,batch=self.canonical,self.stage,self.old_version,self.batch
        baseline=f'''SELECT /*+ BROADCAST(k) */ b.* FROM {E} VERSION AS OF {old_v} b JOIN
            (SELECT source_system,rel_type_id,id FROM {stage} VERSION AS OF 0) k
            ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id'''
        if self.scope_predecessor:
            baseline+=f' WHERE b.entity_version={self.previous_entity_version} AND b.apply_batch_id={lit(self.predecessor)}'
        expected={col:'b.'+col for col in COLS}
        expected.update(entity_version='b.entity_version+1',
            props_json="replace(b.props_json,concat('\"105\":',s.old_json),concat('\"105\":\"',get_json_object(s.props_json,'$.105'),'\"'))",
            source_epoch="'"+self.epoch+"'",apply_batch_id=lit(batch),published_at='s.published_at',
            source_delivery_id=f"concat('{batch}:edge:',cast(s.id AS STRING))",
            source_cursor_json=f"concat('{{\"xid\":\"{self.xid}\",\"seq\":\"',cast(s.id AS STRING),'\"}}')")
        tests=['NOT('+eq('s.'+col,expected[col],col)+')' for col in COLS]
        return f'''SELECT count(*) FROM ({baseline}) b FULL OUTER JOIN {stage} VERSION AS OF 0 s
            ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id
            WHERE b.id IS NULL OR s.id IS NULL OR instr(b.props_json,concat('"105":',s.old_json))=0
            OR NOT(s.old_json RLIKE '^"[0-9a-f]+"$') OR s.old_json=concat('"',get_json_object(s.props_json,'$.105'),'"')
            OR '''+' OR '.join(tests)
    def apply(self):
        E,stage=self.canonical,self.stage
        query=f'''MERGE INTO {E} t USING (SELECT {','.join(COLS)} FROM {stage} VERSION AS OF 0) s
            ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id
            WHEN MATCHED AND t.entity_version={self.previous_entity_version} AND t.apply_batch_id={lit(self.predecessor)} THEN UPDATE SET *'''
        if self.eligibility_placement=='on':
            query=query.replace('WHEN MATCHED AND t.entity_version=', 'AND t.entity_version=').replace(' THEN UPDATE SET *',' WHEN MATCHED THEN UPDATE SET *')
        if self.input_ranges is not None:
            query=query.replace('USING (SELECT ',f'USING (SELECT /*+ REPARTITION_BY_RANGE({self.input_ranges},lookup_hash) */ ')
        return query
    def output(self,new_version):
        integer(new_version)
        if new_version<=self.old_version:raise ValueError('Newer output snapshot required')
        E,stage,batch=self.canonical,self.stage,self.batch
        checks=['NOT('+eq('a.'+col,'s.'+col,col)+')' for col in COLS]
        return f'''SELECT count(*) FROM (SELECT * FROM {E} VERSION AS OF {new_version}
            WHERE entity_version={self.previous_entity_version+1} AND apply_batch_id={lit(batch)}) a FULL OUTER JOIN {stage} VERSION AS OF 0 s
            ON a.source_system=s.source_system AND a.rel_type_id=s.rel_type_id AND a.id=s.id
            WHERE a.id IS NULL OR s.id IS NULL OR '''+' OR '.join(checks)
    def membership(self):return f'SELECT count(*),count(DISTINCT id) FROM {self.stage} VERSION AS OF 0'
    def identities(self,new_version):
        integer(new_version)
        if new_version<=self.old_version:raise ValueError('Newer output snapshot required')
        return f'SELECT count(*),count(DISTINCT id),count_if(entity_version={self.previous_entity_version+1}) FROM {self.canonical} VERSION AS OF {new_version}'
