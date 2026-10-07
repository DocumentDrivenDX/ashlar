"""Synthetic normalized-role reader: retain original JSON, cast explicit known fields."""
import re
from mixed_changes_r228 import Changes
from mixed_change_queries_r230 import INTS,BOOLS,TIMES,schema,fields_sql
ROLES=('source_record','property_journal','tombstone','current_replacement')
def role_row(role):
 if role not in ROLES:raise ValueError('Unknown normalized role')
 c=Changes(8000000,40000000,2)
 if role=='tombstone':
  row=c.change(0)['tombstone'];row['entity_version']='2';return row
 x=c.change(1)
 return x['raw'] if role=='source_record' else x['events'][0] if role=='property_journal' else x['after']
def parse_relation(role,relation):
 # relation is private generated SQL, never external text; public paths validated below.
 row=role_row(role)
 return f"SELECT input_json,{fields_sql(row)} FROM (SELECT input_json,from_json(input_json,'{schema(row)}',map('mode','FAILFAST','allowSingleQuotes','false','allowNonNumericNumbers','false')) AS r FROM ({relation}))"
def volume_query(role,path):
 if type(path) is not str or not re.fullmatch(r'/Volumes/[A-Za-z_][A-Za-z0-9_]*/[A-Za-z_][A-Za-z0-9_]*/[A-Za-z_][A-Za-z0-9_]*/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+\.jsonl',path):raise ValueError('Exact owned volume file path required')
 return parse_relation(role,f"SELECT value AS input_json FROM read_files('{path}',format => 'text')")
def inline_query(role,rows):
 if type(rows) is not list or not 0<len(rows)<=100 or any(type(x) is not str for x in rows):raise ValueError('Bounded conformance rows required')
 values=','.join("('"+x.encode().hex()+"')" for x in rows)
 return parse_relation(role,"SELECT decode(unhex(payload_hex),'UTF-8') AS input_json FROM VALUES "+values+' AS v(payload_hex)')
