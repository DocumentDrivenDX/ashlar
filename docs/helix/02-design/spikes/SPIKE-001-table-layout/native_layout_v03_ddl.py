"""Execute proposed 0.3 in a fresh private schema; never migrate 0.2 tables."""
import json,re
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;N='client_dev.ashlar_layout_v03_20261006_r73'
out=B/'out/native/ashlar_layout_v03_ddl_20261006_r73';c=Client(out)
s='\n'.join(line for line in (B/'sql/delta-layout-v03.sql').read_text().splitlines() if not line.strip().startswith('--'))
statements=[x.strip() for x in s.split(';') if x.strip()]
assert len(statements)==13 and all(x.startswith('CREATE TABLE ') for x in statements)
c.sql('create-schema',f'CREATE SCHEMA {N}')
tables=[]
for stmt in statements:
 table=re.match(r'CREATE TABLE (\w+)',stmt)[1];tables.append(table)
 c.sql('create-'+table,stmt.replace('CREATE TABLE '+table+' (','CREATE TABLE '+N+'.'+table+' (',1))
for table in ['object_current','edge_current','property_journal','tombstone']:
 cols={row[0]:row[1] for row in c.sql('describe-'+table,f'DESCRIBE TABLE {N}.{table}')}
 assert cols['source_cursor_json']=='string' and cols['source_delivery_id']=='string'
assert {row[1] for row in c.sql('tables',f'SHOW TABLES IN {N}')}==set(tables)
(out/'summary.json').write_text(json.dumps({'state':'passed','schema':N,'tables':tables,'cursor_reference_tables':['object_current','edge_current','property_journal','tombstone'],'scope':'Thirteen native CREATEs and new cursor/reference column checks; empty private tables. No reference enforcement, producer transport, publication, performance, scale or engine support claim.'},indent=2)+'\n')
print('All 13 proposed 0.3 native table definitions passed',flush=True)
