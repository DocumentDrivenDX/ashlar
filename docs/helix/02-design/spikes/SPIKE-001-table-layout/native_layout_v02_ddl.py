"""Execute proposed0.2 table DDL in a private schema; no scale claim."""
import json,re
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
out=B/'out/native/ashlar_layout_v02_ddl_20261006_r65';c=Client(out)
ns='client_dev.ashlar_layout_v02_20261006_r65'
raw=(B/'sql/delta-layout-v02.sql').read_text()
clean='\n'.join(line for line in raw.splitlines() if not line.lstrip().startswith('--'))
statements=[x.strip() for x in clean.split(';') if x.strip()];assert len(statements)==12
c.sql('schema',f'CREATE SCHEMA {ns}')
tables=[]
for stmt in statements:
 m=re.match(r'CREATE TABLE ([a-z_]+) ',stmt);assert m,stmt
 name=m.group(1);c.sql('ddl-'+name,stmt.replace('CREATE TABLE '+name+' ','CREATE TABLE '+ns+'.'+name+' ',1));tables.append(name)
 rows=c.sql('describe-'+name,f'DESCRIBE TABLE {ns}.{name}');assert rows
 print(name,'created',flush=True)
 (out/'progress.json').write_text(json.dumps({'state':'creating','tables':tables},indent=2))
c.history();(out/'summary.json').write_text(json.dumps({'state':'passed','schema':ns,'tables':tables,'scope':'12 proposed0.2 CREATE statements and DESCRIBE native execution; empty private tables only; no key enforcement, publication, preservation, performance,scale or external-reader claim'},indent=2));print('Layout0.2 DDL execution passed',flush=True)
