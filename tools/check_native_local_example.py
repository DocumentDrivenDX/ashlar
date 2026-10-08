"""Read-only full-column parity of the retained UMF-backed four-event fixture at exact versions."""
import argparse,hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.effect_validation import validate_effect_snapshot
from databricks_transport import sql_result
from persistent_sql import Client
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source',choices=['local','outbox','csv'],default='local')
parser.add_argument('--materialized-at',default='2026-10-08T17:00:00+00:00',help='Original retained materialization clock; default is historical fixture metadata')
parser.add_argument('--output',help='Fresh output directory for a new observation')
args=parser.parse_args()
N='ashlar_e2e_private_20261008.'+{'local':'runtime','outbox':'runtime_outbox','csv':'runtime_csv'}[args.source]
OUT=B/('out/native/'+{'local':'local_example_full_parity_20261008','outbox':'outbox_delta_full_parity_20261008','csv':'csv_delta_full_parity_20261008'}[args.source])
if args.output:OUT=Path(args.output)
if OUT.exists():raise ValueError('Parity output already exists; retain original evidence and select a fresh output')
c=Client(OUT)
installation=json.loads((B/('out/native/'+{'local':'private_setup_20261008','outbox':'outbox_setup_20261008','csv':'csv_setup_20261008'}[args.source]+'/summary.json')).read_text())
registered={table: value['uuid'] for table,value in installation['tables'].items()}
if installation['namespace']!=N:raise ValueError('Private installation differs')
class Transport:
    def query(self,sql,parameters):
        c.sql('complete-effect-parity',sql,parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
        return sql_result(c.records[-1]['response'])
from fixture_oracle import fixture_columns,fixture_batches,fixture_inventory
transport=Transport()
columns=fixture_columns(ROOT)
source,batches=fixture_batches(ROOT,args.source)
expected,first=fixture_inventory(batches,columns,materialized_at=args.materialized_at)
reports=[];vector={}
for short,rows in expected.items():
    table=N+'.'+short;native_history=transport.query('DESCRIBE HISTORY '+table,{}).rows
    version=max(int(r['version']) for r in native_history);vector[table]=version
    reports.append(validate_effect_snapshot(transport,table,registered[table],version,columns[short],rows))
    if short=='object_current':
        writes=[int(r['version']) for r in native_history if r['operation']=='WRITE']
        initial=min(writes)
        reports.append(validate_effect_snapshot(transport,table,registered[table],initial,columns[short],[r for k,r in first.items() if k[0]==('object' if short=='object_current' else 'edge')]))
summary={'state':'passed','source':args.source,'materialized_at':args.materialized_at,'original_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'version_vector':vector,'reports':reports,'qualification':'Bounded fixture all-column schema/value/multiplicity parity and successful exact-version data reads. Expectations derived directly from original events rather than generated SQL. Historical initial object carrier included; initial edge table remains empty. Does not establish future retained-data availability, protocol policy, active pins, native fencing, publication or accepted Truss schema.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Validated five exact-version full-column inventories; no publication or acknowledgement')
