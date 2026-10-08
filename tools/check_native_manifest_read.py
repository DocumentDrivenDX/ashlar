"""Check real manifest read transport and microsecond custody without publication writes."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.native import NativeBackend
from ashlar.publication import ResolutionError,resolve_publication
from persistent_sql import Client
from databricks_transport import sql_result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():parser.error('New output directory required to preserve receipts')
    c=Client(args.output)
    class Transport:
        def query(self,sql,parameters):
            c.sql('manifest-read-control',sql,parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
            return sql_result(c.records[-1]['response'])
    transport=Transport()
    table='ashlar_e2e_private_20261008.runtime_outbox.publication_manifest'
    class Policy:
        def authorize(self,context,publication_id,tables):
            if context!='read-only-empty-control':raise PermissionError('Wrong observation context')
        def validate_descriptor(self,*args):raise PermissionError('No publication admission supplied')
        def validate_snapshot(self,*args):raise PermissionError('No retention admission supplied')
    backend=NativeBackend(transport,Policy(),table,'468b756d-0c99-42c5-b9b0-1072158c46d0',{})
    object_table='ashlar_e2e_private_20261008.runtime_outbox.object_current'
    try:
        resolve_publication(backend,'not-a-publication-read-control',
            {object_table:'6acb58c4-f660-4f38-8221-1edbb3248eb0'},context='read-only-empty-control',
            supported_profiles=['ashlar-delta/0.3'],supported_revisions={'fixture':['3']})
    except ResolutionError as exc:
        if str(exc)!='Missing or ambiguous publication descriptor':raise
    else:raise ValueError('Absent manifest unexpectedly resolved')
    # A one-row SQL expression checks temporal conversion, not a native manifest.
    clock='1791450000123456'
    observation=transport.query('SELECT cast(unix_micros(timestamp_micros(cast(:clock AS BIGINT))) AS STRING) AS recorded_at',{'clock':clock})
    if observation.rows!=[{'recorded_at':clock}] or observation.columns!=(('recorded_at','STRING'),):raise ValueError('Microsecond text was not preserved')
    summary={'state':'passed','manifest_uuid':backend.manifest_uuid,'absent_publication_refused':True,
        'recorded_at':clock,'original_statements':len(c.records),
        'qualification':'Actual empty native manifest read and lossless SQL timestamp conversion only. No manifest write, admitted descriptor, pin registration, retention proof, source ACK or composed native singleton publication.'}
    (args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))
if __name__=='__main__':main()
