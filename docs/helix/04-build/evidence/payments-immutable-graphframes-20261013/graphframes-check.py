import sys,json,time,tempfile,os
from pathlib import Path
root=Path(__file__).parent;(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from ashlar_host.graphframes_export import run_graphframes_export_command
start=time.monotonic()
source=Path('/private/tmp/ashlar-payments-export-native-20261013-a')
report=json.loads((source/'export-report.json').read_bytes())
release=json.loads(Path(report['release']['path']).read_bytes())
expected_paths=sum(a['dst']==b['src'] for a in release['edges'] for b in release['edges'])
value=run_graphframes_export_command(str(source/'graphframes.json'),root/'graphframes.json',maximum_hops=2)
assert (value['nodes'],value['edges'],len(value['one_hop']),len(value['two_hop']))==(13,14,14,expected_paths)
assert value['publication_id']==report['publication_id']
result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'nodes':13,'edges':14,'two_hop_paths':expected_paths,'publication_id':value['publication_id'],'runtime':value['runtime'],'release_sha256':value['release_sha256'],'custody_sha256':value['custody_sha256'],'scope':'Exact canonical graph carriers and topology; monetary property interpretation and financial SQL scenarios unqualified.'}
(root/'run-result.json').write_text(json.dumps(result,indent=2)+'\n')
