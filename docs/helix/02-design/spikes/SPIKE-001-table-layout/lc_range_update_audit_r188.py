"""Audit completed owned range update and same-key historical operational control."""
import json,math
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_range_update_r187'
s=json.loads((O/'checkpoint.json').read_text());assert s['state'].startswith('Range-input update exact;')
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'query-history.json').read_text())}
assert all(x['response']['status']['state']=='SUCCEEDED' and h[x['statement_id']]['status']=='FINISHED' and h[x['statement_id']]['is_final'] for x in r)
for label in ['clone-detail','final-detail']:
 x=next(x for x in r if x['label']==label);names=[c['name'] for c in x['response']['manifest']['schema']['columns']];d=dict(zip(names,x['response']['result']['data_array'][0]));assert d['id']==s['id'] and json.loads(d['clusteringColumns'])==['lookup_hash'];props=json.loads(d['properties']);assert props['delta.enableRowTracking']=='true' and props['delta.enableDeletionVectors']=='true';assert d['minReaderVersion']=='3' and d['minWriterVersion']=='7'
assert len(s['hot_files'])==6;spans=[(int(f[4],16)-int(f[3],16))/2**256 for f in s['hot_files']];assert all(.15<v<.19 for v in spans)
s['hot_hash_domain_fractions']=spans
D=B/'out/native/ashlar_bucket_post_update_reads_r181';control=[json.loads(x) for x in (D/'statements.jsonl').read_text().splitlines()];ch={x['query_id']:x for x in json.loads((D/'query-history.json').read_text())}
keys=next(x for x in control if x['label']=='oracle')['response']['result']['data_array'];p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1]
for i in range(20):
 a=next(x for x in r if x['label']=='read-'+str(i));b=next(x for x in control if x['label']=='lc-read-'+str(i));assert a['response']['result']['data_array']==b['response']['result']['data_array']==[keys[i]]
 assert sum(f[3]<=keys[i][16]<=f[4] for f in s['hot_files'])==1
rs=[next(x for x in control if x['label']=='lc-read-'+str(i)) for i in range(20)]
s['same20_control']={'caller_p95_ms':p95([x['wall_ms'] for x in rs]),**{k+'_p95':p95([ch[x['statement_id']]['metrics'].get(k,0) for x in rs]) for k in ['execution_time_ms','read_bytes','read_files_count']},'qualification':'Earlier same20 keys/control LC1 has same E23 input/stage0 but different physical files and observation time. Not randomized causal comparison.'}
s['qualification']+=' Audit verifies same hash LC/DV/rowTracking/native protocol and same20 control outputs. All20 sampled hashes intersect exactly one new live-file range. Full-precision reconstructed ranges do not replace stored Delta statistics.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'new':s['reads'],'control':s['same20_control'],'apply':s['apply_metrics']['caller_ms'],'costs':s['costs']},indent=2))
