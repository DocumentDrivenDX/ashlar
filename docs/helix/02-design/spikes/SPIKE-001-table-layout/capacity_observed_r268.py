"""Source-bound billion-scale arithmetic from observed synthetic physical/input sizes."""
import json,hashlib,math
from decimal import Decimal,ROUND_CEILING
from pathlib import Path
B=Path(__file__).resolve().parent
names=['out/native/ashlar_scale_growth_r248/audited-summary.json','out/native/ashlar_scale_edges_r257/audited-summary.json','out/native/ashlar_growth_pruning_r258/audited-summary.json','out/native/ashlar_growth_pruning_r259/audited-summary.json','out/mixed-batch-oracle-r262.json']
n,g,np,ep,change=[json.loads((B/x).read_text()) for x in names]
ceil=lambda x:int(Decimal(x).to_integral_value(rounding=ROUND_CEILING))
scale=lambda size,rows,target:ceil(Decimal(size)*Decimal(target)/Decimal(rows))
roles={}
def add(role,parts):
 out=[]
 for label,size,files,rows,target in parts:out.append({'component':label,'observed_bytes':int(size),'observed_files':int(files),'observed_entities':rows,'target_entities':target,'scaled_bytes':scale(size,rows,target),'linear_emitted_files':scale(files,rows,target)})
 total=sum(x['scaled_bytes'] for x in out);roles[role]={'components':out,'scaled_bytes':total,'linear_emitted_files':sum(x['linear_emitted_files'] for x in out),'theoretical_files_at64MiB':ceil(Decimal(total)/Decimal(64*1024*1024))}
add('object_current',[('node',np['audit']['pinned_live_file_bytes'],np['audit']['files'],8000000,1000000000)])
add('edge_current',[('edge',ep['audit']['pinned_live_file_bytes'],ep['audit']['files'],16000000,5000000000)])
for role in ['source_record','property_journal']:
 d=n['active_details'][role];m=json.loads(g['commits'][role]['operationMetrics'])
 add(role,[('node-only bootstrap',d['sizeInBytes'],d['numFiles'],8000000,1000000000),('edge8M append',m['numOutputBytes'],m['numFiles'],8000000,5000000000)])
d=g['active_details']['adjacency_forward'];add('adjacency_forward',[('edge',d['sizeInBytes'],d['numFiles'],16000000,5000000000)])
active=sum(r['scaled_bytes'] for r in roles.values());input_roles={k:{'batch_rows':v['rows'],'batch_jsonl_bytes':v['utf8_jsonl_bytes'],'bytes_per_changed_entity':str(Decimal(v['utf8_jsonl_bytes'])/100000),'jsonl_bytes_per_s_at10k_changes':v['utf8_jsonl_bytes']//10,'jsonl_bytes_per_s_at100k_changes':v['utf8_jsonl_bytes']} for k,v in change['roles'].items()}
a={'format':'ashlar-observed-capacity-arithmetic/1','source_sha256':{x:hashlib.sha256((B/x).read_bytes()).hexdigest() for x in names},'final_owner_target':{'nodes':1000000000,'edges':5000000000},'roles':roles,'totals':{'scaled_active_bytes':active,'linear_emitted_files':sum(r['linear_emitted_files'] for r in roles.values()),'theoretical_files_at64MiB':sum(r['theoretical_files_at64MiB'] for r in roles.values()),'active_1_5x_size_sensitivity':ceil(Decimal(active)*Decimal('1.5'))},'bootstrap_journal_rows_assumed':24000000000,'change_input_roles':input_roles,'normalized_input_bytes_per_s':{'10k_changes':sum(v['utf8_jsonl_bytes'] for v in change['roles'].values())//10,'100k_changes':sum(v['utf8_jsonl_bytes'] for v in change['roles'].values())},'qualification':'Arithmetic only, not billion load admission/performance proof. Node/edge current use pinned live sizes; mixed raw/history use separately observed node-only bootstrap and edge append output to avoid assuming current node/edge mix equals final mix. Assumes same synthetic entropy/property density/4 bootstrap events per entity; larger ID widths, distributions, Delta protocol/statistics/metadata, real history rates and compaction may change ratios. Linear emitted file count differs from idealized64MiB quotient; actual CTAS omitted64MiB target. Excludes tombstones, source/input staging, retained removed files, logs/checkpoints/DVs, failed files, future change-history retention, duplicate candidates, serving-engine/projection copies, peak spill, compute and monetary price. Sequential physical observations are not atomic retained inventory. Input rates are exact uncompressed local role JSONL arithmetic, not measured native compressed/storage/network throughput or sustained10k/s/100k/s capability. UC Delta selected; UMF deferred and external mapping scope unchanged.'}
(B/'out/capacity-observed-r268.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'totals':a['totals'],'normalized_input_bytes_per_s':a['normalized_input_bytes_per_s']},indent=2))
