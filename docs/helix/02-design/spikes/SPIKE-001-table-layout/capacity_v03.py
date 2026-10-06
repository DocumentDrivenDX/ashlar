"""Storage/file-count arithmetic; no native capacity or timing inference."""
import json,math
from pathlib import Path
B=Path(__file__).resolve().parent
NODES=1_000_000_000;EDGES=5_000_000_000;SECONDS_30D=30*86400
scenarios=[]
for carrier_bytes in (512,2048):
 for adjacency_copies in (0,1,2):
  for target_mib in (16,64):
   for mean_fill in (0.5,1.0):
    mean_file_bytes=int(target_mib*1024**2*mean_fill)
    role_bytes={'object_current':NODES*carrier_bytes,'edge_current':EDGES*carrier_bytes}
    if adjacency_copies:role_bytes['adjacency_forward']=EDGES*96
    if adjacency_copies==2:role_bytes['adjacency_reverse']=EDGES*96
    files={r:math.ceil(v/mean_file_bytes) for r,v in role_bytes.items()}
    f=files['edge_current'];changes=300_000
    # Expected distinct file groups under independent uniform edge updates.
    touched=f*(-math.expm1(changes*math.log1p(-1/f))) if f>1 else 1.0
    scenarios.append({'assumed_compressed_carrier_bytes':carrier_bytes,'adjacency_copies':adjacency_copies,'target_file_mib':target_mib,'assumed_mean_fill_fraction':mean_fill,'assumed_mean_file_bytes':mean_file_bytes,'role_bytes':role_bytes,'role_files':files,'current_and_selected_adjacency_bytes':sum(role_bytes.values()),'current_and_selected_adjacency_files':sum(files.values()),'uniform_300k_edge_updates':{'expected_distinct_baseline_file_groups':round(touched,2),'expected_fraction_touched':round(touched/f,6),'meaning':'Independent uniform assignment to baseline equal-size files; not observed pruning, rewrite volume, DV cost or transaction timing.'}})
history=[]
for fanout in (1,4,10):
 entity_changes=10_000*SECONDS_30D
 history.append({'changed_entities_per_second':10000,'days':30,'assumed_journal_events_per_changed_entity':fanout,'assumed_compressed_journal_bytes_per_event':1024,'journal_events':entity_changes*fanout,'journal_bytes':entity_changes*fanout*1024,'assumed_raw_records_per_changed_entity':1,'assumed_compressed_raw_bytes_per_record':1024,'raw_bytes':entity_changes*1024,'journal_plus_raw_bytes':entity_changes*(fanout+1)*1024,'journal_plus_raw_files_at_assumed_mean_64MiB':math.ceil(entity_changes*fanout*1024/(64*1024**2))+math.ceil(entity_changes*1024/(64*1024**2))})
result={'format':'ashlar-capacity-sensitivity/0.3','scale':{'nodes':NODES,'edges':EDGES},'scope':'Arithmetic sensitivity only; every byte width/fill/fanout is assumed after encoding/compression. No measured entropy, runtime capacity, sustained throughput, native file count, pruning, write amplification or cost admission. History and raw bytes are additional physical table storage alongside current/projection bytes; duplicated payload content still consumes storage in each role.','formulas':{'role_files':'ceil(role_bytes / assumed_mean_file_bytes), rounded separately per table','uniform_updates':'F * (1 - (1 - 1/F)^n)','history':'entity_changes_per_second * days * 86400 * property_fanout * stored_event_bytes'},'current_and_adjacency_scenarios':scenarios,'history_and_raw_scenarios':history,'not_estimated':['old retained Delta versions and deletion-vector overhead','maintenance/staging overlap','degree rows, tombstones and coordination metadata','typed projections and external engine release copies','source revision/manifests/reservations and additional raw records','burst duration/duty cycle and real source change rate','Delta log/checkpoint metadata and service planning latency'],'operational_implication':'Uniformly scattered updates can touch most baseline file groups even for a small fraction of edge rows. Cluster pruning for singleton reads does not establish cheap incremental updates. Count actual rewrite/DV/metadata costs before scale growth.'}
(B/'out/capacity-planning-v03.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'state':'generated','current_scenarios':len(scenarios),'history_scenarios':len(history),'scope':result['scope']}))
