"""Local recorded-range overlap analysis; no native reads or inferred actual matches."""
import bisect,hashlib,json,math
from pathlib import Path
from mixed_history_r215 import compact
from scale_mixed_r219 import source,Workload
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_growth_pruning_r282/audited-summary.json';a=json.loads(p.read_text());keys=[]
 for i in range(100000):
  ordinal=i*104729%40000000;key=hashlib.sha256(compact({'source_system':source(ordinal),'rel_type_id':7+ordinal%3,'id':8000000+ordinal+1}).encode()).hexdigest();keys.append(key)
  if i in [0,1,99999]:assert key==Workload(8000000,40000000).carrier('edge',ordinal)['lookup_hash']
 keys.sort();assert len(set(keys))==100000
 ranges=[]
 for path,size,rows,lo,hi in a['files']:
  candidates=bisect.bisect_right(keys,hi)-bisect.bisect_left(keys,lo);ranges.append({'path':path,'bytes':int(size),'rows':int(rows),'hash_span':(int(hi,16)-int(lo,16))/2**256,'selected_hashes_in_range':candidates})
 ideal=[]
 for files in [614,75000,211134]:
  for changed in [10000,100000]:
   touched=files*(-math.expm1(changed*math.log1p(-1/files)));ideal.append({'equal_disjoint_files':files,'uniform_independent_changed_keys':changed,'expected_touched_files':touched,'expected_fraction':touched/files,'at_64MiB_candidate_bytes':touched*64*1024**2})
 result={'format':'ashlar-recorded-range-fanout/1','state':'Recorded file-range candidate overlap quantified locally','source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'snapshot':a['table'],'selected_keys':100000,'files':len(ranges),'files_with_candidate_keys':sum(r['selected_hashes_in_range']>0 for r in ranges),'candidate_file_bytes':sum(r['bytes'] for r in ranges if r['selected_hashes_in_range']),'wide_files_over_99_percent_domain':sum(r['hash_span']>.99 for r in ranges),'wide_file_bytes':sum(r['bytes'] for r in ranges if r['hash_span']>.99),'total_range_key_intersections':sum(r['selected_hashes_in_range'] for r in ranges),'files_detail':ranges,'ideal_uniform_occupancy_arithmetic':ideal,'qualification':'Observed post-r281 snapshot ranges, not original r274 MERGE scan or persisted Delta statistics. Min/max range inclusion is candidate overlap, never proof a row lives in that file; deletions may leave empty candidate matches. Synthetic100k hash keys exactly reconstructed and spot checked against carrier generator. Ideal occupancy uses equal disjoint uniform files and independent keys, unlike overlapping measured ranges;64MiB quotient is neither actual I/O nor runtime/storage admission. No service or billion-scale performance claim.'}
 (B/'out/file-fanout-r297.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k not in ['files_detail','ideal_uniform_occupancy_arithmetic']},indent=2))
if __name__=='__main__':main()
