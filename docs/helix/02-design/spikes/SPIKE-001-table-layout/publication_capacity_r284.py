"""Source-bound serialized-writer sensitivity; never measured sustained capacity."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_mixed_publish_r281/audited-summary.json';a=json.loads(p.read_text());assert a['state']=='Complete private100k mixed publication passes full intermediate preservation audit';assert a['mutation_s']>0 and a['processing_s']>=a['mutation_s']
 count=100000;out={'format':'ashlar-intermediate-publication-capacity/1','source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'batch_entities':count,'base_nodes':8000000,'base_edges':40000000,'apply_s':a['mutation_s'],'full_processing_s':a['processing_s'],'validation_and_publication_increment_s':a['processing_s']-a['mutation_s'],'models':{}}
 for name,seconds in [('apply_only',a['mutation_s']),('full_audit_publication',a['processing_s'])]:
  out['models'][name]={'single_sample_serial_rate_entities_s':count/seconds,'arrivals':{}}
  for rate in [10000,100000]:
   interval=count/rate;out['models'][name]['arrivals'][str(rate)]={'batch_interval_s':interval,'utilization':seconds/interval,'additional_wait_per_later_batch_s':max(0,seconds-interval),'first_batch_uniform_arrival_age_p95_s':seconds+.95*interval}
 out['qualification']='Arithmetic repeats one observed synthetic service sample on a serialized writer. Not measured service p95, sustained capacity, safe parallelism count, real arrivals or production price. Apply-only excludes validation/publication and cannot pass freshness. Full audit intentionally checks every inherited row; future incremental-custody protocol needs independent correctness and native timing evidence. Owner-selected UC Delta architecture remains fixed.'
 (B/'out/publication-capacity-r284.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out['models'],indent=2))
if __name__=='__main__':main()
