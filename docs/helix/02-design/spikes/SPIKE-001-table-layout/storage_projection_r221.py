"""Actual tiny distinct-native role widths: explicit extrapolation uncertainty."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent;S='out/native/ashlar_mixed_r220/summary.json'
def calculate():
 d=json.loads((B/S).read_text());assert d['state']=='All five distinct-payload native roles match every expected field exactly'
 counts={'object_current':8000000,'edge_current':40000000,'source_record':48000000,'property_journal':192000000,'adjacency_forward':40000000}
 roles={}
 for name,x in d['tables'].items():
  width=x['bytes']/x['rows'];roles[name]={'sample_rows':x['rows'],'sample_bytes':x['bytes'],'sample_files':x['files'],'bytes_per_row_with_tiny_file_overhead':width,'8m_40m_same_width_role_bytes':width*counts[name]}
 total=sum(x['8m_40m_same_width_role_bytes'] for x in roles.values())
 return {'format':'ashlar-native-mixed-storage-sensitivity/1','source':S,'source_sha256':hashlib.sha256((B/S).read_bytes()).hexdigest(),'roles':roles,'8m_40m_same_width_total_bytes':total,'proposed_cap_bytes':140000000000,'same_width_remaining_bytes':140000000000-total,'admission':'Not admitted: bootstrap only, tiny-file width uncertain, no native range generator, before/after change history, retained-version/failure/validation reserve or price qualification.','uncertainty_scenarios':[{'width_multiplier':m,'role_bytes':total*m,'remaining_140gb_bytes':140000000000-total*m} for m in [1,1.5,2]],'limits':['Same synthetic distribution/width arithmetic, not measured future compression or capacity.','192M bootstrap property events assumes4/entity; future changes and property fanout increase journal.','One raw bootstrap record per entity; staging/retries/retention/Delta logs/DVs not included.','Current/journal contain duplicated exact opaque values; each role independently consumes bytes.','No automatic growth because arithmetic fits a ceiling. Full all-row native generator and cost/deadline controllers still required.']}
if __name__=='__main__':
 r=calculate();(B/'out/native-mixed-storage-r221.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r,indent=2))
