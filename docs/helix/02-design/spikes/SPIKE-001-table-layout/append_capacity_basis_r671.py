"""Measured staged growth footprint; no combined publication or scale admission."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent
sources=['out/native/ashlar_append_node_growth_r651/audited-summary-r652.json','out/native/ashlar_append_edge_growth_r665/audited-summary-r669.json','out/full-role-capacity-r634.json']
n,e,old=[json.loads((B/p).read_text()) for p in sources]
assert n['profiles']['object_current']['rows']==8000000 and e['profiles']['edge_current']['rows']==8000000
profiles={**{'node_'+k:v for k,v in n['profiles'].items()},**{'edge_'+k:v for k,v in e['profiles'].items()}}
node_bytes=sum(p['bytes'] for p in n['profiles'].values());edge_bytes=sum(p['bytes'] for p in e['profiles'].values())
result={'format':'ashlar-staged-capacity-basis/1','sources':{p:hashlib.sha256((B/p).read_bytes()).hexdigest() for p in sources},'profiles':profiles,
 'measured_new_node_active_bytes':node_bytes,'measured_first8M_edge_active_bytes':edge_bytes,'measured_staged_active_bytes':node_bytes+edge_bytes,'measured_staged_active_files':sum(p['files'] for p in profiles.values()),
 'conditional_remaining32M_edge_active_bytes':4*edge_bytes,'conditional_full_new40M_edge_active_bytes':5*edge_bytes,
 'next8M_native_cost_sensitivity':{'write_bytes':e['costs']['write_remote_bytes']*8/5.5*1.5,'read_bytes':e['costs']['read_bytes']*2*1.5+10000000000},
 'next_action':'Prepare bounded verification blocks before native16M-new-edge admission: current32M-journal full digest may exceed180s if simply doubled. Revalidate private physical heads; preserve selected pins. Then independent local next8M oracle and chunked appends.',
 'qualification':'Measured separate private7-role active extents, not a unified publication or new production storage total. Conditional32M remainder assumes same synthetic entropy/history ratio and compression; excludes retained versions/logs/orphans/staged inputs/control/export/maintenance peaks. Native write sensitivity uses actual5.5M appends, read sensitivity covers prior8M validation at double extent with1.5x headroom plus10GB reserve. It is not native admission, hard billing cap, concurrency or sustained rate evidence. Interrupted full controller wall time is unqualified; no wall-rate extrapolation. Old R634 six-role footprint remains separately scoped to its recorded heads.'}
(B/'out/append-capacity-basis-r671.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ['profiles','sources']},indent=2))
