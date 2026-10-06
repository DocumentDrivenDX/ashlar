"""Negative integrity controls for the large run's one-generation parity check."""
from pathlib import Path
import json
from persistent_sql import Client
from scale_workload import select,parity_sql,NODES
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_entropy_parity_20261006';c=Client(O,cancel_after=30,observation_timeout=60)
expected=select('edge',64,entropy=True,truss_shape=True)
variants={'unchanged':(expected,0),'lexical-property-change':(f'SELECT * EXCEPT(props_json), CASE WHEN id={NODES+1} THEN concat(props_json,\' \') ELSE props_json END props_json FROM ({expected})',1),'missing-row':(f'SELECT * FROM ({expected}) WHERE id<>{NODES+1}',1)}
for label,(actual,wanted) in variants.items():
 r=c.sql(label,f'WITH actual AS ({actual}) '+parity_sql('actual','edge',64,entropy=True,truss_shape=True));assert r==[[str(wanted)]],(label,r)
(O/'summary.json').write_text(json.dumps({'state':'passed','checks':{'equal_rows':0,'one_byte_level_lexical_change':1,'one_missing_expected_row':1},'scope':'Checker syntax and exact-string/missing-row controls, not large-data admission'},indent=2)+'\n')
