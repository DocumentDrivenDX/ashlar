"""Native no-table adversarial controls for exact-value and join-coverage checks."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,TEXTS,lit
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_parity_controls_r168'
assert not (O/'statements.jsonl').exists(),'Inspect existing control IDs'
base={x:'CAST(1 AS BIGINT)' for x in COLS};base.update({x:lit('v') for x in TEXTS})
base.update(id='CAST(42 AS BIGINT)',props_json=lit('{"105":"e\u0301"}'),retained_json=lit('{"unknown":9007199254740993}'),source_system=lit('s\u00e9'),source_position='CAST(NULL AS BIGINT)',published_at="TIMESTAMP '2026-10-07 00:00:00.123456'",lookup_hash=lit('0'*64),source_cursor_json=lit('{"xid":"9007199254740993","seq":"42"}'))
variants=[('same',base.copy(),0)]
for x in COLS:
 d=base.copy();d[x]='CAST(NULL AS STRING)' if x in TEXTS else ('CAST(0 AS BIGINT)' if x=='source_position' else ("TIMESTAMP '2026-10-07 00:00:00.123457'" if x=='published_at' else 'CAST(2 AS BIGINT)'));variants.append(('mutate-'+x,d,1))
for name,x,value in [('unicode-nfc','props_json','{"105":"\u00e9"}'),('identity-nfd','source_system','se\u0301'),('json-whitespace','props_json','{ "105":"e\u0301"}'),('retained-rounded','retained_json','{"unknown":9007199254740992}'),('cursor-rounded','source_cursor_json','{"xid":"9007199254740992","seq":"42"}')]:
 d=base.copy();d[x]=lit(value);variants.append((name,d,1))
select=lambda d:','.join(d[x]+' AS '+x for x in COLS)
actual=' UNION ALL '.join('SELECT '+lit(name)+' AS test,'+select(d) for name,d,_ in variants)
comparisons=[f"NOT(encode(e.{x},'UTF-8') <=> encode(a.{x},'UTF-8'))" if x in TEXTS else f'NOT(e.{x} <=> a.{x})' for x in COLS]
query='WITH e AS (SELECT '+select(base)+'), a AS ('+actual+') SELECT a.test,count_if('+ ' OR '.join(comparisons)+') FROM e CROSS JOIN a GROUP BY a.test ORDER BY a.test'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=15')
expected=[[name,str(n)] for name,_,n in sorted(variants)];assert c.sql('exact-controls',query)==expected
assert c.sql('wrong-set-count',"WITH e AS (SELECT * FROM VALUES (42),(43) AS v(id)), a AS (SELECT * FROM VALUES (42),(44) AS v(id)) SELECT (SELECT count(*) FROM a),(SELECT count(DISTINCT id) FROM a),(SELECT count(*) FROM e JOIN a ON e.id=a.id)")==[['2','2','1']]
assert c.sql('duplicate-count',"WITH e AS (SELECT * FROM VALUES (42),(43) AS v(id)), a AS (SELECT * FROM VALUES (42),(42) AS v(id)) SELECT (SELECT count(*) FROM a),(SELECT count(DISTINCT id) FROM a),(SELECT count(*) FROM e JOIN a ON e.id=a.id)")==[['2','1','2']]
c.history();c.close();(O/'summary.json').write_text(json.dumps({'state':'26 exact-value controls and2 join-membership/uniqueness counterexamples passed','expected':expected,'qualification':'No tables or mutations. Every20 logical column has a detected change; byte-different NFC/NFD, whitespace, rounded retained/cursor integers and timestamp1us differ, null source_position baseline matches. Equal table counts alone do not establish membership; joined expected coverage plus nonnull global uniqueness is required. Full20M parity relies on those actual snapshot checks, not these constants.'},indent=2)+'\n');print('Exact controls passed')
