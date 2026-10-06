"""Read-only convenience payload sample; no representative scale inference."""
import collections,gzip,hashlib,json,math,time
from pathlib import Path
from decimal import Decimal
from persistent_sql import Client
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_payload_sample_20261006_r80';c=Client(O);start=time.time()
prior=json.loads((B/'out/native/ashlar_scale_baseline_inventory_20261006_r77/summary.json').read_text());assert prior['state']=='passed'
table=prior['table'];version=prior['version']
rows=c.sql('bounded-pinned-payload-sample',f'SELECT source_system,rel_type_id,props_json,retained_json FROM {table} VERSION AS OF {version} LIMIT 256')
assert len(rows)==256
metrics={}
for index,name in [(2,'props_json'),(3,'retained_json')]:
 values=[r[index] for r in rows];assert all(isinstance(v,str) for v in values)
 # Byte lengths and compression use delivered UTF-8 text, never parsed rendering.
 encoded=[v.encode() for v in values];payload=b'\n'.join(encoded)
 frequency=collections.Counter(payload);total=len(payload)
 entropy=-sum((n/total)*math.log2(n/total) for n in frequency.values()) if total else 0
 keysets=collections.Counter();keycounts=collections.Counter()
 for value in values:
  parsed=json.loads(value,parse_float=Decimal);assert isinstance(parsed,dict)
  keysets[tuple(sorted(parsed))]+=1;keycounts.update(parsed.keys())
 sizes=[len(x) for x in encoded];compressed=gzip.compress(payload,mtime=0)
 metrics[name]={'distinct_exact_texts':len(set(values)),'min_utf8_bytes':min(sizes),'max_utf8_bytes':max(sizes),'mean_utf8_bytes':sum(sizes)/len(sizes),'sample_joined_utf8_bytes':total,'sample_gzip_bytes':len(compressed),'gzip_fraction':len(compressed)/total if total else None,'byte_frequency_entropy_bits':entropy,'distinct_top_level_keysets':len(keysets),'top_level_key_occurrences':dict(keycounts),'joined_sample_sha256':hashlib.sha256(payload).hexdigest()}
summary={'state':'observed','table':table,'version':version,'rows':len(rows),'sources':dict(collections.Counter(r[0] for r in rows)),'relationship_types':dict(collections.Counter(r[1] for r in rows)),'payload_metrics':metrics,'elapsed_seconds':time.time()-start,'sampling':'Pinned SELECT LIMIT 256 with no ORDER BY; convenience sample, not random/stratified or full-fixture entropy audit. LIMIT bounds result rows, not an independently proven physical scan budget.','compression_scope':'Local gzip of joined exact UTF-8 bags; not Parquet/Zstd or native stored bytes. Byte entropy is a marginal frequency statistic, not information entropy of independently generated records.','cost':'One read on existing authorized warehouse; no writes or new compute; billing dollars and physical scan cost unavailable.','limits':['historical experimental carrier profile; not full 0.3 cursor/raw/journal shape','sample distribution cannot represent whole fixture or real sources','no compression/file-count/billion-scale extrapolation','no singleton/ingest/performance admission']}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
