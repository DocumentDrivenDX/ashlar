"""Independent accepted100k overlay field expectations from synthetic changes."""
import copy,hashlib,json,time
from pathlib import Path
from third_changes_r378 import ThirdChanges
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent

def main():
 p=B/'out/overlay100k-oracle-r400.json';assert not p.exists();start=time.monotonic();c=ThirdChanges();hs=[];live=[];deleted=[]
 for i in range(100000):
  x=c.change(i);flag=x['after'] is None;row=copy.deepcopy(x['before'] if flag else x['after'])
  if flag:
   t=x['tombstone'];row.update(source_system=t['source_system'],rel_type_id=t['type_id'],id=t['id'],entity_version=t['entity_version'],source_feed=t['source_feed'],source_epoch=t['source_epoch'],source_position=t['source_position'],source_cursor_json=t['source_cursor_json'],source_delivery_id=t['source_delivery_id'],apply_batch_id=t['apply_batch_id'])
  marker={**row,'is_deleted':flag};hs.append(row_hash(marker,FIELDS+('is_deleted',)));(deleted if flag else live).append(row_hash(row,FIELDS))
 digest=lambda h:hashlib.sha256(''.join(sorted(h)).encode()).hexdigest();cdf=json.loads((B/'out/third-cdf-image-oracle-r381.json').read_text());assert len(live)==90000 and len(deleted)==10000 and digest(live)==cdf['roles']['edge_current']['images']['update_postimage']['digest']
 a={'state':'Independent100k complete overlay markers and90k live carriers qualified','fields':list(FIELDS)+['is_deleted'],'rows':100000,'digest':digest(hs),'live':{'rows':90000,'fields':list(FIELDS),'digest':digest(live)},'deleted':{'rows':10000,'fields':list(FIELDS),'digest':digest(deleted)},'wall_s':time.monotonic()-start,'source_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['overlay_oracle_r400.py','third_changes_r378.py','overlay_sql_r395.py','mixed_change_queries_r230.py','out/third-cdf-image-oracle-r381.json']},'qualification':'Deletion marker origin/version comes from exact qualified tombstone; other inactive fields are diagnostic predecessor data, never live current. SHA256 collision assumption. No native/write/read performance evidence.'};p.write_text(json.dumps(a,indent=2)+'\n');print(a['state'],a['wall_s'])
if __name__=='__main__':main()
