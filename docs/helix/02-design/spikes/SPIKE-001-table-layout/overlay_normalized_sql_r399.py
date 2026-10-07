"""Private synthetic accepted input -> complete Delta overlay rows; no execution."""
from normalized_apply_sql_r276 import pin,predecessor
from overlay_sql_r395 import FIELDS

def overlay_input(inputs):
 # Caller must qualify exact immutable raw/current/tombstone origins and partition.
 r=inputs['source_record'];a=inputs['current_replacement'];t=inputs['tombstone']
 before=predecessor(r['table'],r['version']);current=pin(a['table'],a['version']);tomb=pin(t['table'],t['version'])
 deleted={'source_system':'t.source_system','rel_type_id':'t.type_id','id':'t.id','entity_version':'t.entity_version','source_feed':'t.source_feed','source_epoch':'t.source_epoch','source_position':'t.source_position','source_cursor_json':'t.source_cursor_json','source_delivery_id':'t.source_delivery_id','apply_batch_id':'t.apply_batch_id'}
 fields=','.join(f'CASE WHEN a.id IS NULL THEN {deleted.get(f,"b."+f)} ELSE a.{f} END AS {f}' for f in FIELDS)
 return f"SELECT {fields},a.id IS NULL AS is_deleted FROM ({before}) b LEFT JOIN {current} a ON b.source_feed=a.source_feed AND b.source_epoch=a.source_epoch AND b.change_delivery_id=a.source_delivery_id LEFT JOIN {tomb} t ON b.source_feed=t.source_feed AND b.source_epoch=t.source_epoch AND b.change_delivery_id=t.source_delivery_id"
