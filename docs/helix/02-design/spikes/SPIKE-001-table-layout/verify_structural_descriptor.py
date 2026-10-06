"""Read back structural semantics and correct an incomplete fixture descriptor explicitly."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';s=json.loads((B/'out/native/ashlar_structural_publish_20261005_r3/summary.json').read_text());assert s['state']=='completed';c=Client(B/'out/native/ashlar_structural_descriptor_20261005_r4')
jv=s['versions'][F+'.property_journal'];tv=s['versions'][F+'.tombstone_r3']
r=c.sql('lifecycle-presence-and-metadata',f"SELECT count(*),count_if(property_id IS NOT NULL OR entity_kind<>'edge' OR entity_version<>2 OR schema_revision<>'r1' OR source_epoch<>'e' OR source_position<>1 OR source_time_text<>'synthetic-structure:1' OR published_at IS NULL OR (operation='delete' AND (NOT old_present OR new_present OR old_json IS NULL OR new_json IS NOT NULL)) OR (operation='insert' AND (old_present OR NOT new_present OR old_json IS NOT NULL OR new_json IS NULL)) OR operation NOT IN ('delete','insert')) FROM {F}.property_journal VERSION AS OF {jv} WHERE source_feed='fixture-edge-structure'");assert [int(x) for x in r[0]]==[20021,0],r
r=c.sql('tombstone-exact',f"SELECT count(*),count_if(t.source_system IS DISTINCT FROM d.source_system OR t.type_id IS DISTINCT FROM d.rel_type_id OR t.id IS DISTINCT FROM d.id OR t.entity_kind IS DISTINCT FROM 'edge' OR t.entity_version IS DISTINCT FROM 2 OR t.source_feed IS DISTINCT FROM 'fixture-edge-structure' OR t.source_epoch IS DISTINCT FROM 'e' OR t.source_position IS DISTINCT FROM 1) FROM {F}.tombstone_r3 VERSION AS OF {tv} t FULL OUTER JOIN {F}.delete_r1 d ON t.source_system=d.source_system AND t.type_id=d.rel_type_id AND t.id=d.id");assert [int(x) for x in r[0]]==[20,0],r
r=c.sql('original-descriptor',f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.publication_manifest WHERE publication_id='r3-1'");assert len(r)==1 and json.loads(r[0][0])==s['versions'];original=json.loads(r[0][2]);revisions={'pilot:'+str(i):'r1' for i in range(5)}
# Append a corrected immutable descriptor; preserve the original evidence.
if original!=revisions:
 assert original=={},original
 c.sql('corrected-descriptor',f"INSERT INTO {F}.publication_manifest SELECT 'r3-2',profile_version,table_versions_json,source_progress_json,'{json.dumps(revisions,separators=(',',':'))}','{{\"structural_checks\":\"passed\",\"supersedes\":\"r3-1\"}}',current_timestamp() FROM {F}.publication_manifest WHERE publication_id='r3-1'")
 publication='r3-2'
else:publication='r3-1'
r=c.sql('accepted-descriptor-readback',f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.publication_manifest WHERE publication_id='{publication}'");assert len(r)==1 and json.loads(r[0][0])==s['versions'] and json.loads(r[0][2])==revisions
assert json.loads(r[0][1])=={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure']}
(c.out/'summary.json').write_text(json.dumps({'state':'passed','accepted_publication':publication,'original_schema_revisions':original,'versions':s['versions'],'scope':'synthetic lifecycle journal/tombstone and complete descriptor readback'},indent=2));print('Structural metadata and accepted descriptor verified')
