"""Exact full-carrier reader lane for the proposed compute isolation experiment.
Create/run/close each lane on its owning thread. No compute provisioning here.
"""
import json, re, time
from pathlib import Path
from driver_sql import DriverClient
COLS=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position','published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
E='client_dev.ashlar_entropy_20261006_r86.edge_current'
M='client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89'
def expected_r103(base):
    root=Path(base)/'out/native/ashlar_maintained_contention_20261007_r103'
    summary=json.loads((root/'summary.json').read_text())
    assert summary['state'].startswith('completed parallel publication')
    batch=summary['batches'][0]
    records=[json.loads(l) for l in (root/'new-publication-reads/statements.jsonl').read_text().splitlines()]
    matches=[r for r in records if r['label']=='new-reader-expected']
    assert len(matches)==1 and matches[0]['response']['status']['state']=='SUCCEEDED'
    assert 'VERSION AS OF 0' in matches[0]['sql'] and batch['source_stage'] in matches[0]['sql']
    rows=matches[0]['response']['result']['data_array']
    assert len(rows)==30 and all(len(row)==20 for row in rows)
    assert len({(row[0],row[1],row[2]) for row in rows})==30
    return batch['versions'],rows
class ExactReader:
    def __init__(self,client,versions,rows):
        self.client=client;self.versions=versions;self.rows=rows
        self.version=versions[E]
        assert isinstance(self.version,int) and self.version>=0 and rows
        for row in rows:
            assert len(row)==20 and re.fullmatch('[0-9a-f]{64}',row[16])
            for index in (1,2):
                assert isinstance(row[index],str) and re.fullmatch('-?(0|[1-9][0-9]*)',row[index])
                assert -(2**63)<=int(row[index])<2**63 and str(int(row[index]))==row[index]
        self.query=f"SELECT {','.join(COLS)} FROM {E} VERSION AS OF {self.version} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
    def preflight(self):
        self.client.sql('reader-timeout','SET STATEMENT_TIMEOUT=180')
        assert self.client.sql('reader-cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
        manifest=self.client.sql('reader-manifest',f"SELECT table_versions_json FROM {M} WHERE publication_id='r103-b1'")
        assert manifest==[[json.dumps(self.versions,sort_keys=True,separators=(',',':'))]]
    def read(self,phase,index):
        row=self.rows[index%len(self.rows)]
        params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
        actual=self.client.sql(f'{phase}-{index}',self.query,parameters=params,tag=False)
        assert actual==[row], 'Exact pinned reader carrier mismatch'
    def bounded_load(self,stop,max_reads=250,max_seconds=180):
        assert 1<=max_reads<=1000 and 0<max_seconds<=1200
        deadline=time.monotonic()+max_seconds
        count=0
        try:
            while count<max_reads and not stop.is_set() and time.monotonic()<deadline:
                self.read('load',count);count+=1
        except BaseException:
            stop.set();raise
        return count
