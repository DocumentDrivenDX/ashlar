"""Independent graph oracle must retain source identity and parallel edges."""
import hashlib,json,unittest
from pathlib import Path
from ashlar.source import jsonl_batches,records_digest
from fixture_oracle import fixture_inventory,fixture_columns,fixture_batches
ROOT=Path(__file__).resolve().parents[1]
CLOCK='2026-10-09T00:00:00+00:00'
def source_batches(source):
    originals=list(jsonl_batches((ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes().splitlines(keepends=True),feed=source,epoch='one'))
    batches=[]
    for original in originals:
        records=[]
        for record in original.records:
            event=json.loads(record.raw);event['source_system']=source
            records.append((json.dumps(event,ensure_ascii=False,separators=(',',':'))+'\n').encode())
        commit=json.loads(original.commit);commit['records_sha256']=records_digest(records)
        batches.extend(jsonl_batches([original.begin,*records,(json.dumps(commit)+'\n').encode()],feed=source,epoch='one'))
    return batches
class Tests(unittest.TestCase):
    def check(self,batches):return fixture_inventory(batches,fixture_columns(ROOT),materialized_at=CLOCK)[0]
    def test_overlapping_ids_and_parallel_edges_remain_independent(self):
        a,b=source_batches('A'),source_batches('B');rows=self.check([a[0],b[0]])
        self.assertEqual(len(rows['object_current']),6);self.assertEqual(len(rows['edge_current']),4)
        self.assertEqual({(r['source_system'],r['rel_type_id'],r['id']) for r in rows['edge_current']},{('A','2','1'),('A','2','2'),('B','2','1'),('B','2','2')})
        for row in rows['edge_current']:self.assertEqual((row['source_type'],row['source_id'],row['target_type'],row['target_id']),('1','1','1','2'))
    def test_source_update_delete_cannot_remove_other_source(self):
        a,b=source_batches('A'),source_batches('B');rows=self.check([a[0],b[0],a[1]])
        self.assertEqual(len(rows['object_current']),5);self.assertEqual(len(rows['edge_current']),2)
        self.assertEqual({r['source_system'] for r in rows['edge_current']},{'B'})
        self.assertEqual({r['source_system'] for r in rows['tombstone']},{'A'})
        self.assertEqual(len(rows['whole_source_history']),14)
        current={(r['source_system'],r['id']):r for r in rows['object_current']}
        self.assertEqual(current['A','1']['props_json'],'{"23":"updated","24":null}')
        self.assertEqual(current['B','1']['props_json'],'{"23":"first"}')
    def test_existing_single_source_oracle_keeps_original_inventory(self):
        _,batches=fixture_batches(ROOT,'local');rows=self.check(batches)
        self.assertEqual((len(rows['object_current']),len(rows['edge_current']),len(rows['tombstone']),len(rows['whole_source_history'])),(1,0,1,4))

    def test_unicode_source_hash_retains_canonical_utf8_identity(self):
        rows=self.check([source_batches('雪')[0]])
        row=next(r for r in rows['object_current'] if r['id']=='1')
        original='{"source_system":"雪","type_id":1,"id":1}'.encode('utf-8')
        self.assertEqual(row['lookup_hash'],hashlib.sha256(original).hexdigest())
