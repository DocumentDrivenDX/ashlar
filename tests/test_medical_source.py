import hashlib,json,sys,tempfile,unittest
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'src'),str(Path(__file__).resolve().parents[1]/'tools')]
from ashlar.medical_source import build_transaction,SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA
from ashlar.whole_entity import changes_from_batch
from run_medical_outbox_publication import MedicalAdmission,original_medical_oracle,finish_native
from fixture_oracle import fixture_columns
ROOT=Path(__file__).resolve().parents[1]
MODEL=(ROOT/'examples/domain-packs/medical/historical/archive/schemas/ontology.json').read_bytes()
GRAPH=(ROOT/'examples/domain-packs/medical/upstream/graph/fixture.json').read_bytes()
PUBLIC=(ROOT/'docs/helix/04-build/evidence/medical-historical-admission-20261009/public-receipt.json').read_bytes()
class MedicalSourceTests(unittest.TestCase):
    def setUp(self):self.batch,self.bindings=build_transaction(MODEL,GRAPH,PUBLIC,source_system='private-original-medical-fixture')
    def test_complete_typed_source_retains_boolean_tokens_and_null_with_original_unbounded_integer(self):
        self.assertEqual(len(self.batch.records),113);self.assertEqual(len(self.bindings['properties']),48);changes=changes_from_batch(self.batch);self.assertEqual(sum(c.state.key.kind=='object'for c in changes),51);self.assertEqual(sum(c.state.key.kind=='edge'for c in changes),62)
        properties={p['identity'][2]:p['property_id']for p in self.bindings['properties']};boolean=[];integers=[]
        for change in changes:
            retained=json.loads(change.state.retained_json);self.assertEqual(retained['publicReceiptSha256'],PUBLIC_SHA)
            if change.state.key.kind!='object':continue
            original=retained['original'];values=json.loads(change.state.props_json)
            for field,token in original['values'].items():
                if field in ('patients.active','practitioners.active'):boolean.append((token,values[properties[field]]))
                if field=='resource_references.reference_id':integers.append((token,values[properties[field]]))
        self.assertEqual(boolean,[('True',True),('True',True),('True',True),(None,None)]);self.assertEqual(integers,[(str(i),i)for i in range(1,18)])
        model=json.loads(MODEL);field=next(e for e in model['modules'][0]['elements']if e['id']=='resource_references.reference_id');self.assertEqual(field['scalarType'],'integer');self.assertNotIn('width',field.get('facets',{}))
    def test_exact_original_receipt_pin_and_boolean_custody_cannot_be_swapped(self):
        for alteration in ['boolean','validation','source','field','token','missing']:
            public=json.loads(PUBLIC);proof=public['booleanReceipts'][0]
            if alteration=='boolean':proof['value']['boolean']=False
            if alteration=='validation':proof['validation']['valid']=1
            if alteration=='source':proof['source']['id']='changed'
            if alteration=='field':proof['request']['field']['element']='practitioners.active'
            if alteration=='token':proof['request']['token']='true'
            if alteration=='missing':public['booleanReceipts'].pop()
            with self.subTest(alteration=alteration),self.assertRaises(PermissionError):build_transaction(MODEL,GRAPH,json.dumps(public).encode(),source_system=self.batch.feed)
        with self.assertRaises(ValueError):build_transaction(MODEL+b' ',GRAPH,PUBLIC,source_system=self.batch.feed)
    def test_independent_original_fullrow_oracle_matches_decoded_source_and_exposes_bad_binding(self):
        columns=fixture_columns(ROOT);oracle=original_medical_oracle(MODEL,GRAPH,self.bindings,self.batch,columns);self.assertEqual({k:len(v)for k,v in oracle.items()},{'object_current':51,'edge_current':62,'tombstone':0,'whole_source_history':113});changes=changes_from_batch(self.batch);bydelivery={c.delivery_id:c for c in changes}
        for role in ('object_current','edge_current'):
            for row in oracle[role]:
                source=bydelivery[row['source_delivery_id']].state;self.assertEqual(row['props_json'],source.props_json);self.assertEqual(row['retained_json'],source.retained_json)
        bad=json.loads(json.dumps(self.bindings));bad['entities'][0]['id']=bad['entities'][1]['id'];bad['entities'][0]['type_id']=bad['entities'][1]['type_id']
        with self.assertRaises(ValueError):original_medical_oracle(MODEL,GRAPH,bad,self.batch,columns)
    def test_admission_rechecks_whole_original_bytes_and_only_owns_original_changes(self):
        with tempfile.TemporaryDirectory()as directory:
            paths=[Path(directory)/name for name in ('model','graph','bindings','receipt')]
            for p,raw in zip(paths,(MODEL,GRAPH,json.dumps(self.bindings).encode(),PUBLIC)):p.write_bytes(raw)
            admission=MedicalAdmission(self.batch,self.bindings,*paths);admission.admit(changes_from_batch(self.batch)[0]);self.assertEqual(admission.metadata()['original_boolean_receipts'][0]['request']['token'],'True')
            paths[3].write_bytes(PUBLIC+b' ')
            with self.assertRaises(PermissionError):admission.metadata()
    def test_stop_failure_withholds_success_report(self):
        class Spark:
            def stop(self):raise RuntimeError('cleanup failure')
        with tempfile.TemporaryDirectory()as directory:
            with self.assertRaises(RuntimeError):finish_native(None,Spark(),directory,{'success':True})
            self.assertFalse((Path(directory)/'report.json').exists())
if __name__=='__main__':unittest.main()
