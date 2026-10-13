"""Retained applied restoration controls; native/source observations are stubbed."""
import json,sqlite3,unittest
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.commerce_admission import CommerceAdmission
from ashlar_host.driver import NativeDriver
from ashlar_host.delta_custody import encoded,sha
from ashlar_host.original_commerce_reopen import restore_original_commerce_applied
from test_protected_outbox_ack import original

class Tests(unittest.TestCase):
    def test_original_custody_oracle_restoration_and_native_refusal_preserves_active(self):
        request,manifest=original();request['predecessor']='explicit-original-commerce-origin'
        request['request_digest']=sha(encoded({key:value for key,value in request.items()if key!='request_digest'}))
        roles=('object_current','edge_current','tombstone','whole_source_history');previous={role:[]for role in roles};expected={role:[]for role in roles}
        plan={'request':request,'generated_steps':[],'selected_steps':[],'zero_match_elisions':[],'complete_prior_oracle':previous,'observed_native_prior':{}}
        admission=object.__new__(CommerceAdmission);admission.originals=(('/model',b'original-model'),('/graph',b'original-graph'),('/bindings',b'{}'),('/receipt',b'original-public-receipt'));admission.batch=object()
        facts={'profile':'ashlar-original-commerce-public-dataset-admission/0.1'};admission.metadata=lambda:facts
        manifest['validation_report_json']=encoded({'complete':True,'request_digest':request['request_digest'],'source_oracle_sha256':sha(encoded(expected)),'original_source_plan_sha256':sha(encoded(plan)),'source_admission':facts})
        artifact={'manifest':manifest,'effects':{}};db=sqlite3.connect(':memory:')
        try:
            db.execute('CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)');db.execute('CREATE TABLE local_publication_artifact(request_digest TEXT,request TEXT,artifact TEXT)')
            db.execute('INSERT INTO local_source_plan VALUES(?,?)',(request['request_digest'],encoded(plan)))
            db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',(request['request_digest'],encoded(request),encoded(artifact)))
            driver=object.__new__(NativeDriver);driver.context=object();driver.columns={role:[]for role in roles};driver.source_admission=admission
            driver.require=lambda context:None;driver.policy=SimpleNamespace(active=None)
            driver.transport=SimpleNamespace(db=db,targets={table:SimpleNamespace(uuid='original-uuid')for table in json.loads(manifest['table_versions_json'])})
            observed=[]
            def native(selected,raw,context):
                self.assertEqual(selected,request);self.assertEqual(json.loads(raw),artifact);self.assertEqual(driver.policy.active['expected'],expected);observed.append('live-native')
            driver.observe_original_commerce_applied=native
            with patch('ashlar_host.original_commerce_reopen.original_commerce_oracle',return_value=expected):
                result=restore_original_commerce_applied(driver,request['request_digest'])
                self.assertEqual(result.request,request);self.assertEqual(observed,['live-native'])
                self.assertEqual(dict(result.descriptor.raw),manifest)
                retained=driver.policy.active
                driver.observe_original_commerce_applied=lambda *args:(_ for _ in ()).throw(PermissionError('current native refusal'))
                with self.assertRaisesRegex(PermissionError,'current native refusal'):restore_original_commerce_applied(driver,request['request_digest'])
                self.assertIs(driver.policy.active,retained)
                changed=json.loads(manifest['validation_report_json']);changed['source_oracle_sha256']='a'*64
                manifest['validation_report_json']=encoded(changed);db.execute('UPDATE local_publication_artifact SET artifact=?',(encoded({'manifest':manifest,'effects':{}}),))
                with self.assertRaisesRegex(ValueError,'source/prior oracle differs'):restore_original_commerce_applied(driver,request['request_digest'])
                self.assertIs(driver.policy.active,retained)
        finally:db.close()
