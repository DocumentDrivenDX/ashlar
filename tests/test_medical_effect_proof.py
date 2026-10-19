# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual driver/journal/ordinal inspector; native and ordinary SQL ports inert."""
from contextlib import ExitStack, nullcontext
import json
import unittest
from unittest.mock import patch
from ashlar_host.delta_custody import LocalDeltaEffects, LocalDeltaError, original_operation_intent, encoded, sha, LOCAL_OPERATION_CAPACITY_8M
from ashlar_host.driver import PrivatePolicy, _medical_json_budget
from ashlar.publication import Descriptor
import test_medical_publication_observation as observation

class Tests(unittest.TestCase):
    def fixture(self, stack):
        fixture = observation.Tests(); self.addCleanup(fixture.doCleanups)
        f, driver, plan, bounds, descriptor, value = fixture.fixture(stack)
        transport = driver.transport; policy = driver.policy; transport.policy = policy
        policy.context = driver.context; policy.initializing = False
        policy.writer = lambda operation, context: nullcontext()
        policy.admit = lambda intent, context: PrivatePolicy.admit(policy, intent, context)
        transport.installation_id = f.selection.installation_id; transport.operation_capacity = LOCAL_OPERATION_CAPACITY_8M
        transport._profile = lambda: self.assertIs(transport.observation_bounds, bounds)
        native = {}; snapshots = {}; prior = {}
        for role in value['expected']:
            target = transport.targets[value['tables'][role]]
            def snapshot(rows):
                rows = sorted(json.loads(encoded(rows)), key=encoded)
                return {'schema': [[name, family.lower()] for name, family in value['columns'][role]],
                    'rows': rows, 'row_sha256': sha(encoded(rows))}
            snapshots[(target.table, 1)] = snapshot(value['previous_expected'][role])
            snapshots[(target.table, 2)] = snapshot(value['expected'][role])
            prior[target.table] = {'uuid': target.uuid, 'version': 1, 'snapshot': snapshots[(target.table, 1)]}
        def capture(target, version, *, remaining_bytes):
            self.assertIs(transport.observation_bounds, bounds)
            return json.loads(encoded(snapshots[(target.table, version)])), json.loads(value['schemas'][target.table.split('.')[-1]])
        transport.bounded_snapshot = capture
        transport._snapshot = lambda target, version: capture(target, version, remaining_bytes=bounds.total_bytes)[0]
        transport.original_history = lambda target: native.get(target.table, [{'version':1}, {'version':0}])
        digest = value['request']['request_digest']; operation = 'effects:' + digest
        effects = LocalDeltaEffects(transport)
        original, plan_sha, ids = effects.original_plan(operation, digest, value['selected_steps'])
        transport.db.execute('INSERT INTO local_plan VALUES(?,?,?)', (operation, original, plan_sha))
        compact = []
        for key, step in zip(ids, value['selected_steps']):
            target = transport._target(step['statement'])
            intent = original_operation_intent(transport.installation_id, key, digest, step['statement'], step['parameters'], target, transport.operation_capacity)
            metadata = encoded({'profile':'ashlar-local-delta-commit/0.1','installation_id':transport.installation_id,
                'operation':key,'operation_sha256':sha(intent),'request_digest':digest})
            native[target.table] = [{'version':2,'userMetadata':metadata,'operation':'MERGE'}, {'version':1,'userMetadata':None,'operation':'CREATE TABLE'}]
            snap = snapshots[(target.table,2)]
            receipt = {'profile':'ashlar-local-delta-receipt/0.1','operation':key,'operation_sha256':sha(intent),
                'request_digest':digest,'table':target.table,'uuid':target.uuid,'before_version':1,'version':2,
                'commit_metadata':metadata,'native_operation':'MERGE','snapshot':snap}
            transport.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)', (key,intent,sha(intent),1,'committed',encoded(receipt)))
            compact.append([{'version':'2','uuid':target.uuid,'operation_sha256':sha(intent),'snapshot_sha256':snap['row_sha256']}])
        source = {'request':value['request'],'generated_steps':value['generated_steps'],'selected_steps':value['selected_steps'],
            'zero_match_elisions':value['zero_match_elisions'],'complete_prior_oracle':value['previous_expected'],'observed_native_prior':prior}
        original_source = encoded(source)
        transport.db.execute('INSERT INTO local_source_plan VALUES(?,?)',(digest,original_source));transport.db.commit()
        manifest = dict(descriptor.raw);manifest['table_versions_json']=encoded({value['tables'][r]:2 if value['tables'][r] in native else 1 for r in value['expected']})
        policy.active['manifest']=manifest
        proof = {'profile':'ashlar-original-medical-effect-proof/0.1','plan_sha256':plan_sha,'intent_digest':digest,
            'original_native_receipts':compact,'source_plan_reference':driver._medical_source_plan_reference(value['request'],original_source),
            'zero_match_elisions':value['zero_match_elisions'],'original_source_plan_sha256':sha(original_source)}
        return f,driver,bounds,value,{'effects':proof,'manifest':manifest},ids,source,native

    def resolve(self, driver, bounds, value, artifact):
        with driver.writer('original',driver.context),driver.transport.bounded_observations(bounds):
            return driver._validate_medical_effect_proof(value['request'],artifact,driver.context)

    def test_json_preflight_exact_unicode_escaping_and_no_oversize_encoder(self):
        value={'control':'\n\t\x00"\\','unicode':'é😀','null':None,'boolean':True,'integer':-7,'list':['null',None]}
        for ascii_only in (False,True):
            text=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=ascii_only)
            count=len(text.encode())
            self.assertEqual(_medical_json_budget(value,count,ascii_only=ascii_only),count)
            with self.assertRaises(PermissionError):_medical_json_budget(value,count-1,ascii_only=ascii_only)
        from types import SimpleNamespace
        from ashlar_host.driver import NativeDriver
        binding={'bounds':SimpleNamespace(cell_bytes=16)}
        with patch('ashlar_host.driver.encoded',side_effect=AssertionError('Encoder must not run')):
            with self.assertRaises(PermissionError):
                NativeDriver._require_medical_artifact_budget(object(),binding,{}, {'effects':{'large':'x'*100},'manifest':{}})

    def test_bounded_source_blob_query_never_transfers_oversize_original(self):
        with ExitStack() as stack:
            _,d,b,v,a,ids,source,native=self.fixture(stack)
            real=d.transport.db;queries=[]
            class Database:
                def execute(self,sql,parameters=()):
                    if 'FROM local_source_plan' in sql:
                        queries.append((sql,parameters))
                        self_test.assertIn('substr(CAST(original AS BLOB),1,?)',sql)
                        self_test.assertEqual(parameters[0],8*1024*1024+1)
                    return real.execute(sql,parameters)
            self_test=self
            real.execute("UPDATE local_source_plan SET original=CAST(zeroblob(?) AS TEXT)",(8*1024*1024+100,));real.commit()
            d.transport.db=Database()
            with self.assertRaisesRegex(PermissionError,'source plan missing'):self.resolve(d,b,v,a)
            self.assertEqual(len(queries),2)
            d.transport.db=real

    def test_complete_actual_journal_inspector_and_compact_phase_budget(self):
        with ExitStack() as stack:
            f,d,b,v,a,ids,source,native=self.fixture(stack)
            before=list(d.transport.db.execute('SELECT * FROM local_operation'))
            self.assertIsNone(self.resolve(d,b,v,a))
            self.assertEqual(before,list(d.transport.db.execute('SELECT * FROM local_operation')))
            self.assertNotIn('original_generated_steps',a['effects']);self.assertNotIn('observed_native_prior',a['effects'])
            self.assertEqual(json.loads(d.transport.db.execute('SELECT original FROM local_source_plan').fetchone()[0]),source)
            raw=encoded(a);phase=json.dumps({'request':v['request'],'result_json':raw,'descriptor_json':encoded(a['manifest'])},sort_keys=True,separators=(',',':'))
            self.assertLess(len(phase.encode()),b.cell_bytes)
            self.assertEqual(len(a['effects']['original_native_receipts']),3)
            self.assertTrue(all(c.closes==1 for c in f.connections))

    def test_public_apply_and_validate_release_only_resolved_compact_original(self):
        with ExitStack() as stack:
            _, d, b, v, a, ids, source, native = self.fixture(stack)
            returned = {k:a['effects'][k] for k in ('plan_sha256','intent_digest','original_native_receipts')}
            # Effect submission is inert; real retained whole-plan and per-ordinal inspectors remain active.
            with patch.object(LocalDeltaEffects, 'run', return_value=returned), d.writer('original',d.context), d.transport.bounded_observations(b):
                raw=d.apply(v['request'],d.context);actual=json.loads(raw)
                self.assertEqual(actual['effects']['profile'],'ashlar-original-medical-effect-proof/0.1')
                row=actual['manifest']
                descriptor=Descriptor(row['publication_id'],row['profile_version'],json.loads(row['table_versions_json']),
                    json.loads(row['schema_revisions_json']),json.loads(row['source_progress_json']),json.loads(row['validation_report_json']),row)
                self.assertIsNone(d.validate(v['request'],raw,descriptor,d.context))
                self.assertEqual(d.apply(v['request'],d.context),raw)
            self.assertEqual(d.transport.db.execute('SELECT COUNT(*) FROM local_source_plan').fetchone(),(1,))
            self.assertEqual(d.transport.db.execute('SELECT COUNT(*) FROM local_operation').fetchone(),(3,))
            self.assertEqual(d.transport.db.execute('SELECT request,artifact FROM local_publication_artifact').fetchone(),(encoded(v['request']),raw))

    def test_missing_source_reference_profile_or_cell_budget_never_releases(self):
        with ExitStack() as stack:
            _,d,b,v,a,ids,source,native=self.fixture(stack)
            enlarged=json.loads(encoded(a));enlarged['manifest']['validation_report_json']='x'*1048576
            with self.assertRaisesRegex(PermissionError,'phase cell'):
                self.resolve(d,b,v,enlarged)
            self.assertEqual(d.transport.db.execute('SELECT COUNT(*) FROM local_publication_artifact').fetchone(),(0,))
            missing=json.loads(encoded(a));del missing['effects']['source_plan_reference']
            with self.assertRaises(PermissionError):self.resolve(d,b,v,missing)
            d.transport.db.execute('DELETE FROM local_source_plan');d.transport.db.commit()
            with self.assertRaisesRegex(PermissionError,'source plan missing'):
                self.resolve(d,b,v,a)

    def test_closing_source_row_drift_after_first_inspection_withholds_result(self):
        with ExitStack() as stack:
            _,d,b,v,a,ids,source,native=self.fixture(stack);original=d.transport.inspect_committed;calls=[]
            def inspect(key,*args,**kwargs):
                result=original(key,*args,**kwargs);calls.append(key)
                if len(calls)==1:
                    d.transport.db.execute('UPDATE local_source_plan SET original=original || ?',(' ',));d.transport.db.commit()
                return result
            d.transport.inspect_committed=inspect
            with self.assertRaises(PermissionError):self.resolve(d,b,v,a)
            self.assertEqual(calls,ids+ids)

    def test_replaced_transport_policy_never_invokes_foreign_owner(self):
        from types import SimpleNamespace
        with ExitStack() as stack:
            f,d,b,v,a,ids,source,native=self.fixture(stack);foreign=[]
            d.transport.policy=SimpleNamespace(writer=lambda *args:foreign.append('writer'),admit=lambda *args:foreign.append('admit'))
            with self.assertRaisesRegex(PermissionError,'transport policy changed'):self.resolve(d,b,v,a)
            self.assertEqual(foreign,[])
            self.assertTrue(any(c.role=='medical_reader' for c in f.connections));self.assertTrue(any(c.role=='medical_ack' for c in f.connections))

    def test_reference_and_complete_source_plan_drift_refuse(self):
        with ExitStack() as stack:
            _,d,b,v,a,ids,source,native=self.fixture(stack)
            for key, replacement in [('bytes',True),('sha256','b'*64),('request_digest','b'*64),('profile','foreign')]:
                changed=json.loads(encoded(a));changed['effects']['source_plan_reference'][key]=replacement
                with self.subTest(key=key),self.assertRaises(PermissionError):self.resolve(d,b,v,changed)
            changed=json.loads(encoded(source));changed['generated_steps'][0]['statement']+=' '
            raw=encoded(changed);d.transport.db.execute('UPDATE local_source_plan SET original=?',(raw,));d.transport.db.commit()
            # A matching hash of substituted original data is still not semantic admission.
            a['effects']['source_plan_reference']=d._medical_source_plan_reference(v['request'],raw);a['effects']['original_source_plan_sha256']=sha(raw)
            with self.assertRaises(PermissionError):self.resolve(d,b,v,a)

    def test_missing_whole_plan_noncommitted_and_changed_native_receipt_refuse(self):
        with ExitStack() as stack:
            _,d,b,v,a,ids,source,native=self.fixture(stack)
            d.transport.db.execute("UPDATE local_operation SET state='submitted',receipt=NULL WHERE operation=?",(ids[0],));d.transport.db.commit()
            with self.assertRaises((PermissionError,LocalDeltaError)):self.resolve(d,b,v,a)
            d.transport.db.execute('DELETE FROM local_plan');d.transport.db.commit()
            with self.assertRaises((PermissionError,LocalDeltaError)):self.resolve(d,b,v,a)

    def test_first_ordinal_cancellation_still_inspects_other_originals_and_closes(self):
        with ExitStack() as stack:
            f,d,b,v,a,ids,source,native=self.fixture(stack);calls=[];original=d.transport.inspect_committed;primary=KeyboardInterrupt('owned')
            def inspect(key,*args,**kwargs):
                calls.append(key)
                if key==ids[0]:raise primary
                return original(key,*args,**kwargs)
            d.transport.inspect_committed=inspect
            with self.assertRaises(KeyboardInterrupt) as caught:self.resolve(d,b,v,a)
            self.assertIs(caught.exception,primary)
            self.assertEqual(calls,ids+ids)
            self.assertTrue(any(c.role=='medical_reader' for c in f.connections));self.assertTrue(any(c.role=='medical_ack' for c in f.connections))
            self.assertTrue(all(c.closes==1 for c in f.connections))

    def test_compact_receipt_and_native_snapshot_drift_refuse(self):
        with ExitStack() as stack:
            _,d,b,v,a,ids,source,native=self.fixture(stack)
            changed=json.loads(encoded(a));changed['effects']['original_native_receipts'][0][0]['snapshot_sha256']='b'*64
            with self.assertRaises(PermissionError):self.resolve(d,b,v,changed)
            original=d.transport._snapshot
            def changed_snapshot(target,version):
                result=original(target,version)
                if version==2:result['rows'].append(result['rows'][0])
                return result
            d.transport._snapshot=changed_snapshot
            with self.assertRaises((PermissionError,LocalDeltaError)):self.resolve(d,b,v,a)

if __name__=='__main__':unittest.main()
