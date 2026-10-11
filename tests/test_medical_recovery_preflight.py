"""Actual journal/driver/source preflight; native snapshots are explicit inert ports."""
from contextlib import ExitStack
import json
import unittest
from unittest.mock import patch
from ashlar_host.delta_custody import encoded,sha
from ashlar_host.local_physical_carriers import original_local_physical_carriers
import test_medical_effect_proof as proofs

class Tests(unittest.TestCase):
    def fixture(self,stack,partial=True):
        helper=proofs.Tests();self.addCleanup(helper.doCleanups)
        f,d,b,v,a,ids,source,native=helper.fixture(stack)
        binding=d._original_medical_publication_binding;plan=binding['plan'];transport=d.transport
        if partial:
            transport.db.execute('DELETE FROM local_operation WHERE operation=?',(ids[-1],));transport.db.commit()
            table=transport._target(v['selected_steps'][-1]['statement']).table;native.pop(table)
        prior=transport.bounded_snapshot;captures=[]
        def capture(target,version,*,remaining_bytes):
            role=target.table.split('.')[-1];captures.append((role,version,remaining_bytes))
            if role in ('attempts','manifest'):
                schema=json.loads(original_local_physical_carriers()['carriers'][role]['schemaJson']);rows=[]
                return {'schema':[[x['name'],x['type']]for x in schema['fields']],'rows':rows,'row_sha256':sha(encoded(rows))},schema
            return prior(target,version,remaining_bytes=remaining_bytes)
        transport.bounded_snapshot=capture
        stack.enter_context(patch.object(transport,'mutation',side_effect=AssertionError('Preflight never mutates')))
        stack.enter_context(patch.object(transport,'recover',side_effect=AssertionError('Preflight never reconciles uncertain effect')))
        return f,d,plan,b,v,ids,captures
    def call(self,d,plan,b):
        with d.writer('original',d.context),d.transport.bounded_observations(b):
            return d.preflight_original_medical_recovery(plan,context=d.context,bounds=b)
    def test_partial_actual_intents_and_all_committed_receipts_pass_without_writes(self):
        with ExitStack()as stack:
            f,d,plan,b,v,ids,captures=self.fixture(stack)
            before=list(d.transport.db.iterdump())
            self.assertIsNone(self.call(d,plan,b))
            self.assertEqual(before,list(d.transport.db.iterdump()))
            self.assertEqual({x[0]for x in captures},{'object_current','edge_current','tombstone','whole_source_history','attempts','manifest'})
            self.assertTrue(all(x[2]>0 for x in captures));self.assertTrue(all(c.closes==1 for c in f.connections))
    def test_missing_plan_or_later_committed_receipt_refuses_before_any_action(self):
        for failure in ('missing-plan','receipt-drift','source-drift'):
            with self.subTest(failure=failure),ExitStack()as stack:
                f,d,plan,b,v,ids,captures=self.fixture(stack,partial=False)
                if failure=='missing-plan':d.transport.db.execute('DELETE FROM local_plan')
                elif failure=='receipt-drift':d.transport.db.execute("UPDATE local_operation SET receipt='{}' WHERE operation=?",(ids[-1],))
                else:
                    source=json.loads(d.transport.db.execute('SELECT original FROM local_source_plan').fetchone()[0]);source['request']['predecessor']='changed';d.transport.db.execute('UPDATE local_source_plan SET original=?',(encoded(source),))
                d.transport.db.commit();before=list(d.transport.db.iterdump())
                with self.assertRaises(Exception):self.call(d,plan,b)
                self.assertEqual(before,list(d.transport.db.iterdump()))
                self.assertTrue({'object_current','edge_current','tombstone','whole_source_history','attempts','manifest'}<={x[0]for x in captures})
    def test_cancellation_in_committed_inspector_preserves_primary_and_all_native_source_closing(self):
        with ExitStack()as stack:
            f,d,plan,b,v,ids,captures=self.fixture(stack);primary=KeyboardInterrupt();calls=[];actual=d.transport.inspect_committed
            def inspect(*args,**kwargs):
                calls.append(args[0])
                if len(calls)==1:raise primary
                return actual(*args,**kwargs)
            stack.enter_context(patch.object(d.transport,'inspect_committed',side_effect=inspect))
            with self.assertRaises(KeyboardInterrupt)as caught:self.call(d,plan,b)
            self.assertIs(caught.exception,primary);self.assertGreaterEqual(len(calls),4)
            self.assertTrue(all(c.closes==1 for c in f.connections));self.assertTrue(all(sum(x[0]==r for x in captures)>=2 for r in ('attempts','manifest')))
    def test_complete_anchor_shape_and_strict_saved_carriers_refuse(self):
        with ExitStack()as stack:
            f,d,plan,b,v,ids,captures=self.fixture(stack)
            original=d.transport.db.execute('SELECT original FROM local_source_plan').fetchone()[0]
            for kind in ('empty','boolean-version','nonfinite','duplicate'):
                body=json.loads(original)
                if kind=='empty':body['observed_native_prior']={}
                elif kind=='boolean-version':next(iter(body['observed_native_prior'].values()))['version']=True
                elif kind=='nonfinite':body['zero_match_elisions']=float('nan')
                text=encoded(body)if kind!='nonfinite'else json.dumps(body)
                if kind=='duplicate':text=text.replace('"request":','"request":{},"request":',1)
                d.transport.db.execute('UPDATE local_source_plan SET original=?',(text,));d.transport.db.commit()
                with self.subTest(kind=kind),self.assertRaises(Exception):self.call(d,plan,b)
            d.transport.db.execute('UPDATE local_source_plan SET original=?',(original,));d.transport.db.commit()
    def test_committed_prefix_gap_and_closing_state_drift_refuse(self):
        with ExitStack()as stack:
            f,d,plan,b,v,ids,captures=self.fixture(stack,partial=False)
            row=d.transport.db.execute('SELECT * FROM local_operation WHERE operation=?',(ids[0],)).fetchone()
            d.transport.db.execute('DELETE FROM local_operation WHERE operation=?',(ids[0],));d.transport.db.commit()
            with self.assertRaisesRegex(PermissionError,'prefix has a gap'):self.call(d,plan,b)
            d.transport.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',row);d.transport.db.commit()
            actual=d.transport.inspect_committed;changed=[False]
            def drift(*args,**kwargs):
                result=actual(*args,**kwargs)
                if not changed[0]:
                    changed[0]=True;d.transport.db.execute("UPDATE local_operation SET state='submitted',receipt=NULL WHERE operation=?",(ids[-1],));d.transport.db.commit()
                return result
            stack.enter_context(patch.object(d.transport,'inspect_committed',side_effect=drift))
            with self.assertRaises(Exception):self.call(d,plan,b)
            self.assertTrue(changed[0])

    def test_missing_ordinal_with_matching_native_commit_never_recreates_custody(self):
        with ExitStack()as stack:
            f,d,plan,b,v,ids,captures=self.fixture(stack,partial=False)
            d.transport.db.execute('DELETE FROM local_operation WHERE operation=?',(ids[-1],));d.transport.db.commit()
            with self.assertRaisesRegex(PermissionError,'missing ordinal custody'):self.call(d,plan,b)
            self.assertIsNone(d.transport.db.execute('SELECT 1 FROM local_operation WHERE operation=?',(ids[-1],)).fetchone())

    def test_wrong_context_or_equal_replaced_bounds_refuses_before_original_ports(self):
        from dataclasses import replace
        with ExitStack()as stack:
            f,d,plan,b,v,ids,captures=self.fixture(stack);before=len(f.connections)
            with d.writer('original',d.context),d.transport.bounded_observations(b):
                with self.assertRaises(PermissionError):d.preflight_original_medical_recovery(plan,context=object(),bounds=b)
                with self.assertRaises(PermissionError):d.preflight_original_medical_recovery(plan,context=d.context,bounds=replace(b))
            self.assertEqual(len(f.connections),before);self.assertEqual(captures,[])
