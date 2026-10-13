"""Small real SQLite applied-restoration control; native gates are inert stubs."""
import base64
import datetime
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch
import unittest
import test_truss_feed_sessions as sessions_cases
from ashlar.manifest import FIELDS
from ashlar.truss_native_feed import scalar_entity_changes
from ashlar.apply import empty_state,plan_apply
from ashlar_host.driver import NativeDriver,progress_union
from ashlar_host.delta_custody import encoded,sha,LocalDeltaEffects
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_note_publication import graph_expected_rows
from ashlar_host.truss_recovery import restore_applied_truss_publication


class Tests(unittest.TestCase):
    def test_original_applied_plan_restores_and_failure_preserves_active(self):
        sessions,request,context,*_=sessions_cases.Tests().setup_case()
        request['predecessor']='explicit-truss-genesis'
        request['request_digest']=sha(encoded({k:v for k,v in request.items() if k!='request_digest'}))
        batch=sessions.batch();parsed=scalar_entity_changes(batch.begin,sha256=sha(batch.begin.decode()),prior=base64.b64decode(batch.cursor_before),next=base64.b64decode(batch.cursor_after),feed=batch.feed);columns=fixture_columns(RESOURCE_ROOT)
        previous={role:[] for role in columns}
        after=plan_apply(empty_state(),parsed.changes,schema_policy=lambda change:None)
        stamp='2026-10-12T00:00:00+00:00'
        expected=graph_expected_rows(previous,after,parsed.changes,batch,columns,stamp,parsed.event_bytes,parsed.edge_order_keys)
        plan={'request':request,'generated_steps':[],'selected_steps':[],'zero_match_elisions':[],
            'complete_prior_oracle':previous,'observed_native_prior':{}}
        source={'profile':'fixture','qualification':'Structural only'}
        report={'complete':True,'request_digest':request['request_digest'],'predecessor':request['predecessor'],
            'original_source_plan_sha256':sha(encoded(plan)),'source_admission':source,
            'source_oracle_sha256':sha(encoded(expected)),'zero_match_elisions':[]}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3',
            'recorded_at':str(int(datetime.datetime.fromisoformat(stamp).timestamp())*1000000),
            'table_versions_json':'{"local.runtime.object_current":0,"local.runtime.edge_current":0,"local.runtime.tombstone":0,"local.runtime.whole_source_history":0}',
            'schema_revisions_json':request['schema_revisions_json'],
            'source_progress_json':encoded(progress_union({},json.loads(request['source_checkpoint_json']))),
            'validation_report_json':encoded(report)}
        self.assertEqual(set(manifest),set(FIELDS))
        db=sqlite3.connect(':memory:')
        try:
            db.execute('CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)')
            db.execute('CREATE TABLE local_publication_artifact(request_digest TEXT,request TEXT,artifact TEXT)')
            db.execute('CREATE TABLE local_plan(operation TEXT,original TEXT,digest TEXT)')
            db.execute('INSERT INTO local_source_plan VALUES(?,?)',(request['request_digest'],encoded(plan)))
            db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',(request['request_digest'],encoded(request),encoded({'manifest':manifest,'effects':{}})))
            db.execute('INSERT INTO local_plan VALUES(?,?,?)',('effects:'+request['request_digest'],'original-plan','digest'))
            driver=object.__new__(NativeDriver);driver.context=context;driver.held=True
            driver.transport=SimpleNamespace(db=db);driver.columns=columns;driver.policy=SimpleNamespace(active=None)
            admission=object.__new__(TrussGraphAdmission);admission.batch=batch;admission.parsed=parsed
            admission.changes=parsed.changes;admission.admit=lambda change:None;admission.admit_transition=lambda *args:None
            driver.source_admission=admission;driver.source_sessions=sessions;driver.admission_facts=lambda:source
            calls=[];driver.source_admit=lambda selected:calls.append('live-source')
            driver.validate_manifest=lambda *args:calls.append('native-validation')
            with patch.object(LocalDeltaEffects,'original_plan',return_value=('original-plan','digest',[])):
                restore_applied_truss_publication(driver,request['request_digest'])
                self.assertEqual(driver.policy.active['expected'],expected)
                self.assertEqual(calls,['live-source','native-validation'])
                original_active=driver.policy.active
                driver.validate_manifest=lambda *args:(_ for _ in ()).throw(ValueError('native refusal'))
                with self.assertRaisesRegex(ValueError,'native refusal'):
                    restore_applied_truss_publication(driver,request['request_digest'])
                self.assertIs(driver.policy.active,original_active)
        finally:db.close()
