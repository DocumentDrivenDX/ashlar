import json,unittest
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.native import SQLResult
from ashlar.manifest import DeltaManifestStore
from ashlar.publication import Descriptor
from ashlar.retention import RetentionError,publication_retention_report
from ashlar.retention_policy import RetentionGate,RetentionNativePolicy,RetentionManifestPolicy,SQLRetentionProvider
from test_manifest import Executor as ManifestExecutor,Policy as ManifestPolicy,ROW as MANIFEST
import test_singleton as singleton_tests

DAY=86400000000;T='c.s.t';MARGIN=1000000
def report(table=T,version=0,uuid='original'):
    return publication_retention_report({table:{'uuid':uuid,'version':version,'committed_at':str(DAY)}},
        {table:{'data_retention':'7 days','log_retention':'30 days'}},margin_us=MARGIN)
class Provider:
    def __init__(self,table=T,uuid='original',clocks=None):self.table=table;self.uuid=uuid;self.clocks=iter(clocks or [2*DAY,2*DAY]);self.calls=0
    def observe(self,descriptor,context):
        self.calls+=1
        return {'configurations':{self.table:{'data_retention':'7 days','log_retention':'30 days'}},'table_uuids':{self.table:self.uuid},'now_us':str(next(self.clocks))}
def row():return dict(MANIFEST,validation_report_json=json.dumps({'complete':True,'retention':report()}))

class RetentionPolicyTests(unittest.TestCase):
    def test_manifest_rechecks_after_native_commit_and_refusal_never_claims_rollback(self):
        provider=Provider(clocks=[2*DAY,8*DAY]);ex=ManifestExecutor()
        policy=RetentionManifestPolicy(ManifestPolicy(),RetentionGate(provider,minimum_margin_us=MARGIN))
        with self.assertRaises(RetentionError):DeltaManifestStore(ex,policy,'c.s.manifest','uuid').commit(row(),context='admitted')
        self.assertEqual(ex.row,row());self.assertEqual(provider.calls,2)
        self.assertEqual(sum(sql.startswith('MERGE') for sql in ex.calls),1)

    def test_publication_custody_refuses_before_retention_or_effects(self):
        provider=Provider();ex=ManifestExecutor()
        policy=RetentionManifestPolicy(ManifestPolicy(),RetentionGate(provider,minimum_margin_us=MARGIN))
        with self.assertRaises(PermissionError):DeltaManifestStore(ex,policy,'c.s.manifest','uuid').commit(row(),context='denied')
        self.assertEqual(provider.calls,0);self.assertIsNone(ex.row)

    def test_successful_append_and_replay_require_fresh_admission(self):
        provider=Provider(clocks=[2*DAY]*4);ex=ManifestExecutor()
        store=DeltaManifestStore(ex,RetentionManifestPolicy(ManifestPolicy(),RetentionGate(provider,minimum_margin_us=MARGIN)),'c.s.manifest','uuid')
        self.assertEqual(store.commit(row(),context='admitted'),row())
        self.assertEqual(store.commit(row(),context='admitted'),row())
        self.assertEqual(provider.calls,4)

    def test_singleton_discards_fetched_row_when_actual_retention_gate_expires(self):
        from test_publication import TABLE
        p,e,b,policy=singleton_tests.SingletonTests.setup(self)
        b.rows[0]['table_versions_json']=json.dumps({TABLE:6})
        b.rows[0]['validation_report_json']=json.dumps({'retention':report(TABLE,6,'trusted-uuid')})
        provider=Provider(TABLE,'trusted-uuid',clocks=[2*DAY,8*DAY])
        original=SimpleNamespace(validate_descriptor=b.validate_descriptor)
        b.validate_descriptor=RetentionNativePolicy(original,RetentionGate(provider,minimum_margin_us=MARGIN)).validate_descriptor
        with self.assertRaises(RetentionError):singleton_tests.SingletonTests.read(self,p,e,b,policy)
        self.assertFalse(p.active);self.assertEqual(provider.calls,2)
        self.assertTrue(any('LIMIT 2' in sql for sql,_ in e.calls))

    def test_sql_provider_checks_uuid_clock_span_and_reserved_margin(self):
        class Executor:
            def query(self,sql,params):
                if sql.startswith('DESCRIBE'):return SQLResult([{'id':'original'}])
                if sql.startswith('SHOW'):return SQLResult([])
                return SQLResult([{'now_us':str(2*DAY)}])
        provider=SQLRetentionProvider(Executor(),{T:'original'},defaults={'data_retention':'7 days','log_retention':'30 days'},default_profile='independently-qualified',max_observation_span_us=1000)
        descriptor=Descriptor('p','selected',{T:0},{'s':'r'},{},{'retention':report()}, {})
        gate=RetentionGate(provider,minimum_margin_us=MARGIN)
        with patch('ashlar.retention_policy.time.monotonic_ns',side_effect=[0,1]):self.assertIsNone(gate.check(descriptor,'host-context'))
        with patch('ashlar.retention_policy.time.monotonic_ns',side_effect=[0,2000000]):
            with self.assertRaises(RetentionError):gate.check(descriptor,'host-context')
        with self.assertRaises(RetentionError):RetentionGate(provider,minimum_margin_us=999)
