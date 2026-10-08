from dataclasses import replace
import unittest
from ashlar.publication import Descriptor
from ashlar.retention import RetentionError,interval_microseconds,publication_retention_report,validate_publication_retention as validate_core
from ashlar.retention import observe_retention_configuration
from ashlar.native import SQLResult
T='c.s.t';DAY=86400000000
def validate_publication_retention(d,c,*,now_us):return validate_core(d,c,now_us=now_us,table_uuids={T:'original'})
class RetentionTests(unittest.TestCase):
    def pair(self):
        config={T:{'data_retention':'interval 7 days','log_retention':'interval 30 days'}}
        report=publication_retention_report({T:{'uuid':'original','version':5,'committed_at':str(DAY)}},config,margin_us=1000000)
        return Descriptor('p','selected',{T:5},{'s':'r'},{},{'retention':report},{}),config
    def test_window_uses_snapshot_commit_and_minimum_data_log_duration(self):
        d,c=self.pair();deadline=8*DAY-1000000
        self.assertEqual(validate_publication_retention(d,c,now_us=str(2*DAY)),str(deadline))
        with self.assertRaises(RetentionError):validate_publication_retention(d,c,now_us=str(deadline))
    def test_shortened_configuration_tightens_and_longer_cannot_extend_original(self):
        d,c=self.pair();c[T]['log_retention']='1 day'
        with self.assertRaises(RetentionError):validate_publication_retention(d,c,now_us=str(2*DAY))
        c[T]={'data_retention':'30 days','log_retention':'30 days'}
        with self.assertRaises(RetentionError):validate_publication_retention(d,c,now_us=str(8*DAY))
    def test_unknown_missing_future_or_mismatched_snapshot_refuses(self):
        d,c=self.pair()
        for bad in [{},{T:{'data_retention':'7 days'}},{T:{'data_retention':'1 month','log_retention':'30 days'}}]:
            with self.assertRaises(RetentionError):validate_publication_retention(d,bad,now_us=str(2*DAY))
        with self.assertRaises(RetentionError):validate_publication_retention(d,c,now_us='0')
        with self.assertRaises(RetentionError):validate_publication_retention(replace(d,versions={T:6}),c,now_us=str(2*DAY))
    def test_native_identity_substitution_refuses(self):
        d,c=self.pair()
        with self.assertRaises(RetentionError):validate_core(d,c,now_us=str(2*DAY),table_uuids={T:'replaced'})
    def test_fixed_intervals_and_explicit_margin(self):
        self.assertEqual(interval_microseconds('interval 1 week'),7*DAY)
        for value in ['0 days','-1 day','1 month','1.5 days',None]:
            with self.assertRaises(RetentionError):interval_microseconds(value)
        d,c=self.pair()
        with self.assertRaises(RetentionError):publication_retention_report({T:{'uuid':'original','version':5,'committed_at':'0'}},c,margin_us=7*DAY)

class RetentionObservationTests(unittest.TestCase):
    def observer(self,properties,final_uuid='original'):
        class Executor:
            def __init__(self):self.identities=0
            def query(self,sql,parameters):
                if sql.startswith('DESCRIBE DETAIL'):
                    self.identities+=1
                    return SQLResult([{'id':'original' if self.identities==1 else final_uuid}])
                return SQLResult(properties)
        return Executor()
    def observe(self,executor,defaults=None):
        return observe_retention_configuration(executor,T,'original',defaults=defaults or {'data_retention':'7 days','log_retention':'30 days'},default_profile='explicit-qualified-defaults')
    def test_explicit_override_and_missing_property_keep_distinct_custody(self):
        rows=[{'key':'delta.deletedFileRetentionDuration','value':'2 days'},{'key':'delta.future','value':'opaque'}]
        result=self.observe(self.observer(rows))
        self.assertEqual(result['configuration'],{'data_retention':'2 days','log_retention':'30 days'})
        self.assertEqual(result['sources'],{'data_retention':'explicit-property','log_retention':'explicit-qualified-defaults'})
        self.assertEqual(result['original_properties'],tuple(rows))
    def test_native_identity_change_and_ambiguous_or_unsupported_retention_refuse(self):
        with self.assertRaises(RetentionError):self.observe(self.observer([],final_uuid='replacement'))
        for rows in [
            [{'key':'delta.logRetentionDuration','value':'1 month'}],
            [{'key':'x','value':'a'},{'key':'x','value':'b'}],
            [{'key':'x','value':None}],
        ]:
            with self.assertRaises(RetentionError):self.observe(self.observer(rows))
        with self.assertRaises(RetentionError):self.observe(self.observer([]),defaults={'data_retention':'7 days'})
