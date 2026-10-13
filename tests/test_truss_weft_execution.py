"""Basic held-query lifecycle controls, no native compiler/engine qualification."""
import json
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_graph_admission as cases
from ashlar.weft_distribution import DistributionPaths
from ashlar_host.delta_custody import encoded
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_weft_query import BACKEND,execute_truss_string_query


class Tests(unittest.TestCase):
    def test_exact_compiler_sql_integrity_before_query_and_closing(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs)
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1',
            'table_versions_json':encoded(tables),'schema_revisions_json':encoded({json.loads(batch.begin)['prior']['installationId']:'1'}),
            'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        driver=object.__new__(NativeDriver);driver.source_admission=admission;driver.context=object()
        driver.source_sessions=object.__new__(TrussFeedSessions);driver.ack_sessions=driver.source_sessions
        driver.policy=SimpleNamespace(active={'request':{'stream':'feed'},'manifest':manifest})
        driver.tables={'manifest':'local.truss.manifest'}
        targets={table:SimpleNamespace(uuid='00000000-0000-0000-0000-'+str(index).zfill(12),path=Path('/original')/table.split('.')[-1])for index,table in enumerate((*tables,'local.truss.manifest'),1)}
        events=[];guard=['0'];result=['exact']
        class Frame:
            def __init__(self,name,values):
                self.schema=SimpleNamespace(fields=[SimpleNamespace(name=name,dataType=SimpleNamespace(simpleString=lambda:'string'),nullable=False)])
                self.values=values
            def toLocalIterator(self,**kwargs):return iter([[value]for value in self.values])
        def spark_sql(sql,**kwargs):
            events.append(sql)
            if sql=='SELECT guard':return Frame('violations',guard)
            if sql=='WITH original_user_cte SELECT original_user_sql':return Frame('id',result)
            if sql.startswith('DESCRIBE DETAIL'):
                role=sql.rsplit('`.`',1)[-1].rstrip('`')
                return SimpleNamespace(first=lambda:{'id':targets['local.truss.'+role].uuid})
            return SimpleNamespace(collect=lambda:[])
        driver.transport=SimpleNamespace(targets=targets,spark=SimpleNamespace(sql=spark_sql))
        @contextmanager
        def writer(*args):
            events.append('writer-open')
            try:yield
            finally:events.append('writer-close')
        @contextmanager
        def hold(*args,**kwargs):
            events.append('pins-open')
            try:yield
            finally:events.append('pins-close')
        driver.writer=writer;driver.hold=hold;driver.validate_manifest=lambda *args:events.append('native-close')
        def compiler(paths,raw):
            request=json.loads(raw);binding=json.loads(request['target']['bindingJson'])
            self.assertTrue(all(table['name'][0]=='spark_catalog'for table in binding['publication']['tables']))
            artifact={'status':'compiled','backend':BACKEND,'bindingSha256':request['target']['bindingSha256'],'modelPins':binding['modelPins'],
                'parameters':[],'sql':'WITH original_user_cte SELECT original_user_sql','columns':[{'position':1,'outputName':'id','nullable':False,'representation':{'kind':'scalar','logicalType':{'family':'string','facets':{},'nullable':False},'carrier':'text','decoder':'text'}}],
                'obligations':[{'id':'ashlar.candidate.publication','owner':'host','failureCode':'WFT-BINDING','parameters':{key:binding[key]for key in ('publication','modelPins','layoutRevision','layoutSha256')}},
                {'id':'ashlar.candidate.scalarIntegrity','owner':'host','failureCode':'WFT-BINDING','parameters':{'phase':'before-user-query','samePublicationRequired':True,'noPartialPublication':True,'checks':[{'sql':'SELECT guard','failureCode':'WFT-BINDING','record':binding['records'][0]['logical']}]}}]}
            return (encoded(artifact)+'\n').encode()
        paths=DistributionPaths(Path('/index'),Path('/compiler'))
        with patch('ashlar_host.truss_weft_query.resolve_publication',return_value=SimpleNamespace(descriptor=SimpleNamespace(raw=manifest))),patch('ashlar_host.truss_weft_query.compile_distribution',side_effect=compiler):
            answer=execute_truss_string_query(driver,paths,'SELECT a.id FROM A a',revision='1',document_id='cohort',maximum_rows=1)
            self.assertEqual(answer['rows'],[{'id':'exact'}])
            self.assertLess(events.index('SELECT guard'),events.index('WITH original_user_cte SELECT original_user_sql'))
            self.assertEqual(events[-2:],['pins-close','writer-close'])
            events.clear();guard[:]=['1']
            with self.assertRaisesRegex(ValueError,'integrity check refused'):
                execute_truss_string_query(driver,paths,'SELECT a.id FROM A a',revision='1',document_id='cohort',maximum_rows=1)
            self.assertNotIn('WITH original_user_cte SELECT original_user_sql',events)
            self.assertEqual(events[-2:],['pins-close','writer-close'])
            events.clear();guard[:]=['0'];result[:]=['first','overflow']
            with self.assertRaises(ValueError):
                execute_truss_string_query(driver,paths,'SELECT a.id FROM A a',revision='1',document_id='cohort',maximum_rows=1)
            self.assertEqual(events[-2:],['pins-close','writer-close'])
