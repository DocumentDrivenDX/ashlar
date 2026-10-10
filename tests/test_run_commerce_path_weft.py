"""Source-only execution-port tests; fake frames grant no native qualification."""
import copy
import base64
import zlib
import json
import hashlib
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from test_weft_path_plan import FIXTURES, SCHEMAS
from weft_path_plan import PathAdmissionConfig, PathSchemaValidation
from weft_path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
from run_commerce_path_weft import (PathExecutionConfig, PathExecutionError,
    execute_commerce_path, HeldFrameAdapter, _scalar)


class Iterator:
    def __init__(self, rows): self.rows = iter(rows); self.closed = False
    def __iter__(self): return self
    def __next__(self): return next(self.rows)
    def close(self): self.closed = True


def frame(names, rows):
    fields = [SimpleNamespace(name=n, nullable=True,
        dataType=SimpleNamespace(simpleString=lambda: 'string')) for n in names]
    return SimpleNamespace(schema=SimpleNamespace(fields=fields),
        toLocalIterator=lambda **kwargs: Iterator(rows))


class Provider:
    def __init__(self, artifact, rows):
        self.context = object(); self.active = False; self.calls = []; self.fail_close = False
        self.artifact, self.rows = artifact, rows
        self.driver = SimpleNamespace(transport=SimpleNamespace(spark=SimpleNamespace(sql=self.sql)))
        self.tables = json.loads(self.fixture_request['target']['bindingJson'])['publication']['tables'] if hasattr(self, 'fixture_request') else []
    @contextmanager
    def interval(self, context):
        self.active = True
        try: yield
        finally: self.active = False
        if self.fail_close: raise PathExecutionError('Fixture interval closing refused')
    def runtime(self, context): self.calls.append('runtime')
    def resolve(self, context): return 'fixed full fixture vector'
    def admit_binding(self, *args): self.calls.append('binding')
    def native_table_schema(self, table, *args):
        self.calls.append('schema')
        types = {'id':'BIGINT','source_system':'STRING','type_id':'BIGINT',
                 'schema_revision':'STRING','props_json':'STRING','rel_type_id':'BIGINT',
                 'source_id':'BIGINT','target_id':'BIGINT'}
        return {'table': table, 'schema': {'type': 'struct', 'fields': [
            {'name': name, 'type': 'long' if types[name]=='BIGINT' else 'string', 'nullable': True, 'metadata': {}} for name in types]},
            'nativeTypes': list(map(list, types.items()))}
    def closed_interval_custody(self, context): return {'fixture_only': True, 'closed': True}
    def sql(self, sql, args):
        if not self.active: raise AssertionError('Unheld SQL')
        self.calls.append(('sql', sql, copy.deepcopy(args)))
        if sql == self.artifact['sql']:
            names = [c.get('carrierName', c['outputName']) for c in self.artifact['columns']]
            return frame(names, self.rows)
        return frame(['violations'], [['0']])


class PathExecutionTests(unittest.TestCase):
    def fixture(self, name='original_collection_retains_pins_keys_wide_order_and_closed_inventory'):
        return copy.deepcopy(next(x for x in FIXTURES if x['test'] == name and x['response']['status'] == 'compiled'))
    def setup(self, fixture, rows):
        provider = Provider(fixture['response'], rows)
        opened = SimpleNamespace(provider=provider, context=provider.context,
            model=fixture['request']['modules'][0]['documentJson'].encode('utf8'), graph=b'original graph fixture',
            original_native_files={'native.parquet': 'fixed'}, native_files=lambda: {'native.parquet': 'fixed'})
        config = PathExecutionConfig(PathAdmissionConfig(16 * 1024 * 1024,
            PathSchemaValidation(SCHEMAS, lambda *args: None)),
            PathCaptureConfig(100, 10000, 100000), PathDecodeConfig(10000), None, None)
        oracle = lambda model, graph, request: {'rows': rows, 'witnesses': {'literal': True}, 'scope': 'fixture only'}
        return opened, config, oracle
    def execute(self, x, opened, config, oracle):
        return execute_commerce_path(opened, x['request'], x['response'], copy.deepcopy(x['response']),
            config=config, original_oracle=oracle)
    def test_complete_collection_guard_order_and_original_slots(self):
        x = self.fixture(); rows = [['L1', '{"items":[{"intermediate":["P1"],"terminal":["S1"],"edges":["4","1"]}],"truncated":false}']]
        opened, config, oracle = self.setup(x, rows)
        result = self.execute(x, opened, config, oracle)
        self.assertEqual(result['native_result']['rows'], rows)
        self.assertEqual(result['decoded'][0][1].items[0].edges, ('4', '1'))
        self.assertFalse(opened.provider.active)
        sql_calls = [c for c in opened.provider.calls if isinstance(c, tuple)]
        self.assertEqual(sql_calls[-1][1], x['response']['sql'])
        self.assertEqual(sql_calls[-1][2], {'p'+str(p['position']): p['value'] for p in x['response']['parameters']})
        self.assertGreater(len(result['guards']), 3)
    def test_all_selected_compiled_profiles_retain_all_guard_roles(self):
        total = 0
        for original in FIXTURES:
            if original['response']['status'] != 'compiled': continue
            x = copy.deepcopy(original)
            opened, config, oracle = self.setup(x, [])
            # This trusted fixture callback tests plumbing only, not public UMF semantics.
            def public_source(request, artifact, checks):
                raw = json.dumps({'sourceText':request['modules'][0]['documentJson'],
                    'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],
                    'checks':checks})
                receipt = json.dumps({'originalRequestText':raw,'umfRevision':'fixture-source', 'admitted':True})
                return {'originalRequestText':raw,'originalReceiptText':receipt,
                    'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
            config = PathExecutionConfig(config.admission, config.capture, config.decoder,
                                         public_source, 'fixture-source')
            result = self.execute(x,opened,config,oracle)
            expected_checks = sum(len(o['parameters'].get('checks',o['parameters'].get('scans',[])))
                                  for o in x['response']['obligations'])
            self.assertEqual(len(result['guards']), expected_checks)
            self.assertFalse(opened.provider.active)
            total += 1
        self.assertEqual(total, 17)

    def test_actual_arithmetic_source_guard_and_receipt_precede_capacity(self):
        x=self.fixture('scalar_parameters_and_arithmetic_keep_original_tokens_and_separate_guards')
        opened,config,oracle=self.setup(x, [])
        def source(request, artifact, checks):
            calls=[c[1] for c in opened.provider.calls if isinstance(c,tuple)]
            arithmetic=next(o for o in x['response']['obligations'] if o['id']=='ashlar.arithmetic.exact')
            self.assertFalse(any(c['sql'] in calls for c in arithmetic['parameters']['checks']))
            self.assertTrue(all(c['check']['sql'] in calls for c in checks))
            raw=json.dumps({'sourceText':request['modules'][0]['documentJson'],
                'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':checks})
            # Mutating these isolated callback snapshots must not alter native SQL.
            artifact['sql']='FORGED CALLBACK SQL';request['sql']='FORGED'
            checks.clear()
            opened.provider.calls.append('public-receipt')
            receipt=json.dumps({'originalRequestText':raw,'umfRevision':'fixture-source','admitted':True})
            return {'originalRequestText':raw,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
        config=PathExecutionConfig(config.admission,config.capture,config.decoder,source,'fixture-source')
        result=self.execute(x,opened,config,oracle)
        calls=opened.provider.calls
        receipt_position=calls.index('public-receipt')
        arithmetic=next(o for o in x['response']['obligations'] if o['id']=='ashlar.arithmetic.exact')
        for check in arithmetic['parameters']['checks']:
            position=next(i for i,c in enumerate(calls) if isinstance(c,tuple) and c[1]==check['sql'])
            self.assertGreater(position,receipt_position)
        self.assertEqual([c for c in calls if isinstance(c,tuple)][-1][1],x['response']['sql'])
        self.assertIsNotNone(result['public_source'])

    def test_actual_nonempty_direct_integer_is_independent_of_other_arithmetic(self):
        fixtures=json.loads(zlib.decompress(base64.b64decode('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')))
        self.assertEqual(len(fixtures),2)
        for original in fixtures:
            for mutation in (None,'missing-own-guard','wrong-own-identity','missing-source'):
                x=copy.deepcopy(original)
                if mutation in ('missing-own-guard','wrong-own-identity'):
                    owning=next(o for o in x['response']['obligations'] if o['id']=='ashlar.candidate.scalarIntegrity')
                    guard=next(c for c in owning['parameters']['checks'] if c.get('representabilityOnly'))
                    if mutation=='missing-own-guard':owning['parameters']['checks'].remove(guard)
                    else:guard['field']['element']='other.original.field'
                opened,config,oracle=self.setup(x,x['nativeRows'])
                def source(request,artifact,checks):
                    raw=json.dumps({'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],
                        'bindingSha256':artifact['bindingSha256'],'checks':checks})
                    receipt=json.dumps({'originalRequestText':raw,'umfRevision':'fixture-source','admitted':True})
                    return {'originalRequestText':raw,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
                config=PathExecutionConfig(config.admission,config.capture,config.decoder,
                    None if mutation=='missing-source' else source,None if mutation=='missing-source' else 'fixture-source')
                if mutation:
                    with self.assertRaises(ValueError):self.execute(x,opened,config,oracle)
                else:
                    result=self.execute(x,opened,config,oracle)
                    self.assertEqual(result['decoded'][0][0].value,10)
                    self.assertEqual(result['decoded'][0][0].original,'10')

    def test_guard_failure_never_executes_query(self):
        x=self.fixture(); opened,config,oracle=self.setup(x, [])
        opened.provider.driver.transport.spark.sql=lambda sql,args: frame(['violations'], [['1']])
        with self.assertRaises(PathExecutionError): self.execute(x,opened,config,oracle)
        self.assertFalse(opened.provider.active)
    def test_empty_perhop_schema_still_required(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        opened.provider.native_table_schema=lambda table,*args: {'table':table,'schema':{'type':'struct','fields':[{'name':'id','nullable':True}]},'nativeTypes':[['id','STRING']]}
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
        self.assertFalse(any(isinstance(c,tuple) for c in opened.provider.calls))
    def test_schema_owning_types_and_order_cannot_disagree_even_empty(self):
        for mutation in ('missing-type', 'missing-metadata', 'wrong-id-type', 'reordered', 'duplicate'):
            x=self.fixture();opened,config,oracle=self.setup(x, [])
            original=opened.provider.native_table_schema
            def schema(table,*args):
                receipt=original(table,*args)
                if mutation=='missing-type':receipt['schema']['fields'][0].pop('type')
                if mutation=='missing-metadata':receipt['schema']['fields'][0].pop('metadata')
                if mutation=='wrong-id-type':receipt['schema']['fields'][0]['type']='string'
                if mutation=='reordered':receipt['nativeTypes'].reverse()
                if mutation=='duplicate':receipt['nativeTypes'].append(receipt['nativeTypes'][0])
                return receipt
            opened.provider.native_table_schema=schema
            with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
    def test_suppressing_interval_cannot_publish_incomplete_body(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        @contextmanager
        def suppress(context):
            opened.provider.active=True
            try:yield
            except PathExecutionError:pass
            finally:opened.provider.active=False
        opened.provider.interval=suppress
        opened.provider.driver.transport.spark.sql=lambda *args,**kwargs:frame(['violations'],[['1']])
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)

    def test_interval_close_failure_withholds_provisional_result(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        opened.provider.fail_close=True
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
    def test_original_bag_mismatch_refuses_without_repair(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        oracle=lambda *args:{'rows':[['L1','missing']],'witnesses':{},'scope':'literal independent fixture'}
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
    def test_missing_source_port_refuses_before_provider(self):
        x=self.fixture('scalar_parameters_and_arithmetic_keep_original_tokens_and_separate_guards')
        opened,config,oracle=self.setup(x, [])
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
        self.assertEqual(opened.provider.calls, [])
    def test_full_closing_native_vector_drift_refuses(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        values=iter([{'native.parquet':'fixed'},{'native.parquet':'changed'}])
        opened.native_files=lambda:next(values)
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
    def test_opened_model_mismatch_refuses_before_callbacks(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        opened.model=b'different original source'
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
        self.assertEqual(opened.provider.calls, [])
    def test_capture_budget_withholds_whole_result(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        config=PathExecutionConfig(config.admission,PathCaptureConfig(100,1,1),config.decoder,None,None)
        with self.assertRaises(PathExecutionError):self.execute(x,opened,config,oracle)
    def test_tagged_scalar_value_source_and_left_absence(self):
        identity={'documentId':'doc','revision':'rev','module':'m','element':'field'}
        descriptor={'identity':identity,'kind':'scalar','availability':'required',
                    'type':{'family':'string','facets':{},'nullable':True}}
        column={'representation':{'kind':'value','descriptor':identity,'nativeNull':False}}
        self.assertEqual(_scalar(column,{'op':'field'},'{"state":"null"}',False,[descriptor]),{'state':'null'})
        with self.assertRaises(PathExecutionError):_scalar(column,{'op':'field'},'{"state":"absent"}',False,[descriptor])
        column['representation']['outerJoin']={'scan':'s1','record':identity}
        self.assertEqual(_scalar(column,{'op':'field'},'{"state":"absent"}',False,[descriptor]),{'state':'absent'})
        self.assertEqual(_scalar(column,{'op':'field'},'{"state":"value","value":""}',False,[descriptor])['value'].original,'')
        with self.assertRaises(PathExecutionError):_scalar(column,{'op':'field'},'{"state":"value","value":0}',False,[descriptor])

    def test_oracle_callback_and_retained_rows_are_isolated(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        retained={'rows':[], 'witnesses':{'original':True}, 'scope':'literal fixture'}
        def oracle(model,graph,request):
            request['sql']='forged oracle SQL'
            return retained
        original_interval=opened.provider.interval
        @contextmanager
        def mutate(context):
            with original_interval(context):yield
            retained['rows'].append(['forged'])
            retained['witnesses'].clear()
        opened.provider.interval=mutate
        result=self.execute(x,opened,config,oracle)
        self.assertEqual(result['oracle']['rows'],[])
        self.assertEqual(result['oracle']['witnesses'],{'original':True})
        self.assertEqual([c for c in opened.provider.calls if isinstance(c,tuple)][-1][1],x['response']['sql'])

    def test_validated_public_receipt_is_not_callback_owned(self):
        x=self.fixture('scalar_parameters_and_arithmetic_keep_original_tokens_and_separate_guards')
        opened,config,oracle=self.setup(x, [])
        retained={}
        def source(request,artifact,checks):
            raw=json.dumps({'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],
                'bindingSha256':artifact['bindingSha256'],'checks':checks})
            receipt=json.dumps({'originalRequestText':raw,'umfRevision':'fixture-source','admitted':True})
            retained.update(originalRequestText=raw,originalReceiptText=receipt,receiptSha256=hashlib.sha256(receipt.encode()).hexdigest())
            return retained
        original_interval=opened.provider.interval
        @contextmanager
        def mutate(context):
            with original_interval(context):yield
            retained['originalReceiptText']='forged after validation'
        opened.provider.interval=mutate
        config=PathExecutionConfig(config.admission,config.capture,config.decoder,source,'fixture-source')
        result=self.execute(x,opened,config,oracle)
        receipt=result['public_source']
        self.assertEqual(hashlib.sha256(receipt['originalReceiptText'].encode()).hexdigest(),receipt['receiptSha256'])
        self.assertIsNot(receipt,retained)
    def test_closing_file_snapshot_is_checked_once_and_retained(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        observations=[]
        def files():
            observations.append(True)
            return {'native.parquet':'fixed' if len(observations)<=2 else 'unchecked drift'}
        opened.native_files=files
        result=self.execute(x,opened,config,oracle)
        self.assertEqual(len(observations),2)
        self.assertEqual(result['opening_native_files'],result['closing_native_files'])

    def test_observed_schema_custody_is_not_callback_owned(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        retained=[];original_schema=opened.provider.native_table_schema
        def schema(*args):
            receipt=original_schema(*args);retained.append(receipt);return receipt
        opened.provider.native_table_schema=schema
        original_interval=opened.provider.interval
        @contextmanager
        def mutate(context):
            with original_interval(context):yield
            for receipt in retained:receipt['nativeTypes'][0][1]='forged after validation'
        opened.provider.interval=mutate
        result=self.execute(x,opened,config,oracle)
        self.assertTrue(all(s['nativeTypes'][0][1]=='BIGINT' for s in result['native_schemas']))

    def test_unheld_frame_port_refuses(self):
        x=self.fixture();opened,config,oracle=self.setup(x, [])
        with self.assertRaises(PathExecutionError):HeldFrameAdapter(opened.provider,opened.context,config.capture).capture('SELECT original', {})
    def test_exact_closed_math_and_count_branches(self):
        rep={'kind':'scalar','carrier':'text','decoder':'exact-integer','logicalType':{'family':'integer','facets':{},'nullable':False}}
        col={'representation':rep};self.assertEqual(_scalar(col,{'op':'arithmetic'},'-9007199254740993',True).original,'-9007199254740993')
        for value in ['1e2','1.0','9'*39]:
            with self.assertRaises(PathExecutionError):_scalar(col,{'op':'arithmetic'},value,True)
        for value in ['-1',str(2**63)]:
            with self.assertRaises(ValueError):_scalar(col,{'op':'count'},value,True)


if __name__=='__main__': unittest.main()
