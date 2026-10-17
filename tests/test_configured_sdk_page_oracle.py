"""Original graph/read owners with inert compiler metadata; no engine calls."""
import copy,json,unittest
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.configured_sdk_page_oracle import configured_sdk_page_oracle,STRING
from ashlar_host.configured_typed_graph_sdk_binding import configured_typed_graph_sdk_request
from test_configured_typed_graph_projection_oracle import graph_scope


def authored(scope,filtered=False):
    c=scope.source.configuration;aliases={n:'spark_catalog.ashlar_configured_query_test.'+n.split('.')[-1]for n in scope.descriptor.versions}
    params={'name':{'family':'string','value':'Ben'}}if filtered else{}
    request=configured_typed_graph_sdk_request(c,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,aliases,'Original uninterpreted SQL',params,layout_sha256=LAYOUT_SHA256)
    binding=json.loads(request['target']['bindingJson']);record=next(r for r in binding['records']if r['typeId']=='17');pin=request['modules'][0]['pin'];ids=[p['logical']for p in record['properties']]
    key={'id':'identity','fields':[ids[0]],'types':[STRING]};names=['key','name','__weft_page_key_0'];fields=[ids[0],ids[1],ids[0]]
    outputs=[{'name':n,'expression':{'op':'field','identity':f,'scan':'s0'}}for n,f in zip(names,fields)]
    columns=[{'nullable':False,'outputName':n,'position':i+1,'representation':{'kind':'scalar','logicalType':STRING,'carrier':'text','decoder':'text'},'sourceIdentities':[f]}for i,(n,f)in enumerate(zip(names,fields))]
    caps={'key.uniqueStable','limit','order.asc','project','scan','type.string'}
    filters=[]
    if filtered:
        caps.update(('equal','filter','parameter.named'));filters=[{'left':{'identity':ids[1],'scan':'s0','span':{'start':0,'end':1},'type':STRING},'op':'equal','right':{'kind':'parameter','name':'name','span':{'start':0,'end':1},'type':STRING,'value':'Ben'}}]
    ir={'aggregate':False,'filters':filters,'groups':[],'irVersion':'weft-ir/0.2.2','joins':[],'limit':3,'modulePins':[pin],'order':[{'identity':ids[0],'scan':'s0','span':{'start':0,'end':1},'type':STRING}], 'outputs':outputs,'pageKey':key,'readProfile':{'subset':'entity-page','version':'weft-application-read/0.2.0'},'requiredCapabilities':sorted(caps),'source':{'occurrence':'s0','pin':pin,'record':record['logical']},'typeGraph':[{'availability':'required','identity':f,'kind':'scalar','type':STRING}for f in ids]}
    backend={'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-pages-candidate','interfaceVersion':'weft-backend/0.2.2','targetProfile':'spark4.0.1-delta4.0.0-candidate'}
    artifact={'status':'compiled','backend':backend,'interfaceVersion':'weft-compile/0.2.2','dialect':'weft-sql/0.2.0','modelPins':[pin],'bindingSha256':request['target']['bindingSha256'],'logicalPlan':ir,'columns':columns}
    return request,{'version':'weft.engine-page/0.1','status':'compiled','compiler':'ashlar-spark4-pages','request':request,'artifact':artifact,'after':None,'pageSize':2,'page':{'generatedContinuationFilter':None,'generatedOrderPositions':[0],'generatedOutputPositions':[2],'generatedSpans':'diagnostic-anchors','keyOutputs':[2],'pageSize':2,'visibleOutputs':2},'outputs':[{'carrier':'string','name':n,'nullable':False}for n in names[:2]],'structuralDigest':{'scope':'inert metadata'}}


class Tests(unittest.TestCase):
    def test_real_held_prefix_first_lookahead_after_named_filter_old_vs_current_and_expiry(self):
        for ordinal in (1,3):
            with graph_scope(ordinal)as scope:
                request,program=authored(scope);oracle=configured_sdk_page_oracle(scope,request,program)
                expected=[['person-1','Ada','person-1'],['person-2','Ben','person-2'],['person-3','雪','person-3']]
                if ordinal==3:expected=[['person-1','Ada','person-1'],['person-3','雪','person-3']]
                self.assertEqual(oracle({},None)['rows'],expected)
                self.assertEqual(oracle({},('person-1',))['rows'],expected[1:])
                request,program=authored(scope,True);filtered=configured_sdk_page_oracle(scope,request,program)
                self.assertEqual(filtered({'name':'Ben'},None)['rows'],[['person-2','Ben','person-2']]if ordinal==1 else[])
                self.assertEqual(filtered({'name':'雪'},None)['rows'],[['person-3','雪','person-3']])
            with self.assertRaises(ValueError):oracle({},None)
    def test_descriptor_key_hidden_filter_inventory_and_original_source_drift_refuse(self):
        with graph_scope(1)as scope:
            request,program=authored(scope,True)
            for mutation in ('after','hidden','key','literal','join'):
                changed=copy.deepcopy(program)
                if mutation=='after':changed['after']=['person-1']
                elif mutation=='hidden':changed['artifact']['columns'][-1]['outputName']='foreign'
                elif mutation=='key':changed['artifact']['logicalPlan']['pageKey']['id']='foreign'
                elif mutation=='literal':changed['artifact']['logicalPlan']['filters'][0]['right']['kind']='literal'
                else:changed['artifact']['logicalPlan']['joins']=[True]
                with self.subTest(mutation=mutation),self.assertRaises(ValueError):configured_sdk_page_oracle(scope,request,changed)
            # Authentic041 AND capability is required only for multiple predicates.
            both=copy.deepcopy(program);both['artifact']['logicalPlan']['filters'].append(copy.deepcopy(both['artifact']['logicalPlan']['filters'][0]))
            both['artifact']['logicalPlan']['requiredCapabilities'].append('and')
            self.assertEqual(configured_sdk_page_oracle(scope,request,both)({'name':'Ben'},None)['rows'],[['person-2','Ben','person-2']])
            both['artifact']['logicalPlan']['requiredCapabilities'].remove('and')
            with self.assertRaises(ValueError):configured_sdk_page_oracle(scope,request,both)
            oracle=configured_sdk_page_oracle(scope,request,program)
            for params,after in [({},None),({'name':1},None),({'name':'Ben'},['person-1']),({'name':'Ben'},('person-1','extra'))]:
                with self.assertRaises(ValueError):oracle(params,after)
            source=scope.source.configuration.source;original=source.read_bytes();source.write_bytes(original+b' ')
            try:
                with self.assertRaises(ValueError):oracle({'name':'Ben'},None)
            finally:source.write_bytes(original)
