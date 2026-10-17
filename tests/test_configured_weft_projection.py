"""Authored tiny IR fixtures in actual minted source/read/alias owners."""
import copy,json,unittest,uuid
from types import SimpleNamespace
from unittest.mock import patch
import test_configured_query_aliases as fixtures
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.configured_weft_binding import configured_count_star_request
from ashlar_host.configured_weft_projection import configured_projection_oracle
from ashlar_host.count_star_admission import BACKEND

class Tests(unittest.TestCase):
    def test_required_string_positions_count_and_shape_refusals(self):
        expired=[]
        def action(scope,transport,aliases,spark):
            c=scope.source.configuration;model=c.intake.source;source=scope.source.original_source
            def make(*,count=False,duplicate=False,optional=False):
                sql='SELECT COUNT(*) AS n FROM Item i'if count else ('SELECT i.label AS first_label,i.label AS second_label FROM Item i'if duplicate else 'SELECT i.label FROM Item i')
                request=configured_count_star_request(scope,dict(aliases),sql,layout_sha256=LAYOUT_SHA256)
                binding=json.loads(request['target']['bindingJson']);record=binding['records'][0];pin=request['modules'][0]['pin'];logical=record['logical']
                scalar={'family':'integer'if count else'string','facets':{},'nullable':False};field=next(p['logical']for p in record['properties']if p['logical']['element']==('caption'if optional else'label'))
                expression={'op':'count','type':scalar}if count else{'op':'field','scan':'s0','identity':field};name='n'if count else'label'
                names=['first_label','second_label']if duplicate else[name]
                outputs=[{'expression':expression,'name':output_name}for output_name in names]
                columns=[{'nullable':False,'outputName':output['name'],'position':i,'representation':{'kind':'scalar','carrier':'text','decoder':'exact-integer'if count else'text','logicalType':scalar},'sourceIdentities':[logical if count else field]}for i,output in enumerate(outputs,1)]
                ir={'aggregate':count,'filters':[],'groups':[],'irVersion':'weft-ir/0.4.1','joins':[],'limit':None,'modulePins':[pin],'order':[],'outputs':outputs,'pageKey':None,'readProfile':None,'requiredCapabilities':['aggregate','aggregate.count','project','scan']if count else['project','scan','type.string'],'source':{'occurrence':'s0','pin':pin,'record':logical},'typeGraph':[]if count else[{'availability':'required','identity':field,'kind':'scalar','type':scalar}]}
                artifact={'status':'compiled','backend':dict(BACKEND),'interfaceVersion':'weft-compile/0.4.1','dialect':'weft-sql/0.4.1','bindingSha256':request['target']['bindingSha256'],'modelPins':[pin],'logicalPlan':ir,'columns':columns}
                return request,artifact
            request,artifact=make(duplicate=True);oracle=configured_projection_oracle(scope,request,artifact)
            result=oracle(model,source,request);self.assertEqual(result['rows'],[['first','first'],['second','second']])
            result['rows'][0][0]='caller mutation';self.assertEqual(oracle(model,source,request)['rows'][0],['first','first'])
            # Unaliased repeated labels require the separate positioned-output
            # capability; this initial oracle subset deliberately refuses it.
            positioned=copy.deepcopy(artifact)
            positioned['logicalPlan']['requiredCapabilities'].append('project.positionedOutputs')
            for index,(output,column)in enumerate(zip(positioned['logicalPlan']['outputs'],positioned['columns'])):
                output['name']='label';column['outputName']='label';column['carrierName']='output_'+str(index)
            with self.assertRaises(ValueError):configured_projection_oracle(scope,request,positioned)
            count_request,count_artifact=make(count=True)
            self.assertEqual(configured_projection_oracle(scope,count_request,count_artifact)(model,source,count_request)['rows'],[['2']])
            for member,value in (('filters',[{}]),('joins',[{}]),('groups',[{}]),('order',[{}]),('limit',1),('pageKey',{}),('readProfile',{})):
                changed=copy.deepcopy(artifact);changed['logicalPlan'][member]=value
                with self.subTest(member=member),self.assertRaises(ValueError):configured_projection_oracle(scope,request,changed)
            optional_request,optional=make(optional=True)
            with self.assertRaises(ValueError):configured_projection_oracle(scope,optional_request,optional)
            with self.assertRaises(ValueError):oracle(model,source+b'changed',request)
            changed=copy.deepcopy(request);changed['sql']='foreign'
            with self.assertRaises(ValueError):oracle(model,source,changed)
            expired.append((oracle,model,source,request))
        def native_port(**fields):
            if 'uuid'in fields:fields['uuid']=str(uuid.uuid5(uuid.NAMESPACE_URL,fields['uuid']))
            return SimpleNamespace(**fields)
        with patch.object(fixtures.read_fixture,'SimpleNamespace',side_effect=native_port):fixtures.run_with_alias_scope(action)
        oracle,model,source,request=expired[0]
        with self.assertRaises(ValueError):oracle(model,source,request)
