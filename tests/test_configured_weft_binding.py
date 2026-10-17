"""Pure builder checks inside the existing real minted read-scope fixture."""
import json,hashlib,unittest,uuid
from types import SimpleNamespace
from unittest.mock import patch
import test_configured_local_read as read_fixture
from ashlar_host.configured_local_read import open_configured_local_read
from ashlar_host.configured_weft_binding import configured_count_star_request
from ashlar_host.count_star_admission import BACKEND
from ashlar.weft_binding import string_compile_request,LAYOUT_SHA256

class Tests(unittest.TestCase):
    def test_original_model_vector_and_homes_survive_alias_projection(self):
        original_read=read_fixture.read_configured_local_singleton;tested=[];expired=[]
        def read(transport,source,progress,**kwargs):
            if tested:return original_read(transport,source,progress,**kwargs)
            scope_args={k:v for k,v in kwargs.items()if k not in ('type_id','entity_id')}
            with open_configured_local_read(transport,source,progress,**scope_args)as scope:
                aliases={table:'spark_catalog.ashlar_configured_query_test.'+table.split('.')[-1]for table in scope.descriptor.versions}
                sql='SELECT i.label FROM Item i';manifest=dict(scope.descriptor.raw)
                c=source.configuration
                baseline=string_compile_request(sql,c.intake,c.policy,scope.descriptor,schema_alias=c.schema_alias,table_uuids={t:p[0]for t,p in scope.vector.targets.items()},manifest_uuid=scope.manifest_uuid,layout_sha256=LAYOUT_SHA256)
                request=configured_count_star_request(scope,aliases,sql,layout_sha256=LAYOUT_SHA256)
                self.assertEqual(request['sql'],sql);self.assertEqual(request['modules'],baseline['modules'])
                self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.1');self.assertEqual(request['dialect'],'weft-sql/0.4.1')
                self.assertEqual(set(request['target']),{'backendId','backendVersion','targetProfile','bindingJson','bindingSha256'})
                for key in ('backendId','backendVersion','targetProfile'):self.assertEqual(request['target'][key],BACKEND[key])
                old=json.loads(baseline['target']['bindingJson']);new=json.loads(request['target']['bindingJson'])
                self.assertEqual(old['records'],new['records']);self.assertEqual(old['modelPins'],new['modelPins'])
                for original,physical in zip(old['publication']['tables'],new['publication']['tables']):
                    self.assertEqual(physical,{**original,'name':aliases['.'.join(original['name'])].split('.')})
                self.assertEqual(new['publication']['manifestUuid'],scope.manifest_uuid);self.assertEqual(dict(scope.descriptor.raw),manifest)
                self.assertEqual(request['target']['bindingSha256'],hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest())
                for changed in ({}, {k:'spark_catalog.ashlar_configured_query_other.'+v.split('.')[-1]if i==0 else v for i,(k,v)in enumerate(aliases.items())}, {k:'spark_catalog.ashlar_configured_query_test.foreign'for k in aliases}):
                    with self.assertRaises(ValueError):configured_count_star_request(scope,changed,sql,layout_sha256=LAYOUT_SHA256)
                with self.assertRaises(ValueError):configured_count_star_request(SimpleNamespace(),aliases,sql,layout_sha256=LAYOUT_SHA256)
                tested.append(request);expired.append((scope,aliases));return scope.singleton(kwargs['type_id'],kwargs['entity_id'])
        # Reuse the existing inert native/source/progress fixture rather than mint
        # a fake token or duplicate its complete five-phase setup. Its ordinary
        # singleton and no-write assertions still execute unchanged.
        def native_port(**fields):
            if 'uuid'in fields:fields['uuid']=str(uuid.uuid5(uuid.NAMESPACE_URL,fields['uuid']))
            return SimpleNamespace(**fields)
        with patch.object(read_fixture,'SimpleNamespace',side_effect=native_port),patch.object(read_fixture,'read_configured_local_singleton',side_effect=read):
            helper=read_fixture.Tests();self.addCleanup(helper.doCleanups)
            helper.test_old_and_deleted_singleton_full_core_read_and_no_writes()
        self.assertEqual(len(tested),1)
        scope,aliases=expired[0]
        with self.assertRaises(ValueError):configured_count_star_request(scope,aliases,'SELECT COUNT(*) FROM Item i',layout_sha256=LAYOUT_SHA256)
