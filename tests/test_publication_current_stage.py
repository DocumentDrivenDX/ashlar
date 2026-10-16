"""Pure mapping controls only; inert population has no source/ACK authority."""
import copy
import hashlib
import json
from types import SimpleNamespace
import unittest
from ashlar_host.publication_current_stage import commerce_current_rows,commerce_current_request,stage_commerce_current,PublicationCurrentStage

class StageTests(unittest.TestCase):
    def opened(self):
        model={'umf':'0.8.0','id':'urn:umf:domain:commerce','modules':[{'id':'domain','elements':[
            {'id':'products','kind':'record','members':[{'module':'domain','element':'products.sku'}]},
            {'id':'products.sku','kind':'field','name':'sku','scalarType':'string','cardinality':'one','nullability':'required'}]}]}
        return SimpleNamespace(model=json.dumps(model).encode(),manifest={'publication_id':'retained'},manifest_bytes=b'original manifest',
            bindings={'types':[{'identity':['object','domain','products'],'type_id':'7'}],
            'properties':[{'identity':[model['id'],'domain','products.sku'],'property_id':'21'}]},
            independent_expected={'object_current':[
                {'source_system':'private-original-commerce-fixture','type_id':'7','id':'3','props_json':'{"21":"SKU\\u0000λ"}'},
                {'source_system':'private-original-commerce-fixture','type_id':'7','id':'4','props_json':'{"21":""}'}]})

    def test_exact_publication_identity_raw_string_and_original_model(self):
        opened=self.opened();before=copy.deepcopy(opened.__dict__)
        rows=commerce_current_rows(opened)
        self.assertEqual([r[1] for r in rows],['SKU\x00λ',''])
        self.assertEqual(json.loads(rows[0][0]),['private-original-commerce-fixture','7','3'])
        request=commerce_current_request(opened,case='sku')
        self.assertEqual(request['modules'][0]['documentJson'].encode(),opened.model)
        self.assertEqual(request['sql'],'SELECT p.sku FROM products p')
        for source in request['opaqueSources']:
            self.assertEqual(source['sha256'],hashlib.sha256(source['utf8'].encode()).hexdigest())
        self.assertEqual(opened.__dict__,before)

    def test_wrong_home_null_and_duplicate_identity_refuse(self):
        for mutate in (lambda o:o.bindings['properties'].clear(),
                       lambda o:o.independent_expected['object_current'][0].update(props_json='{"21":null}'),
                       lambda o:o.independent_expected['object_current'].append(copy.deepcopy(o.independent_expected['object_current'][0]))):
            with self.subTest(mutate=mutate):
                opened=self.opened();mutate(opened)
                with self.assertRaises((ValueError,KeyError)):commerce_current_rows(opened)

    def test_no_imported_description_can_mint_stage_or_reach_native_effects(self):
        class Spark:
            def sql(self,*args):raise AssertionError('No native effects permitted')
        with self.assertRaises(TypeError):stage_commerce_current(object(),Spark(),object(),schema='private_stage')
        with self.assertRaises(TypeError):PublicationCurrentStage(None,None,None,'{}',(),'',())

    def test_stage_schema_is_fresh_once_and_reusable_only_by_original_hold(self):
        from unittest.mock import patch
        from ashlar_host.publication_current_stage import create_publication_stage_schema,PublicationStageSchema
        class Held:
            def __init__(self):self.context=object();self.active=True
            def require_current(self,context):
                if not self.active or context is not self.context:raise PermissionError('closed')
        class Spark:
            def __init__(self):self.statements=[]
            def sql(self,statement):
                if self.statements:raise FileExistsError('existing schema')
                self.statements.append(statement)
        held=Held();spark=Spark()
        with patch('ashlar_host.publication_current_authority.HeldPublicationCurrent',Held):
            owner=create_publication_stage_schema(held,spark,schema='fresh')
            owner.require_current(held,spark,'fresh');owner.require_current(held,spark,'fresh')
            self.assertEqual(spark.statements,['CREATE DATABASE `spark_catalog`.`fresh`'])
            with self.assertRaises(FileExistsError):create_publication_stage_schema(held,spark,schema='fresh')
            with self.assertRaises(PermissionError):owner.require_current(held,Spark(),'fresh')
            with self.assertRaises(TypeError):PublicationStageSchema(held,spark,'fresh')
