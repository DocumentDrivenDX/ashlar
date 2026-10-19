# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert SDK metadata controls; no native query or authority evidence."""
import copy
import dataclasses
import json
import types
import unittest
from ashlar_host.databricks_current_metadata import inspect_databricks_current_metadata


class MetadataTests(unittest.TestCase):
    def workspace(self):
        self.calls=[]
        self.data={'full_name':'catalog.schema.objects','table_id':'uc-id',
            'table_type':'MANAGED','data_source_format':'DELTA','browse_only':False,'columns':[
             {'position':0,'name':'key','type_name':'STRING','type_json':'{"name":"key","nullable":false,"type":"string"}',
              'nullable':False,'collation':'UTF8_BINARY'}]}
        def me():self.calls.append('me');return types.SimpleNamespace(id='stable-id',user_name='email-not-identity')
        def get(**kwargs):self.calls.append(kwargs);return types.SimpleNamespace(as_dict=lambda:copy.deepcopy(self.data))
        return types.SimpleNamespace(config=types.SimpleNamespace(host='https://workspace.example'),current_user=types.SimpleNamespace(me=me),tables=types.SimpleNamespace(get=get))
    def test_current_facts_are_detached_and_uc_identity_is_distinct(self):
        w=self.workspace();facts=inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')
        self.assertEqual(self.calls,['me',{'full_name':'catalog.schema.objects','include_browse':False},'me'])
        self.assertEqual(facts.workspace_host,'https://workspace.example');self.assertEqual(facts.principal_id,'stable-id');self.assertEqual(facts.uc_table_id,'uc-id')
        self.assertFalse(hasattr(facts,'delta_table_id'));self.assertFalse(hasattr(facts,'authority'))
        self.data['columns'][0]['name']='changed';self.assertEqual(facts.columns[0].name,'key')
        self.assertEqual(json.loads(facts.original_selected_metadata_json)['columns'][0]['type_json'],'{"name":"key","nullable":false,"type":"string"}')
        with self.assertRaises(dataclasses.FrozenInstanceError):facts.uc_table_id='other'
    def test_mismatch_and_native_refusal_have_no_fallback(self):
        for key,value in [('table_id','delta-uuid'),('table_type','EXTERNAL'),('data_source_format','PARQUET')]:
            w=self.workspace();self.data[key]=value
            with self.assertRaises(ValueError):inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')
        w=self.workspace()
        def refuse(**kwargs):raise PermissionError('native refusal')
        w.tables.get=refuse
        with self.assertRaises(PermissionError):inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')
        w=self.workspace();self.data['columns'][0]['nullable']=1
        with self.assertRaises(ValueError):inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')

    def test_host_scope_and_complete_original_column_metadata(self):
        w=self.workspace();self.data['columns'][0]['type_text']='STRING COLLATE UTF8_BINARY'
        facts=inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')
        self.assertEqual(json.loads(facts.original_selected_metadata_json)['columns'][0]['type_text'],'STRING COLLATE UTF8_BINARY')
        w=self.workspace();w.config.host='https://user:secret@workspace.example'
        with self.assertRaises(ValueError):inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')
        self.assertEqual(self.calls,[])

    def test_current_principal_drift_refuses_observation(self):
        w=self.workspace();calls=[0]
        def me():
            calls[0]+=1
            return types.SimpleNamespace(id='first' if calls[0]==1 else 'second')
        w.current_user.me=me
        with self.assertRaises(ValueError):inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')

    def test_native_metadata_closed_correspondence(self):
        for kind in ('browse','casefold','duplicate','nonfinite','float','name','nullable','missing-type'):
            w=self.workspace()
            if kind=='browse':self.data.pop('browse_only')
            elif kind=='casefold':
                second=copy.deepcopy(self.data['columns'][0]);second.update(position=1,name='KEY',type_json='{"name":"KEY","nullable":false,"type":"string"}');self.data['columns'].append(second)
            else:
                self.data['columns'][0]['type_json']={
                    'duplicate':'{"name":"key","name":"key","nullable":false}',
                    'nonfinite':'{"name":"key","nullable":false,"extra":NaN}',
                    'float':'{"name":"key","nullable":false,"extra":1e999}',
                    'name':'{"name":"other","nullable":false}',
                    'nullable':'{"name":"key","nullable":true}',
                    'missing-type':'{"name":"key","nullable":false}'}[kind]
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                inspect_databricks_current_metadata(w,relation=('catalog','schema','objects'),expected_uc_table_id='uc-id')
