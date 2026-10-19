# SPDX-License-Identifier: MIT OR Apache-2.0
"""Canonical package/source sync and retained installation correspondence only."""
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
import copy,hashlib,json,unittest
from ashlar_host.managed_carriers import ManagedCarriers,TABLES,PREFIX,GENERATED_SHA256,load_managed_carriers,validate_managed_installation
ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def test_exact_ten_canonical_files_and_original_ordered_fields(self):
        value=load_managed_carriers();original=(ROOT/'sql/ashlar-delta-v03/runtime-carriers.generated.json').read_bytes()
        self.assertEqual(value.generated_bytes,original);self.assertEqual(hashlib.sha256(original).hexdigest(),GENERATED_SHA256)
        self.assertEqual(len(list(files('ashlar_host').joinpath('resources','managed-delta-v03').iterdir())),10)
        generated=json.loads(original)
        for name,raw in value.inputs:self.assertEqual(raw,(ROOT/(PREFIX+name)).read_bytes())
        for role in TABLES:
            self.assertEqual(value.ordered_fields(role),tuple((c['name'],{'string':'STRING','long':'BIGINT','boolean':'BOOLEAN','timestamp':'TIMESTAMP'}[c['deltaType']],c['nullable'])for c in generated['carriers'][role]['columns']))
        self.assertEqual(dict(value.generator),generated['generator'])
        self.assertIn('resources/managed-delta-v03/*.json',(ROOT/'pyproject.toml').read_text())
        with self.assertRaises(ValueError):replace(value,generated_bytes=original+b' ')
        altered=list(value.inputs);altered[0]=(altered[0][0],altered[0][1]+b' ')
        with self.assertRaises(ValueError):replace(value,inputs=tuple(altered))
    def test_original_installation_and_changed_provenance_or_order_refuse(self):
        carriers=load_managed_carriers();generated=json.loads(carriers.generated_bytes);namespace='private.managed'
        installation={'namespace':namespace,'state':'installed','authenticated_owner':'original-owner','generated_carriers_sha256':GENERATED_SHA256,'umf_generator':generated['generator'],'model_inputs':generated['inputs'],
            'tables':{namespace+'.'+role:{'uuid':f'00000000-0000-0000-0000-{i:012d}','columns':[list(c)for c in carriers.ordered_columns(role)]}for i,role in enumerate(TABLES,1)}}
        self.assertEqual(validate_managed_installation(installation,carriers),namespace)
        for kind in ('provenance','columns','local-profile','uuid'):
            changed=copy.deepcopy(installation)
            if kind=='provenance':changed['umf_generator']['revision']='0'*40
            elif kind=='columns':changed['tables'][namespace+'.object_current']['columns'].reverse()
            elif kind=='local-profile':changed['tables'].pop(namespace+'.source_record')
            else:changed['tables'][namespace+'.edge_current']['uuid']=changed['tables'][namespace+'.object_current']['uuid']
            with self.subTest(kind=kind),self.assertRaises(ValueError):validate_managed_installation(changed,carriers)

if __name__=='__main__':unittest.main()
