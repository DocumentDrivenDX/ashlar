# SPDX-License-Identifier: MIT OR Apache-2.0
"""Passive exact installation correspondence, independently composed stage fixtures."""
import copy
from dataclasses import replace
import hashlib
import json
import unittest
from unittest.mock import patch
from ashlar_host.original_medical_installation_handoff import validate_original_medical_installation_handoff
from ashlar_host.original_medical_local_configuration import parse_original_medical_local
from ashlar_host.local_physical_carriers import original_local_physical_carriers, ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
import test_original_medical_local_configuration as configuration_fixture


class Tests(unittest.TestCase):
    def setUp(self):
        fixture = configuration_fixture.OriginalMedicalConfigurationTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        self.config = parse_original_medical_local(json.dumps(fixture.configuration('publication')).encode())
        for target in self.config.targets: target.path.mkdir(parents=True)
        properties = {'ashlar.local.installation.id':self.config.installation_id,
                      'ashlar.local.registry.sha256':'a'*64, 'ashlar.local.journal.sha256':'b'*64}
        generated = original_local_physical_carriers(); native = {}
        for target in self.config.targets:
            base = dict(uuid=target.uuid, location=target.path.as_uri(),
                schema=json.loads(generated['carriers'][target.table]['schemaJson']),
                minReaderVersion=1, minWriterVersion=2)
            metadata = json.dumps(dict(profile='ashlar-local-delta-installation-commit/0.1',
                installation_id=self.config.installation_id, registry_sha256='a'*64,
                journal_sha256='b'*64, uuid=target.uuid), sort_keys=True, separators=(',', ':'))
            native[target.table] = {
                'creation':dict(base, version=0, properties={}, history=[{'version':0,'userMetadata':None}]),
                'registration':dict(base, version=1, properties=dict(properties), history=[
                    {'version':1,'operation':'SET TBLPROPERTIES','userMetadata':metadata},
                    {'version':0,'userMetadata':None}])}
        self.value = dict(profile='ashlar-original-medical-local-installation/0.1',
            installation_id=self.config.installation_id, journal=str(self.config.journal),
            targets=[dict(table='local.original_medical.'+t.table,path=str(t.path),uuid=t.uuid) for t in self.config.targets],
            capacity=dict(self.config.capacity), generated_provenance_sha256=ORIGINAL_LOCAL_PHYSICAL_PROVENANCE,
            qualification='Passive original stage correspondence only', native=native)
    def selected(self, value=None):
        raw = json.dumps(self.value if value is None else value, separators=(',', ':')).encode()
        config = replace(self.config, installation=replace(self.config.installation,
            bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        return raw, config
    def test_complete_closed_handoff_is_passive_without_native_or_journal_effects(self):
        raw, config = self.selected()
        with patch('sqlite3.connect') as connected, patch('os.open') as opened:
            selection = validate_original_medical_installation_handoff(raw, config)
        connected.assert_not_called(); opened.assert_not_called()
        self.assertEqual(selection.installation_id, config.installation_id)
        self.assertEqual(tuple(t.uuid for t in selection.targets), tuple(t.uuid for t in config.targets))
        self.assertEqual(tuple(t.table for t in selection.targets), tuple(t['table'] for t in self.value['targets']))
    def test_changed_bytes_and_selected_capacity_bound_refuse(self):
        raw, config = self.selected()
        with self.assertRaises(PermissionError): validate_original_medical_installation_handoff(raw+b' ', config)
        config = replace(config, bounds=replace(config.bounds, receipt_bytes=1024))
        with self.assertRaises(PermissionError): validate_original_medical_installation_handoff(raw, config)
    def test_mixed_targets_registration_and_history_refuse_even_when_reselected(self):
        for case in ('target','properties','history','version','schema','role'):
            value = copy.deepcopy(self.value); witness = value['native']['object_current']['registration']
            if case == 'target': value['targets'].reverse()
            if case == 'properties': witness['properties']['ashlar.local.registry.sha256'] = 'c'*64
            if case == 'history': witness['history'][0]['userMetadata'] = '{}'
            if case == 'version': witness['version'] = True
            if case == 'schema': witness['schema']['fields'][0]['nullable'] = 1
            if case == 'role': del value['native']['edge_current']
            raw, config = self.selected(value)
            with self.subTest(case=case), self.assertRaises((PermissionError,ValueError)):
                validate_original_medical_installation_handoff(raw, config)
    def test_unknown_and_duplicate_members_refuse(self):
        value = copy.deepcopy(self.value); value['authority'] = True
        with self.assertRaises(ValueError): validate_original_medical_installation_handoff(*self.selected(value))
        raw, config = self.selected(); raw = raw.replace(b'"version":0', b'"version":0,"version":0', 1)
        config = replace(config, installation=replace(config.installation, bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
        with self.assertRaises(ValueError): validate_original_medical_installation_handoff(raw, config)
