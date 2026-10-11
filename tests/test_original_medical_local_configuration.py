"""Closed medical selections do not acquire SDK, source or native authority."""
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar.medical_source import SOURCE_SHA, GRAPH_SHA, PUBLIC_SHA
from ashlar_host.original_medical_local_configuration import (
    parse_original_medical_local, OriginalMedicalInitializationConfig,
    OriginalMedicalPublicationConfig, OriginalMedicalReplayConfig, OriginalMedicalReadConfig,
    OriginalMedicalLocalSource, OriginalMedicalBounds, MedicalPinnedFile)


class OriginalMedicalConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.installation_id = '07c04255-f06b-499d-996e-cbd441d07121'
    def pin(self, name, digest='a'*64, size=1):
        return {'path':str(self.root/name),'sha256':digest,'bytes':size}
    def configuration(self, operation='initialization'):
        v = {'profile':'ashlar-original-medical-local-'+operation+'/0.1',
            'output':str(self.root/'evidence'), 'root':str(self.root/'native'),
            'installation_id':self.installation_id,
            'capacity':{'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608},
            'inputs':{'model':self.pin('model.json', SOURCE_SHA, 33236),
                      'graph':self.pin('graph.json', GRAPH_SHA, 160290),
                      'baseline':self.pin('baseline.json', PUBLIC_SHA, 2746047)},
            'producer':{'source':str(self.root/'umf'),'bun':str(self.root/'bun'),'git':str(self.root/'git'),
                'timeout_seconds':60,'maximum_output_bytes':1048576,'maximum_receipt_bytes':4194304},
            'runtime':{'python':self.pin('python',size=20000000),'python_version':'3.9.24',
                'java_home':str(self.root/'jdk'),'java':self.pin('jdk/bin/java'), 'java_version':'17.0.9',
                'jvm_config':self.pin('jdk/lib/jvm.cfg'), 'pyspark_root':str(self.root/'spark-python'),
                'spark_version':'3.5.3','delta_root':str(self.root/'delta-python'),'delta_version':'3.2.1',
                'jars':[self.pin('jars/delta-spark.jar'),self.pin('jars/delta-storage.jar')],
                'qualification':self.pin('runtime-receipt.json')},
            'source':{'host':'127.0.0.1','port':25435,'database':'medical_fixture',
                'authentication':'isolated-loopback-trust',
                'scope':{'service_schema':'ashlar_ack_medical','scope_id':'02d50efc-a97f-4139-81b8-a3c734088c21',
                    'installation_id':self.installation_id,'consumer':'medical-consumer','feed':'private-original-medical-fixture',
                    'epoch':'original-medical-v1'},
                'source_schema':'medical_source','source_signature_sha256':'b'*64,
                'writer_role':'medical_writer','reader_role':'medical_reader','ack_role':'medical_ack',
                'connect_seconds':5,'statement_ms':15000,'lock_ms':1000},
            'bounds':{'input_bytes':4194304,'rows':10000,'cell_bytes':1048576,'total_bytes':16777216,
                'stream_bytes':1048576,'receipt_bytes':1048576,'journal_bytes':8388608,
                'native_seconds':300,'whole_seconds':540,'master':'local[1]','driver_memory':'512m'}}
        if operation == 'initialization':v['schemas']=self.pin('checked-generated-schemas.json')
        else:
            v.update(installation=self.pin('initialization/installation.json'),journal=str(self.root/'native/operations.sqlite'),
                targets=[{'table':role,'path':str(self.root/'native'/role),'uuid':'00000000-0000-4000-8000-'+str(n).zfill(12)}
                    for n,role in enumerate(('object_current','edge_current','tombstone','whole_source_history','attempts','manifest'),1)])
            if operation in ('replay','read'):v.update(request=self.pin('publication/request.json'),publication=self.pin('publication/manifest.json'))
        return v
    def parse(self, value):return parse_original_medical_local(json.dumps(value,separators=(',',':')).encode())
    def test_all_four_exact_types_profiles_without_effects_or_ambient_defaults(self):
        with patch.dict(os.environ,{'PGHOSTADDR':'foreign-secret','PYTHONPATH':'foreign-sdk'},clear=False),patch('os.open')as opened:
            for op,kind in (('initialization',OriginalMedicalInitializationConfig),('publication',OriginalMedicalPublicationConfig),('replay',OriginalMedicalReplayConfig),('read',OriginalMedicalReadConfig)):
                config=self.parse(self.configuration(op));self.assertIs(type(config),kind)
                self.assertEqual(config.source.database,'medical_fixture');self.assertEqual(config.runtime.python_version,'3.9.24')
            opened.assert_not_called()
    def test_newer_selected_profile_is_not_silently_excluded_or_qualified(self):
        v=self.configuration();v['runtime'].update(python_version='3.11.11',spark_version='4.0.0',delta_version='4.0.0',java_version='21.0.12')
        config=self.parse(v);self.assertEqual(config.runtime.spark_version,'4.0.0')
        self.assertFalse(hasattr(config,'initialize'));self.assertFalse(hasattr(config.runtime,'admit'))
    def test_owning_operation_profiles_and_members_not_interchangeable(self):
        for op in ('publication','replay','read'):
            value=self.configuration(op);value['profile']=self.configuration()['profile']
            with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['invented']=True
        with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();del value['bounds']
        with self.assertRaises(ValueError):self.parse(value)
    def test_unknown_and_duplicate_nested_members_refuse(self):
        value=self.configuration();value['source']['password']='not permitted'
        with self.assertRaises(ValueError):self.parse(value)
        raw=json.dumps(self.configuration()).replace('"host": "127.0.0.1"','"host": "127.0.0.1", "host": "other"').encode()
        with self.assertRaises(ValueError):parse_original_medical_local(raw)
    def test_original_hashes_and_aggregate_input_limit(self):
        value=self.configuration();value['inputs']['model']['sha256']='0'*64
        with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['bounds']['input_bytes']=1000
        with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['inputs']['baseline']['bytes']=4194304
        with self.assertRaises(ValueError):self.parse(value)
    def test_bool_float_nonfinite_and_depth_byte_overflow(self):
        for change in (True,1.0,0,10001):
            value=self.configuration();value['bounds']['rows']=change
            with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['source']['port']=True
        with self.assertRaises(ValueError):self.parse(value)
        with self.assertRaises(ValueError):parse_original_medical_local(b'{"profile":NaN}')
        with self.assertRaises(ValueError):parse_original_medical_local(b' '*262145)
        with self.assertRaises(ValueError):parse_original_medical_local(b'['*33+b'0'+b']'*33)
    def test_no_higher_caps_or_nonlocal_native_profile(self):
        for key,wrong in (('native_seconds',301),('whole_seconds',541),('stream_bytes',1048577),('journal_bytes',134217729),('master','local[2]'),('driver_memory','1g')):
            value=self.configuration();value['bounds'][key]=wrong
            with self.assertRaises(ValueError):self.parse(value)
    def test_exact_scope_installation_and_original_epoch(self):
        value=self.configuration();value['installation_id']='older-installation'
        with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['source']['scope']['epoch']='commerce-v1'
        with self.assertRaises(ValueError):self.parse(value)
    def test_remote_endpoint_ambient_auth_fields_and_role_aliases_refuse(self):
        for key,wrong in (('host','example.com'),('authentication','password'),('database','bad; SQL'),('ack_role','medical_reader'),('connect_seconds',11)):
            value=self.configuration();value['source'][key]=wrong
            with self.assertRaises(ValueError):self.parse(value)
    def test_paths_lexical_symlink_and_output_aliases_refuse(self):
        for wrong in ('relative','/tmp/../tmp/model.json','/tmp//model.json'):
            value=self.configuration();value['inputs']['model']['path']=wrong
            with self.assertRaises(ValueError):self.parse(value)
        target=self.root/'target';target.mkdir();(self.root/'alias').symlink_to(target,target_is_directory=True)
        value=self.configuration();value['output']=str(self.root/'alias/evidence')
        with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['output']=value['root']
        with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['output']=str(self.root/'model.json/subdir')
        with self.assertRaises(ValueError):self.parse(value)
    def test_native_root_cannot_mutate_producer_or_runtime_selection(self):
        for root in ('umf/new-installation','spark-python/new-installation','delta-python/new-installation','jdk/new-installation','jars','jars/new-installation','runtime-receipt.json/native'):
            value=self.configuration();value['root']=str(self.root/root)
            with self.assertRaises(ValueError):self.parse(value)
        value=self.configuration();value['runtime']['python']['path']=str(self.root/'native/python')
        with self.assertRaises(ValueError):self.parse(value)
    def test_hardlink_and_fifo_selected_files_refuse_without_open(self):
        original=self.root/'file';original.write_bytes(b'x');alias=self.root/'hardlink';os.link(original,alias)
        with patch('os.open')as opened,self.assertRaises(ValueError):MedicalPinnedFile(alias,'a'*64,1)
        opened.assert_not_called()
        fifo=self.root/'fifo';os.mkfifo(fifo)
        with self.assertRaises(ValueError):MedicalPinnedFile(fifo,'a'*64,1)
    def test_existing_exact_six_targets_and_journal_not_latest_or_partial(self):
        for mutate in (lambda v:v['targets'].pop(),lambda v:v['targets'].reverse(),lambda v:v['targets'][5].update(uuid=v['targets'][0]['uuid']),lambda v:v['targets'][0].update(path=str(self.root/'foreign')),lambda v:v.update(journal=str(self.root/'foreign.sqlite'))):
            value=self.configuration('publication');mutate(value)
            with self.assertRaises(ValueError):self.parse(value)
    def test_replay_and_read_mandatory_retained_request_and_manifest(self):
        for op in ('replay','read'):
            value=self.configuration(op);del value['request']
            with self.assertRaises(ValueError):self.parse(value)
            value=self.configuration(op);value['publication']=value['request']
            with self.assertRaises(ValueError):self.parse(value)
    def test_runtime_closed_descriptor_and_jar_selection(self):
        for mutate in (lambda v:v['runtime']['java'].update(path=str(self.root/'foreign/java')),lambda v:v['runtime'].update(jars=[]),lambda v:v['runtime'].update(jars=[v['runtime']['jars'][0]]*2),lambda v:v['runtime']['jvm_config'].update(bytes=65537)):
            value=self.configuration();mutate(value)
            with self.assertRaises(ValueError):self.parse(value)
    def test_exact_owner_types_frozen_capacity_and_no_runtime_discovery(self):
        config=self.parse(self.configuration())
        with self.assertRaises(TypeError):config.capacity['max_intent_bytes']=1
        class ForgedSource(OriginalMedicalLocalSource):pass
        source=ForgedSource(**config.source.__dict__)
        with self.assertRaises(ValueError):replace(config,source=source)
        with self.assertRaises(ValueError):replace(config,bounds=dict(config.bounds.__dict__))
        with self.assertRaises(ValueError):MedicalPinnedFile(self.root/'empty','a'*64,0)
        self.assertEqual(config.capacity['max_intent_bytes'],8388608)

    def test_journal_budget_is_selected_without_enlarging_existing_selection(self):
        value=self.configuration()
        self.assertEqual(self.parse(value).bounds.journal_bytes,8388608)
        value['bounds']['journal_bytes']=134217728
        self.assertEqual(self.parse(value).bounds.journal_bytes,134217728)


if __name__=='__main__':unittest.main()
