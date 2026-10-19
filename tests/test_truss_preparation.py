# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small public-carrier host controls; no actual Truss/native qualification."""
from dataclasses import dataclass, replace
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace, ModuleType
from unittest.mock import patch
import zipfile
from ashlar_host.truss_preparation_configuration import (PinnedFile,ImportRoot,PreparationDocument,
    TrussPreparationLimits,TrussPreparationConfig,parse_truss_preparation_configuration,decode)
from ashlar_host.truss_preparation import (capture_preparation,json_bytes,prepare_installed_views,
    emit_preparation_transport,parse_preparation_transport,run_truss_preparation_command,
    verify_wheel_correspondence)


@dataclass(frozen=True)
class CatalogDocument:
    document_id: str
    document_revision: str
    content: bytes
    artifact_identity: object=None

@dataclass(frozen=True)
class DocumentInterpretation:
    document: CatalogDocument
    observation_bytes: bytes

@dataclass(frozen=True)
class PreparationProvenance:
    owner_revision: str='original-owner'
    owner_bundle_sha256: str='complete-original-bundle'
    acceptance_schema_sha256: str='original-schema'
    release_manifest_sha256: str='original-release'
    runtime_version: str='1.4.2'
    scope: str='original_umf_preparation_only'
    installation_profiles_verified: bool=False

@dataclass(frozen=True)
class PreparedAcceptance:
    input_bytes: bytes
    documents: tuple
    declarations_bytes: bytes
    archive_documents_bytes: bytes
    provenance: PreparationProvenance
    report_evidence_bytes: bytes

class PreparationRejected(ValueError):
    def __init__(self,reason,diagnostics=()):self.reason=reason;self.diagnostics=diagnostics


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve();os.chmod(self.root,0o700)
        self.site=self.root/'site';self.site.mkdir();(self.site/'truss').mkdir()
        self.origin=self.site/'truss/__init__.py';self.origin.write_bytes(b'# original wheel payload\n')
        self.original=b' \n{"umf":"0.7.0","id":"original","unknown":{"preserve":"\\u96ea"}}\n'
        self.model=self.root/'original.umf.json';self.model.write_bytes(self.original)
        self.cfg=self.root/'choices.json';self.cfg.write_bytes(' \n{"original": "unverified-profile", "unknown": "雪"}\n'.encode())
        self.python=self.root/'python';self.python.write_bytes(b'explicit-python');self.bun=self.root/'bun';self.bun.write_bytes(b'explicit-bun')
        self.wheel=self.root/'truss.whl';self.build_wheel()
        self.config=TrussPreparationConfig((PreparationDocument('original','r1',None,self.pin(self.model)),),self.pin(self.cfg),self.pin(self.python),self.pin(self.bun),'3.11.13','1.4.2',(ImportRoot(self.site,(self.pin(self.origin),),('truss',)),),'truss-toolkit','0.0.1.dev0','a'*40,self.pin(self.wheel),self.origin,self.root/'result',TrussPreparationLimits(1048576,1048576,1048576,65536,2))
        self.calls=[];self.cancel=None
        self.public=ModuleType('truss');self.public.__file__=str(self.origin)
        for name in ('CatalogDocument','DocumentInterpretation','PreparedAcceptance','PreparationProvenance','PreparationRejected'):setattr(self.public,name,globals()[name])
        self.public.prepare_acceptance=self.prepare
        self.oldpath=list(sys.path);self.addCleanup(lambda:sys.path.__setitem__(slice(None),self.oldpath))

    def pin(self,path):return PinnedFile(path,path.stat().st_size,hashlib.sha256(path.read_bytes()).hexdigest())

    def build_wheel(self,changed=False):
        members={**({'truss/py.typed':b''}if(self.site/'truss/py.typed').exists()else{}),'truss/__init__.py':b'changed'if changed else self.origin.read_bytes(),'truss_toolkit-0.0.1.dev0.dist-info/METADATA':b'Name: truss-toolkit\nVersion: 0.0.1.dev0\n'}
        rows=[]
        for name,raw in members.items():rows.append('%s,sha256=%s,%s\n'%(name,base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode(),len(raw)))
        rows.append('truss_toolkit-0.0.1.dev0.dist-info/RECORD,,\n')
        with zipfile.ZipFile(self.wheel,'w')as z:
            for name,raw in members.items():z.writestr(name,raw)
            z.writestr('truss_toolkit-0.0.1.dev0.dist-info/RECORD',''.join(rows))

    def prepare(self,documents,configuration,*,bun_executable):
        self.calls.append((documents,configuration,bun_executable))
        if self.cancel is not None:raise self.cancel
        return PreparedAcceptance(b'{"prepared-input":"original"}',tuple(DocumentInterpretation(d,b'{"complete":false,"unavailable":["owner-op"],"unknown":{"retained":true}}')for d in documents),b'["original-declarations"]',b'["original-archives"]',PreparationProvenance(),b'{"all":"report-evidence","unavailable":true}')

    def public_context(self):
        from contextlib import ExitStack
        stack=ExitStack();stack.enter_context(patch.dict(sys.modules,{'truss':self.public}));stack.enter_context(patch('ashlar_host.truss_preparation.sys.executable',str(self.python)));stack.enter_context(patch('ashlar_host.truss_preparation.sys.version_info',(3,11,13)))
        stack.enter_context(patch('ashlar_host.truss_preparation.importlib.metadata.distribution',return_value=SimpleNamespace(version=self.config.version,locate_file=lambda name:self.site)))
        return stack

    def views(self):
        with self.public_context():return prepare_installed_views(self.config)

    def wire(self):
        metadata,files=self.views();stream=io.BytesIO();emit_preparation_transport(self.config,metadata,files,stream);return stream.getvalue()

    def configuration(self):
        def pin(f):return {'path':str(f.path),'bytes':f.bytes,'sha256':f.sha256}
        c=self.config
        return {'profile':'ashlar-truss-preparation/0.1','documents':[{'document_id':d.document_id,'document_revision':d.document_revision,'artifact_identity':d.artifact_identity,'source':pin(d.source)}for d in c.documents],'configuration':pin(c.configuration),'runtime':{'python':pin(c.python),'bun':pin(c.bun),'python_version':c.python_version,'bun_version':c.bun_version,'import_roots':[{'path':str(r.path),'files':[pin(f)for f in r.files],'directories':list(r.directories)}for r in c.import_roots]},'package':{'distribution':c.distribution,'version':c.version,'source_revision':c.source_revision,'wheel':pin(c.wheel),'module_origin':str(c.module_origin)},'output':str(c.output),'limits':{k:getattr(c.limits,k)for k in ('input_bytes','capture_bytes','output_bytes','receipt_bytes','deadline_seconds')}}

    def test_exact_closed_configuration_and_portable_import(self):
        result=parse_truss_preparation_configuration(json.dumps(self.configuration()).encode())
        self.assertEqual(result.documents[0].source.sha256,self.config.documents[0].source.sha256)
        for change in (lambda v:v.update(extra=True),lambda v:v['limits'].update(deadline_seconds=True),lambda v:v['limits'].update(deadline_seconds=2.5),lambda v:v.update(profile='other')):
            v=self.configuration();change(v)
            with self.assertRaises((ValueError,TypeError)):parse_truss_preparation_configuration(json.dumps(v).encode())
        with self.assertRaises(ValueError):decode(b'{"a":1,"a":2}')
        with self.assertRaises(ValueError):decode(b'['*13+b'0'+b']'*13)

    def test_hardlink_and_initial_command_replacement_refuse_before_effects(self):
        alias=self.root/'alias';os.link(self.model,alias)
        with self.assertRaises(ValueError):self.pin(alias)
        with self.assertRaises(ValueError):self.config.documents[0].source.renew()
        alias.unlink()
        command=self.root/'command.json';command.write_bytes(json.dumps(self.configuration()).encode())
        real_read=os.read;replaced=False
        def read(fd,n):
            nonlocal replaced
            raw=real_read(fd,n)
            if not replaced:
                replaced=True;command.rename(self.root/'original-command.json');command.write_bytes(raw)
            return raw
        with patch('ashlar_host.truss_preparation.os.read',side_effect=read),patch('ashlar_host.truss_preparation.capture_preparation')as capture,self.assertRaises(ValueError):run_truss_preparation_command(command)
        capture.assert_not_called();self.assertFalse(self.config.output.exists())

    def test_complete_wheel_zero_byte_resource_and_nonempty_roles(self):
        marker=self.site/'truss/py.typed';marker.write_bytes(b'')
        resource=self.pin(marker);self.assertEqual(resource.read(1),b'');resource.renew()
        self.build_wheel()
        config=replace(self.config,wheel=self.pin(self.wheel),import_roots=(ImportRoot(self.site,(self.pin(self.origin),resource),('truss',)),))
        verify_wheel_correspondence(config)
        for role in ('configuration','python','bun','wheel'):
            with self.assertRaises(ValueError):replace(config,**{role:resource})
        with self.assertRaises(ValueError):PreparationDocument('original','r1',None,resource)
        for size in (-1,True,0.0):
            with self.assertRaises(ValueError):PinnedFile(marker,size,resource.sha256)

    def test_request_astral_identifier_bound_precedes_public_call(self):
        document=replace(self.config.documents[0],document_id='😀'*100)
        # ASCII JSON needs twelve bytes per supplementary character.
        self.config=replace(self.config,documents=(document,),limits=replace(self.config.limits,input_bytes=2400))
        with self.public_context(),self.assertRaises(ValueError):prepare_installed_views(self.config)
        self.assertEqual(self.calls,[])

    def test_public_call_preserves_bytes_order_and_all_views(self):
        metadata,files=self.views();documents,configuration,bun=self.calls[0]
        self.assertEqual(documents,(CatalogDocument('original','r1',self.original,None),));self.assertEqual(configuration,self.cfg.read_bytes());self.assertEqual(bun,str(self.bun))
        self.assertEqual(dict(files)['document-0000.umf.json'],self.original)
        self.assertIn(b'"unavailable"',dict(files)['observation-0000.json']);self.assertIn(b'"unknown"',dict(files)['observation-0000.json'])
        self.assertEqual(len(files),7);self.assertEqual(dict(files)['original-configuration.json'],self.cfg.read_bytes());self.assertFalse(metadata['provenance']['installation_profiles_verified'])
        raw=self.wire();header,returned=parse_preparation_transport(self.config,raw)
        self.assertEqual({n:bytes(v)for n,v in returned},dict(files));self.assertEqual(header['status'],'prepared')

    def test_owner_unsupported_refusal_preserves_complete_diagnostics(self):
        self.public.prepare_acceptance=lambda *a,**k:(_ for _ in ()).throw(PreparationRejected('unsupported_profile',(b'{"unknown":"unchanged"}',)))
        metadata,files=self.views();self.assertEqual(metadata['status'],'refused');self.assertEqual(files,[('original-configuration.json',self.cfg.read_bytes()),('refusal-0000.json',b'{"unknown":"unchanged"}')]);out=io.BytesIO();emit_preparation_transport(self.config,metadata,files,out);self.assertEqual({n:bytes(v)for n,v in parse_preparation_transport(self.config,out.getvalue())[1]},dict(files))

    def test_aggregate_output_and_encoded_transport_overflow_before_write(self):
        metadata,files=self.views();config=replace(self.config,limits=replace(self.config.limits,output_bytes=65536,capture_bytes=65536))
        out=io.BytesIO()
        with self.assertRaises(ValueError):emit_preparation_transport(config,metadata,[('a',b'a'*40000),('b',b'b'*40000)],out)
        self.assertEqual(out.getvalue(),b'')
        with self.assertRaises(ValueError):json_bytes({'unicode':'雪'*500},100)

    def test_wheel_record_and_installed_namespace_correspondence(self):
        verify_wheel_correspondence(self.config)
        self.build_wheel(changed=True);config=replace(self.config,wheel=self.pin(self.wheel))
        with self.assertRaises(ValueError):verify_wheel_correspondence(config)
        (self.site/'foreign.pth').write_bytes(b'foreign')
        with self.assertRaises(ValueError):self.config.import_roots[0].renew()

    def test_missing_view_digest_document_drift_and_trailing_data_refuse(self):
        raw=self.wire()
        for changed in (raw+b'extra',raw[:-1],raw[:-1]+b'x'):
            with self.assertRaises(ValueError):parse_preparation_transport(self.config,changed)
        metadata,files=self.views();foreign=[(n,b'{}'if n=='original-configuration.json'else v)for n,v in files]
        out=io.BytesIO();emit_preparation_transport(self.config,metadata,foreign,out)
        with self.assertRaises(ValueError):parse_preparation_transport(self.config,out.getvalue())
        self.model.write_bytes(self.original+b' ')
        with self.assertRaises(ValueError):self.config.renew()

    def test_public_error_and_closing_cancel_primary_identity(self):
        primary=KeyboardInterrupt('privacy sentinel');self.cancel=primary
        with self.public_context(),patch.object(TrussPreparationConfig,'renew',side_effect=[None,OSError('secondary')]):
            with self.assertRaises(KeyboardInterrupt)as observed:prepare_installed_views(self.config)
        self.assertIs(observed.exception,primary)

    def test_stream_short_write_and_zero_write(self):
        metadata,files=self.views()
        class Short(io.BytesIO):
            def write(self,data):return super().write(data[:3])
        stream=Short();emit_preparation_transport(self.config,metadata,files,stream);self.assertEqual(parse_preparation_transport(self.config,stream.getvalue())[0]['status'],'prepared')
        with self.assertRaises(ValueError):emit_preparation_transport(self.config,metadata,files,SimpleNamespace(write=lambda data:0,flush=lambda:None))

    def test_selected_origin_runtime_and_configuration_before_public_call(self):
        for label in ('origin','version','interpreter'):
            with self.subTest(label=label),self.public_context():
                self.calls.clear()
                if label=='origin':
                    original=self.public.__file__;self.public.__file__=str(self.site/'foreign.py')
                elif label=='version':
                    patcher=patch('ashlar_host.truss_preparation.importlib.metadata.distribution',return_value=SimpleNamespace(version='foreign',locate_file=lambda name:self.site));patcher.start()
                else:
                    patcher=patch('ashlar_host.truss_preparation.sys.version_info',(3,12,1));patcher.start()
                try:
                    with self.assertRaises(ValueError):prepare_installed_views(self.config)
                    self.assertEqual(self.calls,[])
                finally:
                    if label=='origin':self.public.__file__=original
                    else:patcher.stop()

    def test_parent_substitution_preserves_foreign_and_withholds_receipt(self):
        # The private parent is replaced only after the actual child acquisition
        # point; held original directory descriptors cannot adopt its successor.
        parent=self.root/'private';parent.mkdir(mode=0o700)
        self.config=replace(self.config,output=parent/'result')
        raw=self.wire();p=self.root/'command.json';p.write_bytes(json.dumps(self.configuration()).encode())
        retired=self.root/'retired'
        def child(*args,**kwargs):
            parent.rename(retired);parent.mkdir(mode=0o700);(parent/'foreign').write_bytes(b'preserve')
            return raw,b''
        with patch('ashlar_host.truss_preparation.capture_preparation',side_effect=child):
            with self.assertRaises(ValueError):run_truss_preparation_command(p)
        self.assertEqual((parent/'foreign').read_bytes(),b'preserve')
        self.assertFalse((parent/'result').exists());self.assertFalse((retired/'result/receipt.json').exists())

    def test_command_cancel_keeps_primary_and_attempts_closing(self):
        p=self.root/'command.json';p.write_bytes(json.dumps(self.configuration()).encode());primary=GeneratorExit('private sentinel');seen=[]
        original=TrussPreparationConfig.renew
        def renew(config):
            seen.append('renew')
            if len(seen)>1:raise OSError('closing failure')
            return original(config)
        with patch('ashlar_host.truss_preparation.capture_preparation',side_effect=primary),patch.object(TrussPreparationConfig,'renew',renew):
            with self.assertRaises(GeneratorExit)as observed:run_truss_preparation_command(p)
        self.assertIs(observed.exception,primary);self.assertEqual(len(seen),2)
        self.assertFalse((self.config.output/'receipt.json').exists())

    def test_real_stdlib_capture_combined_bound_deadline_and_reap(self):
        python=str(Path(sys.executable).resolve());environment={'PYTHONDONTWRITEBYTECODE':'1'}
        raw,err=capture_preparation([python,'-I','-B','-S','-c','import os;os.write(1,b"ok");os.write(2,b"log")'],cwd=self.root,environment=environment,deadline_seconds=2,capture_bytes=32)
        self.assertEqual((raw,err),(b'ok',b'log'))
        with self.assertRaises(ValueError):capture_preparation([python,'-I','-B','-S','-c','import os;os.write(1,b"1234");os.write(2,b"5678")'],cwd=self.root,environment=environment,deadline_seconds=2,capture_bytes=7)
        with self.assertRaises(TimeoutError):capture_preparation([python,'-I','-B','-S','-c','import time;time.sleep(5)'],cwd=self.root,environment=environment,deadline_seconds=1,capture_bytes=32)

    def test_selector_acquisition_failure_reaps_real_child_preserves_primary(self):
        import ashlar_host.truss_preparation as owner
        primary=KeyboardInterrupt('selector acquisition');created=[];original=owner.subprocess.Popen
        def spawn(*args,**kwargs):
            p=original(*args,**kwargs);created.append(p);return p
        with patch.object(owner.subprocess,'Popen',side_effect=spawn),patch.object(owner.selectors,'DefaultSelector',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt)as observed:capture_preparation([str(Path(sys.executable).resolve()),'-I','-B','-S','-c','import time;time.sleep(5)'],cwd=self.root,environment={},deadline_seconds=2,capture_bytes=32)
        self.assertIs(observed.exception,primary);self.assertIsNotNone(created[0].poll());self.assertTrue(created[0].stdout.closed);self.assertTrue(created[0].stderr.closed)

    def test_command_fresh_release_no_clobber_and_closing_drift_withholds_receipt(self):
        raw=self.wire();p=self.root/'command.json';p.write_bytes(json.dumps(self.configuration()).encode())
        with patch('ashlar_host.truss_preparation.capture_preparation',return_value=(raw,b'')):
            result=run_truss_preparation_command(p)
        self.assertEqual(result['status'],'prepared');self.assertEqual((self.config.output/'document-0000.umf.json').read_bytes(),self.original)
        with patch('ashlar_host.truss_preparation.capture_preparation')as capture:
            with self.assertRaises(FileExistsError):run_truss_preparation_command(p)
            capture.assert_not_called()
        # A fresh second output, changed source only after child transport.
        config=replace(self.config,output=self.root/'drift');self.config=config;p.write_bytes(json.dumps(self.configuration()).encode())
        def child(*args,**kwargs):self.model.write_bytes(self.original+b' ');return raw,b''
        with patch('ashlar_host.truss_preparation.capture_preparation',side_effect=child):
            with self.assertRaises(ValueError):run_truss_preparation_command(p)
        self.assertFalse((config.output/'receipt.json').exists())


if __name__=='__main__':unittest.main()
