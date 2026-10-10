"""Public mechanical ports with inert executable/fixture-only proof callbacks."""
from dataclasses import dataclass,FrozenInstanceError
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch
from ashlar import _weft_installation_mechanics as m

class Refusal(ValueError):pass
def refuse():raise Refusal('fixture-refusal')

@dataclass(frozen=True)
class Installed:
    ready_bytes:bytes
    cleanup_pending:bool=False

class MechanicsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
    def script(self,body):
        path=self.root/'fake'
        if path.exists():path.chmod(0o644)
        path.write_text('#!'+sys.executable+'\n'+body);path.chmod(0o555);return path
    def transport(self,path,request=b'original',**ports):
        return m.compile_transport(path,request,refuse=refuse,
            validate_response=ports.get('validate',lambda raw:None),closing_verify=ports.get('closing',lambda:None))
    def fixture(self,profile):
        layout=m.installation_layout(profile)
        config=SimpleNamespace(output=self.root/profile,index_revision='a'*40,index_sha256='b'*64,
                               realization_id='fixture-'+profile,observed_target='fixture-target',observed_os='fixture-os')
        verified=SimpleNamespace(executable_bytes=b'fixture',provenance_bytes=b'{}',resources=tuple((name,b'{}')for name in layout.resources))
        def verify(config,raw,*,ready_present):
            self.assertFalse(ready_present);self.assertFalse((config.output/'ready.json').exists())
            self.assertEqual(json.loads(raw)['format'],layout.ready_format)
            self.assertEqual(sorted(p.relative_to(config.output).as_posix()for p in config.output.rglob('*')if p.is_file()),sorted((layout.executable,'provenance.json')+layout.resources))
            return Installed(raw)
        return layout,config,verified,verify
    def publish(self,profile='paths',**overrides):
        layout,config,verified,verify=self.fixture(profile)
        ports=dict(layout=layout,write=lambda path,raw:m.write_owned(path,raw,refuse=refuse),
                   read=lambda path,*limit:path.read_bytes(),encode=lambda value:json.dumps(value,separators=(',',':')).encode(),verify=verify,refuse=refuse)
        ports.update(overrides)
        return m.publish_installation(config,verified,**ports),config,layout
    def test_closed_frozen_layouts_no_unchecked_names(self):
        old=m.installation_layout('paths');new=m.installation_layout('paths-keys')
        self.assertEqual((old.executable,len(old.schemas)),('weft-paths',5))
        self.assertEqual((new.executable,len(new.schemas)),('weft-paths-keys',6))
        self.assertEqual(new.schemas[-1],'application-result-v0.2.schema.json')
        self.assertNotEqual(old.ready_format,new.ready_format)
        with self.assertRaises(FrozenInstanceError):old.executable='caller'
        for value in ('unknown',None,True):
            with self.assertRaises(ValueError):m.installation_layout(value)
        with self.assertRaises(ValueError):m.InstallationLayout('paths','escape','format','prefix',())
    def test_exclusive_writer_no_overwrite_and_primary_identity(self):
        target=self.root/'file';target.write_bytes(b'original')
        with self.assertRaises(FileExistsError):m.write_owned(target,b'changed',refuse=refuse)
        self.assertEqual(target.read_bytes(),b'original')
        class Cancel(KeyboardInterrupt):
            def __setattr__(self,name,value):
                if name=='cleanup_failed':raise RuntimeError('marker-refused')
                super().__setattr__(name,value)
        primary=Cancel();close=os.close
        def cleanup(fd):close(fd);raise OSError('close')
        with patch.object(m.os,'write',side_effect=primary),patch.object(m.os,'close',side_effect=cleanup):
            with self.assertRaises(Cancel)as caught:m.write_owned(self.root/'new',b'value',refuse=refuse)
        self.assertIs(caught.exception,primary)
    def test_finish_preserves_cleanup_only_baseexception_attempts_all(self):
        primary=GeneratorExit();called=[]
        def first():called.append(1);raise primary
        def second():called.append(2);raise OSError('other')
        with self.assertRaises(GeneratorExit)as caught:m.finish(None,(first,second))
        self.assertIs(caught.exception,primary);self.assertEqual(called,[1,2])
    def test_both_layouts_commit_modes_and_owned_no_clobber(self):
        for profile in ('paths','paths-keys'):
            result,config,layout=self.publish(profile)
            self.assertEqual((config.output/'ready.json').read_bytes(),result.ready_bytes)
            self.assertFalse(result.cleanup_pending)
            self.assertEqual((config.output/layout.executable).stat().st_mode&0o777,0o555)
            for name in ('provenance.json',)+layout.resources: self.assertEqual((config.output/name).stat().st_mode&0o777,0o444)
            with self.assertRaises(Refusal):self.publish(profile)
            self.assertEqual((config.output/'ready.json').read_bytes(),result.ready_bytes)
    def test_precommit_and_postcommit_failures_keep_distinct_availability(self):
        primary=KeyboardInterrupt()
        with patch.object(m.shutil,'rmtree',side_effect=OSError('cleanup')):
            with self.assertRaises(KeyboardInterrupt)as caught:self.publish(verify=lambda *a,**k:(_ for _ in ()).throw(primary))
        self.assertIs(caught.exception,primary);self.assertFalse((self.root/'paths/ready.json').exists())
        # Preserve the previous incomplete directory; a separate fresh target owns this commit.
        original=m.shutil.rmtree
        def maintenance(path):
            if Path(path).name.startswith('.ashlar-paths-keys-'):raise OSError('maintenance')
            return original(path)
        with patch.object(m.shutil,'rmtree',side_effect=maintenance):result,config,_=self.publish('paths-keys')
        self.assertTrue(result.cleanup_pending);self.assertTrue((config.output/'ready.json').is_file())
    def test_interrupted_owned_ready_link_retains_commit_in_both_layouts(self):
        link=m.os.link
        for profile in ('paths','paths-keys'):
            primary=KeyboardInterrupt();observed=[]
            def linked_then_cancel(source,target):
                link(source,target);observed.append(Path(target).read_bytes());raise primary
            with patch.object(m.os,'link',side_effect=linked_then_cancel):
                with self.assertRaises(KeyboardInterrupt) as caught:self.publish(profile)
            self.assertIs(caught.exception,primary)
            target=self.root/profile/'ready.json'
            self.assertEqual(target.read_bytes(),observed[0])
            self.assertEqual(json.loads(target.read_bytes())['format'],m.installation_layout(profile).ready_format)
            self.assertFalse(any(path.name.startswith(m.installation_layout(profile).staging_prefix) for path in self.root.iterdir()))

    def test_postlink_cancel_with_staging_cleanup_marks_original_both_layouts(self):
        link=m.os.link
        for profile in ('paths','paths-keys'):
            primary=KeyboardInterrupt()
            def linked_then_cancel(source,target):link(source,target);raise primary
            with patch.object(m.os,'link',side_effect=linked_then_cancel),patch.object(m.shutil,'rmtree',side_effect=OSError('staging-cleanup')):
                with self.assertRaises(KeyboardInterrupt) as caught:self.publish(profile)
            self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
            self.assertTrue((self.root/profile/'ready.json').is_file())

    def test_foreign_ready_inode_is_not_committed_or_deleted(self):
        for profile in ('paths','paths-keys'):
            primary=KeyboardInterrupt();foreign=[]
            def foreign_then_cancel(source,target):
                Path(target).write_bytes(b'foreign-ready');foreign.append(Path(target).stat().st_ino);raise primary
            with patch.object(m.os,'link',side_effect=foreign_then_cancel):
                with self.assertRaises(KeyboardInterrupt) as caught:self.publish(profile)
            self.assertIs(caught.exception,primary)
            target=self.root/profile/'ready.json'
            self.assertEqual(target.read_bytes(),b'foreign-ready')
            self.assertEqual(target.stat().st_ino,foreign[0])

    def test_result_construction_failure_is_precommit(self):
        with self.assertRaises(TypeError):self.publish(verify=lambda *a,**k:object())
        self.assertFalse((self.root/'paths/ready.json').exists())
    def test_original_bytes_validate_then_closing_and_no_result_on_drift(self):
        raw=b'{"fixture":"original"}\n'
        path=self.script('import sys\nassert sys.stdin.buffer.read()==b"original"\nsys.stdout.buffer.write('+repr(raw)+')\n')
        called=[]
        self.assertEqual(self.transport(path,validate=lambda b:called.append(('validate',b)),closing=lambda:called.append(('closing',))),raw)
        self.assertEqual(called,[('validate',raw),('closing',)])
        with self.assertRaises(Refusal):self.transport(path,closing=refuse)
    def test_real_pipe_setup_cancel_closes_all_and_reaps(self):
        path=self.script('import time\ntime.sleep(10)\n');primary=KeyboardInterrupt();children=[];spawn=m.subprocess.Popen
        def process(*args,**kwargs):child=spawn(*args,**kwargs);children.append(child);return child
        with patch.object(m.subprocess,'Popen',side_effect=process),patch.object(m.os,'set_blocking',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt)as caught:self.transport(path)
        self.assertIs(caught.exception,primary);self.assertIsNotNone(children[0].poll())
        self.assertTrue(all(p.closed for p in (children[0].stdin,children[0].stdout,children[0].stderr)))
    def test_real_deadline_kills_reaps_and_closes_owned_pipes(self):
        path=self.script('import time\ntime.sleep(10)\n');children=[];spawn=m.subprocess.Popen
        def process(*args,**kwargs):child=spawn(*args,**kwargs);children.append(child);return child
        with patch.object(m.subprocess,'Popen',side_effect=process),patch.object(m,'time',SimpleNamespace(monotonic=Mock(side_effect=[0,31]))):
            with self.assertRaises(Refusal):self.transport(path)
        self.assertIsNotNone(children[0].poll());self.assertTrue(all(p.closed for p in (children[0].stdin,children[0].stdout,children[0].stderr)))
    def test_cleanup_only_generatorexit_not_reduced_to_generic_error(self):
        path=self.script('print("fixture")\n');selector=m.selectors.DefaultSelector();primary=GeneratorExit()
        with patch.object(m.selectors,'DefaultSelector',return_value=selector),patch.object(selector,'close',side_effect=primary):
            with self.assertRaises(GeneratorExit)as caught:self.transport(path)
        self.assertIs(caught.exception,primary);selector.close()
    def test_success_closed_stdio_descendant_stops_heartbeat(self):
        heartbeat=self.root/'heartbeat'
        body='import os,time\npid=os.fork()\nif pid==0:\n os.close(0);os.close(1);os.close(2)\n while True:\n  open('+repr(str(heartbeat))+',"w").write(str(time.monotonic()))\n  time.sleep(0.01)\nwhile not os.path.exists('+repr(str(heartbeat))+'):time.sleep(0.01)\nprint("fixture")\n'
        path=self.script(body);self.assertEqual(self.transport(path),b'fixture\n')
        before=heartbeat.read_bytes();time.sleep(0.08);self.assertEqual(heartbeat.read_bytes(),before)
    def test_stderr_response_and_request_bounds(self):
        for body in ('import sys\nsys.stderr.write("x"*4097)\n', 'print("two\\nlines")\n'):
            with self.assertRaises(Refusal):self.transport(self.script(body))
        with patch.object(m.subprocess,'Popen',side_effect=AssertionError('no launch')):
            with self.assertRaises(Refusal):self.transport(self.root/'not-opened',b'x'*(m.PROTOCOL_LIMIT+1))

if __name__=='__main__':unittest.main()
