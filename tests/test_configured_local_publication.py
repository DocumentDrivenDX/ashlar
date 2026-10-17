"""Real portable publisher/progress over inert native ports; no Delta execution."""
import json,re,sqlite3,tempfile,unittest,os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import test_configured_source_admission as fixtures
from ashlar.native import SQLResult
from ashlar_host.configured_source_local import open_configured_local_source
from ashlar_host.configured_local_publication import ConfiguredLocalPublicationDriver,ROLES
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaEffects
from ashlar_host.driver import PrivatePolicy
from ashlar_host.managed_carriers import load_managed_carriers

class Tests(unittest.TestCase):
    def test_actual_phases_manifest_progress_sequential_restart_and_replay(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=fixtures.Tests().fixture(root);context=object()
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),open_configured_local_source(config,stream='original-local',context=context)as source:
                tables={r:'local.configured.'+r for r in (*ROLES,'attempts','manifest')}
                targets={t:SimpleNamespace(table=t,uuid=r+'-uuid',path=root/r)for r,t in tables.items()}
                transport=object.__new__(LocalDeltaTransport);transport.targets=targets;transport.journal_path=root/'journal';transport.db=sqlite3.connect(transport.journal_path)
                self.addCleanup(transport.db.close);transport.db.execute('CREATE TABLE local_plan(operation TEXT)');policy=PrivatePolicy(context,list(targets.values()));policy.initializing=False;transport.policy=policy
                versions={t:0 for t in targets};bags={r:[]for r in ROLES};snapshots={(r,0):[]for r in ROLES};plans={};recoveries=[];interrupted=[False];phases=[];manifests=[];effects=[];calls=[];carriers=load_managed_carriers()
                def query(sql,p):
                    calls.append(sql)
                    table=next((t for t in targets if '.'.join('`'+part+'`'for part in t.split('.'))in sql),None)
                    if sql.startswith('DESCRIBE'):return SQLResult([{'id':targets[table].uuid,'location':str(targets[table].path)}])
                    if table==tables['attempts']:
                        return SQLResult([{k:row[k]for k in ('phase','request_digest','payload_json','payload_digest')}for row in phases if row['stream']==p['stream']and row['batch_id']==p['batch']])
                    if table==tables['manifest']:return SQLResult([dict(row)for row in manifests if row['publication_id']==p.get('id',p.get('publication_id'))])
                    role=table.rsplit('.',1)[1]
                    if 'LIMIT 0'in sql:return SQLResult([],carriers.ordered_columns(role))
                    version=int(re.search(r'VERSION AS OF ([0-9]+)',sql)[1]);return SQLResult(snapshots[(role,version)])
                def mutation(operation,sql,p,**kw):
                    if 'payload'in p:phases.append(json.loads(p['payload']))
                    elif 'row'in p:manifests.append(json.loads(p['row']))
                    else:raise AssertionError('Unexpected mutation port')
                    return SQLResult([])
                transport.query=query;transport.mutation=mutation;transport.prepare_mutation=lambda *a,**k:None;transport.original_history=lambda target:[{'version':versions[target.table]}]
                current=[None]
                def run(self,operation,digest,steps,**kw):
                    effects.append(operation);d=current[0]
                    for role in ROLES:
                        bags[role]=d.expected[role];versions[tables[role]]+=1;snapshots[(role,versions[tables[role]])]=bags[role]
                    if d.ordinal==2 and not interrupted[0]:interrupted[0]=True;raise RuntimeError('Original effects finished; response lost')
                    return {'operation':operation,'qualification':'inert effect port'}
                def prepare(self,operation,digest,steps,**kw):
                    value=json.dumps(steps,sort_keys=True)
                    if operation in plans:selftest.assertEqual(plans[operation],value)
                    plans[operation]=value
                selftest=self
                def observe(self,operation,digest,steps,**kw):selftest.assertEqual(plans[operation],json.dumps(steps,sort_keys=True))
                def recover(self,operation,digest,steps,**kw):
                    observe(self,operation,digest,steps,**kw);recoveries.append(operation)
                    return {'operation':operation,'qualification':'inert original recovery'}
                def admit(source_arg,request,manifest,supplied):self.assertIs(source_arg,source);self.assertIs(supplied,context)
                with patch.object(LocalDeltaEffects,'run',run),patch.object(LocalDeltaEffects,'prepare',prepare),patch.object(LocalDeltaEffects,'observe',observe),patch.object(LocalDeltaEffects,'recover',recover):
                    for ordinal in (1,2,3):
                        d=ConfiguredLocalPublicationDriver(transport,policy,source,tables=tables,context=context,materialization_clock_us='1791547200000000',current_admission=admit);current[0]=d
                        d.select_original(ordinal)
                        if ordinal==2:
                            with self.assertRaises(RuntimeError):d.publish_selected()
                            self.assertEqual(d.progress.completed_batches(),1)
                            # New actual driver resumes the retained applying
                            # phase and original plan, despite advanced heads.
                            d=ConfiguredLocalPublicationDriver(transport,policy,source,tables=tables,context=context,materialization_clock_us='1791547200000000',current_admission=admit);current[0]=d;d.select_original(ordinal)
                        descriptor=d.publish_selected()
                        self.assertEqual(d.progress.completed_batches(),ordinal);self.assertEqual(descriptor.publication_id,source.spec(ordinal,context).publication_id)
                    self.assertEqual(recoveries,[effects[1]]);self.assertEqual(len(effects),3);self.assertEqual(len(phases),15);self.assertEqual(len(manifests),3)
                    self.assertEqual(len(bags['object_current']),1);self.assertEqual(len(bags['tombstone']),1)
                    replay=ConfiguredLocalPublicationDriver(transport,policy,source,tables=tables,context=context,materialization_clock_us='1791547200000000',current_admission=admit);current[0]=replay
                    replay.select_original(3);self.assertEqual(replay.publish_selected(),descriptor)
                    self.assertEqual(recoveries,[effects[1]]);self.assertEqual(len(effects),3);self.assertEqual(len(phases),15);self.assertEqual(len(manifests),3)
                    wrong=ConfiguredLocalPublicationDriver(transport,policy,source,tables=tables,context=context,materialization_clock_us='1791547200000001',current_admission=admit)
                    with self.assertRaises(ValueError):wrong.select_original(3)
                    self.assertEqual(len(effects),3)

    def test_writer_opening_cancellation_always_closes_actual_lock_fd(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            d=object.__new__(ConfiguredLocalPublicationDriver);d.context=object();d.held=False;d.request={'original':'request'}
            d.source=SimpleNamespace(stream='original');d.transport=SimpleNamespace(journal_path=root/'journal')
            cancel=KeyboardInterrupt();closed=[];real_close=os.close
            def close(fd):closed.append(fd);real_close(fd)
            with patch('ashlar_host.configured_local_publication.os.fstat',side_effect=cancel),patch('ashlar_host.configured_local_publication.os.close',side_effect=close):
                with self.assertRaises(KeyboardInterrupt)as raised:
                    with d.writer('original',d.context):self.fail('Unadmitted body')
            self.assertIs(raised.exception,cancel);self.assertEqual(len(closed),1);self.assertFalse(d.held)
