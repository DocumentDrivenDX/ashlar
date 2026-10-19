# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual core read/phase/progress custody over inert native SQL only."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,re,sqlite3,tempfile,unittest
from unittest.mock import patch
import test_configured_source_admission as fixtures
from ashlar.native import SQLResult
from ashlar.publication import descriptor_from_manifest_row
from ashlar.attempt_store import PHASES
from ashlar_host.configured_source_local import open_configured_local_source
from ashlar_host.configured_local_read import read_configured_local_singleton,open_configured_local_read,ConfiguredLocalRead
from ashlar_host.file_progress import JournaledJsonlProgress
from ashlar_host.delta_custody import LocalDeltaTransport,encoded,sha
from ashlar_host.managed_carriers import load_managed_carriers
from ashlar_host.managed_source_oracle import configured_prefix_carrier_rows

class Tests(unittest.TestCase):
    def test_old_and_deleted_singleton_full_core_read_and_no_writes(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=fixtures.Tests().fixture(root);context=object()
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),open_configured_local_source(config,stream='original',context=context)as source:
                clock='1791547200000000';carriers=load_managed_carriers();roles=('object_current','edge_current','tombstone','whole_source_history');tables={r:'local.original.'+r for r in (*roles,'manifest','attempts')}
                t=object.__new__(LocalDeltaTransport);t.journal_path=root/'journal';t.installation_id='original';t.db=sqlite3.connect(t.journal_path);self.addCleanup(t.db.close)
                t.targets={table:SimpleNamespace(table=table,uuid=role+'-uuid',path=root/role)for role,table in tables.items()}
                (root/'.configured-source-writer.lock').write_bytes(b'')
                snapshots={};manifests={};phases={};requests={};calls=[]
                for n in (1,2,3):
                    spec=source.spec(n,context);request=spec.request();requests[n]=request
                    expected=configured_prefix_carrier_rows(source.configuration,source.batches[:n],materialization_clock_us=clock,carriers=carriers).rows();snapshots[n]=expected
                    row={'publication_id':spec.publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':encoded({tables[r]:n for r in roles}),'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':encoded({source.configuration.feed:json.loads(request['source_checkpoint_json'])}),'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],'source_oracle_sha256':sha(encoded(expected)),'source_admission':source.metadata(context)}),'recorded_at':clock}
                    manifests[spec.publication_id]=row;artifact=encoded({'effects':{'scope':'inert fixture port'},'manifest':row});records=[]
                    for i,phase in enumerate(PHASES):
                        payload=encoded({'request':request,'result_json':None if i<2 else artifact,'descriptor_json':encoded(row)if i==4 else None})
                        records.append({'phase':phase,'request_digest':request['request_digest'],'payload_json':payload,'payload_digest':sha(payload)})
                    phases[request['batch_id']]=records
                @contextmanager
                def resolver(descriptor,ctx):self.assertIs(ctx,context);yield descriptor
                progress=JournaledJsonlProgress(t,source.configuration.source,source.metadata(context)['source_sha256'],SimpleNamespace(admit=lambda *a:None),resolver,stream=source.stream,feed=source.configuration.feed,epoch=source.configuration.epoch)
                for n in (1,2,3):progress.acknowledge(requests[n],descriptor_from_manifest_row(manifests[source.spec(n,context).publication_id]),context)
                reader=JournaledJsonlProgress.open_existing(t,source.configuration.source,source.metadata(context)['source_sha256'],stream=source.stream,feed=source.configuration.feed,epoch=source.configuration.epoch)
                def query(sql,p):
                    calls.append(sql);self.assertTrue(sql.startswith(('SELECT ','DESCRIBE ','SHOW ')))
                    table=next((table for table in t.targets if '.'.join('`'+x+'`'for x in table.split('.'))in sql),None)
                    if sql.startswith('DESCRIBE'):return SQLResult([{'id':t.targets[table].uuid}])
                    role=table.rsplit('.',1)[1]
                    if role=='manifest':return SQLResult([manifests[p['publication_id']]])
                    if role=='attempts':return SQLResult(phases[p['batch']])
                    if 'LIMIT 0'in sql:return SQLResult([],carriers.ordered_columns(role))
                    n=int(re.search('VERSION AS OF ([0-9]+)',sql)[1]);rows=snapshots[n][role]
                    if 'LIMIT 2'in sql:rows=[r for r in rows if (r['source_system'],r['type_id'],r['id'])==(p['source'],p['type'],p['id'])]
                    return SQLResult(rows)
                t.query=query;admissions=[]
                def current(s,request,manifest,ctx):self.assertIs(s,source);self.assertIs(ctx,context);admissions.append(manifest)
                kwargs=dict(type_id=next(iter(source.configuration.policy.types)),entity_id=2,tables=tables,context=context,materialization_clock_us=clock,current_admission=current)
                before=t.db.total_changes;retained=reader.retained_entries()
                old=read_configured_local_singleton(t,source,reader,ordinal=1,**kwargs)
                self.assertEqual(old['id'],'2');self.assertEqual(json.loads(old['props_json']),{'1023':'second'})
                self.assertIsNone(read_configured_local_singleton(t,source,reader,ordinal=3,**kwargs))
                self.assertEqual(t.db.total_changes,before);self.assertEqual(reader.retained_entries(),retained)
                scope_kwargs={k:v for k,v in kwargs.items()if k not in ('type_id','entity_id')}
                with open_configured_local_read(t,source,reader,ordinal=1,**scope_kwargs)as held:
                    self.assertIs(type(held),ConfiguredLocalRead);self.assertEqual(held.singleton(kwargs['type_id'],2)['id'],'2')
                    detached=held.source_rows();detached['object_current'].clear();self.assertTrue(held.source_rows()['object_current'])
                    with self.assertRaises(AttributeError):held.context=object()
                count=len(calls)
                with self.assertRaises(ValueError):held.renew()
                with self.assertRaises(ValueError):held.executor.query('SELECT forbidden',{})
                self.assertEqual(len(calls),count)
                released=[];original_snapshots=snapshots[1]['object_current']
                try:
                    with self.assertRaises(ValueError):
                        with open_configured_local_read(t,source,reader,ordinal=1,**scope_kwargs):snapshots[1]['object_current']=[]
                        released.append(True)
                finally:snapshots[1]['object_current']=original_snapshots
                self.assertEqual(released,[])
                for cancellation in (False,True):
                    primary=KeyboardInterrupt();fail=[False]
                    def admission(s,request,manifest,ctx):
                        if fail[0]:raise OSError('closing source callback')
                        return current(s,request,manifest,ctx)
                    try:
                        with open_configured_local_read(t,source,reader,ordinal=1,**{**scope_kwargs,'current_admission':admission}):
                            fail[0]=True
                            if cancellation:raise primary
                        self.fail('Closing source failure released scope')
                    except KeyboardInterrupt as caught:self.assertTrue(cancellation);self.assertIs(caught,primary)
                    except OSError:self.assertFalse(cancellation)
                # A complete bag at the chosen snapshot cannot replace original
                # canonical committed phase custody.
                original=phases[requests[1]['batch_id']];phases[requests[1]['batch_id']]=original[:-1]
                with self.assertRaises(ValueError):read_configured_local_singleton(t,source,reader,ordinal=1,**kwargs)
                self.assertEqual(t.db.total_changes,before)
                with self.assertRaises(ValueError):read_configured_local_singleton(t,source,reader,ordinal=1,**{**kwargs,'type_id':999})
