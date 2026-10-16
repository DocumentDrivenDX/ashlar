"""Pinned local Delta4 optimistic transaction primitive for engine implementers.

This is an atomic commit prerequisite, not an action compiler or authorization
issuer. Trusted engine code must evaluate the original Weft plan and markers
against the returned preimage before submitting its complete candidate frame.
No connection/session, table, epoch, retry or replacement identity is created.
"""
from dataclasses import dataclass
from pathlib import Path
import hashlib,json,re,time
from urllib.parse import urlparse,unquote
from py4j.protocol import Py4JJavaError

class DeltaAtomicRefused(ValueError):pass
class DeltaAlreadySubmitted(DeltaAtomicRefused):pass
class DeltaReconciliationPending(DeltaAtomicRefused):pass
class DeltaAtomicConflict(DeltaAtomicRefused):
    def __init__(self,original):
        self.original=original
        super().__init__("Original native Delta optimistic conflict; no retry")

@dataclass(frozen=True)
class DeltaAtomicReceipt:
    table_uuid:str
    epoch:str
    version:int
    request_sha256:str
    metadata_json:str

class DeltaAtomic:
    def __init__(self,spark,*,path,table_uuid,epoch,request_sha256,maximum_rows):
        if spark.version!='4.0.1':raise DeltaAtomicRefused('Exact Spark4.0.1 required')
        if any(type(v)is not str or not v or len(v)>1024 for v in (table_uuid,epoch)) or not re.fullmatch('[0-9a-f]{64}',request_sha256):raise DeltaAtomicRefused('Original store, epoch and request SHA-256 required')
        if type(maximum_rows)is not int or not 1<=maximum_rows<=10000:raise DeltaAtomicRefused('Explicit bounded whole-table transaction required')
        path=Path(path)
        if not path.is_absolute() or not path.is_dir() or path.is_symlink():raise DeltaAtomicRefused('Original local table path required')
        self.spark,self.path,self.table_uuid,self.epoch=spark,path,table_uuid,epoch
        self.request_sha256,self.maximum_rows=request_sha256,maximum_rows
        self.used=False;self.submitted=False;self.metadata_json=None;self.jvm=spark._jvm
        loader=self.jvm.java.lang.Thread.currentThread().getContextClassLoader()
        native=loader.loadClass('org.apache.spark.sql.delta.DeltaLog')
        location=urlparse(native.getProtectionDomain().getCodeSource().getLocation().toExternalForm())
        if location.scheme!='file' or location.netloc or hashlib.sha256(Path(unquote(location.path)).read_bytes()).hexdigest()!='538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e':
            raise DeltaAtomicRefused('Exact original Delta4.0.0 JVM class bytes required')
        self.log=self.jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession,str(path))
        self.transaction=self.log.startTransaction()
        metadata=self.transaction.metadata();configuration=metadata.configuration()
        if metadata.id()!=table_uuid or not configuration.get('umf.actions.epoch').isDefined() or configuration.get('umf.actions.epoch').get()!=epoch or not configuration.get('delta.isolationLevel').isDefined() or configuration.get('delta.isolationLevel').get()!='Serializable':raise DeltaAtomicRefused('Original native table/epoch/Serializable profile differs')
        protocol=self.transaction.protocol()
        if protocol.minReaderVersion()!=1 or protocol.minWriterVersion()!=2:raise DeltaAtomicRefused('Original Delta protocol1/2 required')
        self.version=int(self.transaction.readVersion())
        self.application='umf.actions.native-delta/0.1:'+epoch+':'+request_sha256
        if int(self.transaction.txnVersion(self.application))!=-1:raise DeltaAlreadySubmitted('Original native transaction marker exists; reconcile its original commit')
        self.transaction.readWholeTable()
        self.files=self.transaction.filterFiles()
        self.preimage=spark.read.format('delta').option('versionAsOf',self.version).load(str(path))
        if self.preimage.limit(maximum_rows+1).count()>maximum_rows:raise DeltaAtomicRefused('Complete original table exceeds transaction bound')
        self.schema_json=self.preimage.schema.json()
    def commit(self,candidate,*,markers_json):
        if self.used:raise DeltaAtomicRefused('Transaction is single-use; no retry')
        self.used=True
        def pairs(items):
            result={}
            for k,v in items:
                if k in result:raise DeltaAtomicRefused('Duplicate original marker member')
                result[k]=v
            return result
        if type(markers_json)is not str or len(markers_json.encode())>1048576:raise DeltaAtomicRefused('Bounded complete original marker JSON required')
        markers=json.loads(markers_json,object_pairs_hook=pairs,parse_constant=lambda v:(_ for _ in ()).throw(DeltaAtomicRefused('Nonfinite marker')))
        if type(markers)is not dict:raise DeltaAtomicRefused('Original marker object required')
        if candidate.schema.json()!=self.schema_json or candidate.limit(self.maximum_rows+1).count()>self.maximum_rows:raise DeltaAtomicRefused('Complete unchanged physical schema and bounded candidate required')
        metadata=json.dumps({'profile':'umf.actions.native-delta-atomic/0.1','tableUuid':self.table_uuid,'epoch':self.epoch,'requestSha256':self.request_sha256,'readVersion':self.version,'markers':markers},ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
        self.metadata_json=metadata
        added=self.transaction.writeFiles(candidate._jdf)
        actions=self.jvm.java.util.ArrayList()
        for i in range(self.files.size()):actions.add(self.files.apply(i).removeWithTimestamp(int(time.time()*1000),True))
        for i in range(added.size()):actions.add(added.apply(i))
        none=self.jvm.scala.Option.empty()
        actions.add(self.jvm.org.apache.spark.sql.delta.actions.SetTransaction(self.application,0,none))
        operation=getattr(self.jvm.org.apache.spark.sql.delta,'DeltaOperations$Write')(self.jvm.org.apache.spark.sql.SaveMode.Overwrite,none,none,self.jvm.scala.Option.apply(metadata))
        sequence=self.jvm.org.apache.spark.api.python.PythonUtils.toSeq(actions)
        self.submitted=True
        try:
            version=int(self.transaction.commit(sequence,operation))
        except Py4JJavaError as error:
            name=error.java_exception.getClass().getName()
            if name in ('io.delta.exceptions.ConcurrentAppendException','io.delta.exceptions.ConcurrentDeleteReadException','io.delta.exceptions.ConcurrentDeleteDeleteException','io.delta.exceptions.MetadataChangedException','io.delta.exceptions.ProtocolChangedException'):
                raise DeltaAtomicConflict(error) from error
            raise
        # Read back the original commit, never the latest history row. The local
        # development profile deliberately requires retained commit JSON bytes.
        original=self.path/'_delta_log'/f'{version:020d}.json'
        if not original.is_file():raise DeltaAtomicRefused('Committed original log unavailable; outcome requires reconciliation')
        events=[json.loads(line)for line in original.read_text().splitlines()]
        if sum(e.get('txn',{}).get('appId')==self.application and e['txn'].get('version')==0 for e in events)!=1 or sum(e.get('commitInfo',{}).get('userMetadata')==metadata for e in events)!=1:
            raise DeltaAtomicRefused('Original atomic data/transaction/marker commit correspondence unavailable')
        return DeltaAtomicReceipt(self.table_uuid,self.epoch,version,self.request_sha256,metadata)
    def reconcile(self,*,maximum_commits=100):
        """Inspect only this retained original submission; absence stays uncertain.

        This engine custody operation grants no caller authority, never retries
        a mutation, and never clears an owning executor's quarantine.
        """
        if not self.submitted or self.metadata_json is None:raise DeltaAtomicRefused('Original submitted transaction custody required')
        if type(maximum_commits)is not int or not 1<=maximum_commits<=10000:raise DeltaAtomicRefused('Bounded original commit inspection required')
        none=self.jvm.scala.Option.empty()
        snapshot=self.log.update(False,none,none);metadata=snapshot.metadata();configuration=metadata.configuration()
        if metadata.id()!=self.table_uuid or not configuration.get('umf.actions.epoch').isDefined()or configuration.get('umf.actions.epoch').get()!=self.epoch:
            raise DeltaAtomicRefused('Original native store/epoch changed during reconciliation')
        tip=int(snapshot.version())
        if tip-self.version>maximum_commits:raise DeltaReconciliationPending('Original commit inspection exceeds bound; retain uncertainty')
        found=[]
        for version in range(self.version+1,tip+1):
            original=self.path/'_delta_log'/f'{version:020d}.json'
            if not original.is_file():raise DeltaReconciliationPending('Original commit history unavailable; retain uncertainty')
            events=[json.loads(line)for line in original.read_text().splitlines()]
            markers=[e['txn']for e in events if e.get('txn',{}).get('appId')==self.application]
            if not markers:continue
            if len(markers)!=1 or markers[0].get('version')!=0 or sum(e.get('commitInfo',{}).get('userMetadata')==self.metadata_json for e in events)!=1:
                raise DeltaAtomicRefused('Original transaction/verification custody differs')
            found.append(DeltaAtomicReceipt(self.table_uuid,self.epoch,version,self.request_sha256,self.metadata_json))
        if len(found)!=1:raise DeltaReconciliationPending('No unique retained original commit; retain uncertainty')
        return found[0]
