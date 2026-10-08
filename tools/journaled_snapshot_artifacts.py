"""Host native full-row snapshot capture with immutable original proposal custody.

The supplied policy must admit source, expected state, target identities and held
writer/source authority. Parity alone grants no publication/retention/ACK authority.
"""
import hashlib
import json
from ashlar.attempt_store import _request_digest
from ashlar.effect_validation import validate_effect_snapshot
from ashlar.publisher import PublicationError
from ashlar.schema import _json
from ashlar.stored_publisher import _artifact


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)


class JournaledSnapshotArtifacts:
    """Capture exact native versions/complete rows before retaining a proposal.

    snapshots(request,effects,context) supplies independently derived bounded
    targets: table -> {uuid,version,columns,rows}. manifest(request,effects,
    witnesses,context) supplies the fully admitted original manifest row.
    Policy.admit(request,effects,targets,context) must return None, both for
    capture and recovery. Caller must hold the whole writer/source interval.
    Recovery returns retained bytes only: a capturing row without an artifact
    is unresolved, never permission to observe a replacement snapshot.
    """
    def __init__(self,journal,executor,policy,snapshots,manifest,*,namespace):
        if not isinstance(namespace,str) or not namespace or len(namespace.encode())>1024 or '\x00' in namespace:
            raise PublicationError('Explicit bounded artifact namespace required')
        self.db=journal.db;self.executor=executor;self.policy=policy
        self.snapshots=snapshots;self.manifest=manifest;self.prefix=namespace+':'
        with self.db:self.db.execute('CREATE TABLE IF NOT EXISTS snapshot_artifact (operation TEXT PRIMARY KEY,request_json TEXT NOT NULL,effects_json TEXT NOT NULL,targets_json TEXT NOT NULL,artifact_text TEXT,artifact_digest TEXT,capture_digest TEXT NOT NULL)')

    def _input(self,request,effects):
        request=dict(request);_request_digest(request)
        effects=_json(encoded(effects).encode())
        if not isinstance(effects,dict) or effects.get('intent_digest')!=request['request_digest']:
            raise PublicationError('Original request-bound effect results required')
        return request,effects,self.prefix+request['request_digest']

    def _row(self,operation,request,effects):
        row=self.db.execute('SELECT request_json,effects_json,targets_json,artifact_text,artifact_digest,capture_digest FROM snapshot_artifact WHERE operation=?',(operation,)).fetchone()
        if row is None or row[:2]!=(encoded(request),encoded(effects)):
            raise PublicationError('Original snapshot capture custody missing or changed')
        if hashlib.sha256(encoded(list(row[:3])).encode()).hexdigest()!=row[5]:
            raise PublicationError('Original capture inventory digest differs')
        if (row[3] is None)!=(row[4] is None):raise PublicationError('Torn snapshot artifact custody')
        if row[3] is not None and hashlib.sha256(row[3].encode()).hexdigest()!=row[4]:
            raise PublicationError('Original snapshot artifact digest differs')
        return row[:5]

    def _admit(self,request,effects,targets,context):
        if self.policy.admit(_json(encoded(request).encode()),_json(encoded(effects).encode()),
            _json(encoded(targets).encode()),context) is not None:
            raise PublicationError('Complete current snapshot admission required')

    def capture(self,request,effects,context):
        request,effects,operation=self._input(request,effects)
        if self.db.execute('SELECT operation FROM snapshot_artifact WHERE operation=?',(operation,)).fetchone():
            return self.recover(request,effects,context)
        targets=_json(encoded(self.snapshots(dict(request),_json(encoded(effects).encode()),context)).encode())
        if not isinstance(targets,dict) or not targets or len(targets)>16:
            raise PublicationError('Bounded complete snapshot target inventory required')
        for target in targets.values():
            if not isinstance(target,dict) or set(target)!={'uuid','version','columns','rows'}:
                raise PublicationError('Exact snapshot interpretation required')
        target_text=encoded(targets)
        if len(target_text.encode())>2*1024*1024:raise PublicationError('Snapshot custody budget exceeded')
        self._admit(request,effects,targets,context)
        with self.db:
            capture_digest=hashlib.sha256(encoded([encoded(request),encoded(effects),target_text]).encode()).hexdigest()
            created=self.db.execute('INSERT OR IGNORE INTO snapshot_artifact VALUES (?,?,?,?,NULL,NULL,?)',(operation,encoded(request),encoded(effects),target_text,capture_digest)).rowcount==1
        if not created:return self.recover(request,effects,context)
        if self._row(operation,request,effects)[2]!=target_text:raise PublicationError('Original target inventory conflict')
        witnesses=[]
        for table,target in targets.items():
            witnesses.append(validate_effect_snapshot(self.executor,table,target['uuid'],target['version'],target['columns'],target['rows']))
        self._admit(request,effects,targets,context)
        row=self.manifest(dict(request),_json(encoded(effects).encode()),witnesses,context)
        text=encoded({'effects':effects,'manifest':row})
        _,descriptor=_artifact(text,request)
        if dict(descriptor.versions)!={table:target['version'] for table,target in targets.items()}:
            raise PublicationError('Manifest does not bind the complete captured version vector')
        digest=hashlib.sha256(text.encode()).hexdigest()
        with self.db:self.db.execute('UPDATE snapshot_artifact SET artifact_text=?,artifact_digest=? WHERE operation=? AND artifact_text IS NULL',(text,digest,operation))
        if self._row(operation,request,effects)[3:]!=(text,digest):raise PublicationError('Original snapshot artifact conflict')
        return text

    def recover(self,request,effects,context):
        request,effects,operation=self._input(request,effects)
        row=self._row(operation,request,effects)
        self._admit(request,effects,_json(row[2].encode()),context)
        if row[3] is None:raise PublicationError('Original snapshot observations unresolved; replacement capture is forbidden')
        artifact,descriptor=_artifact(row[3],request)
        if encoded(artifact['effects'])!=encoded(effects):raise PublicationError('Retained artifact effect receipts differ')
        targets=_json(row[2].encode())
        if dict(descriptor.versions)!={table:target['version'] for table,target in targets.items()}:
            raise PublicationError('Retained snapshot vector differs')
        return row[3]
