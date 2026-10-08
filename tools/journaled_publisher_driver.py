"""Host driver connecting original effect custody to StoredPublisherBackend.

Native source/writer admission, parity, retention, pins and ACK are mandatory
injected services. This module grants none of them and does not generate SQL.
"""
from contextlib import contextmanager
import hashlib
import json
from ashlar.attempt_store import _request_digest
from ashlar.schema import _json
from ashlar.publisher import PublicationError
from ashlar.stored_publisher import _artifact


class JournaledPublisherDriver:
    """Keep the original request/steps and applied artifact across host restarts.

    planner(request, context) -> exact ordered effect steps.
    artifacts.capture/recover(request, effects, context) -> original artifact JSON.
    recover must reconcile original artifact observations, never replace a known
    proposal. validator(request, text, descriptor, context) and acknowledger
    (request, descriptor, context) must independently admit complete current
    native obligations and return None. An absent recovery record refuses.
    """
    def __init__(self,effects,writer_policy,planner,artifacts,validator,acknowledger,*,namespace):
        if not isinstance(namespace,str) or not namespace or len(namespace.encode())>1024 or '\x00' in namespace:
            raise PublicationError('Explicit bounded publisher operation namespace required')
        self.effects=effects;self.db=effects.transport.db;self.policy=writer_policy
        self.planner=planner;self.artifacts=artifacts;self.validator=validator;self.acknowledger=acknowledger
        self.prefix='publisher-effects:'+namespace+':'
        self._held=None
        with self.db:self.db.execute('CREATE TABLE IF NOT EXISTS publisher_effect_artifact (operation TEXT PRIMARY KEY,request_json TEXT NOT NULL,steps_json TEXT NOT NULL,artifact_text TEXT,artifact_digest TEXT)')

    @contextmanager
    def writer(self,stream,context):
        if self._held is not None:raise PublicationError('Publisher driver session already held')
        with self.policy.writer(stream,context) as permit:
            if permit is not None:raise PublicationError('Complete publisher writer admission required')
            self._held=(stream,context)
            try:yield
            finally:self._held=None

    def _request(self,request,context):
        request=dict(request);_request_digest(request)
        if self._held is None or self._held[0]!=request['stream'] or self._held[1] is not context:
            raise PublicationError('Original held publisher driver context required')
        text=json.dumps(request,sort_keys=True,separators=(',',':'),ensure_ascii=False)
        return request,text,self.prefix+request['request_digest']

    def _retained(self,operation,text):
        row=self.db.execute('SELECT request_json,steps_json,artifact_text,artifact_digest FROM publisher_effect_artifact WHERE operation=?',(operation,)).fetchone()
        if row is None or row[0]!=text:raise PublicationError('Original publisher effect custody missing or changed')
        if (row[2] is None)!=(row[3] is None):raise PublicationError('Torn original artifact custody')
        if row[2] is not None and hashlib.sha256(row[2].encode()).hexdigest()!=row[3]:
            raise PublicationError('Original artifact digest differs')
        return row

    def _execute(self,request,context,recovery):
        request,text,operation=self._request(request,context)
        if not recovery:
            steps=json.dumps(self.planner(dict(request),context),sort_keys=True,separators=(',',':'),ensure_ascii=False)
            if len(steps.encode())>1024*1024:raise PublicationError('Effect step custody budget exceeded')
            with self.db:
                created=self.db.execute('INSERT OR IGNORE INTO publisher_effect_artifact VALUES (?,?,?,NULL,NULL)',(operation,text,steps)).rowcount==1
            recovery=not created
            retained=self._retained(operation,text)
            if retained[1]!=steps:raise PublicationError('Original planned steps changed')
        retained=self._retained(operation,text)
        function=self.effects.recover if recovery else self.effects.run
        effects=function(operation,request['request_digest'],_json(retained[1].encode()),context=context)
        if retained[2] is not None:
            artifact=retained[2]
        else:
            capture=self.artifacts.recover if recovery else self.artifacts.capture
            artifact=capture(dict(request),effects,context)
        parsed,_=_artifact(artifact,request)
        if json.dumps(parsed['effects'],sort_keys=True,separators=(',',':'))!=json.dumps(effects,sort_keys=True,separators=(',',':')):raise PublicationError('Applied artifact differs from original effect responses')
        digest=hashlib.sha256(artifact.encode()).hexdigest()
        with self.db:self.db.execute('UPDATE publisher_effect_artifact SET artifact_text=?,artifact_digest=? WHERE operation=? AND artifact_text IS NULL',(artifact,digest,operation))
        if self._retained(operation,text)[2:]!=(artifact,digest):raise PublicationError('Original applied artifact conflict')
        return artifact

    def apply(self,request,context):return self._execute(request,context,False)
    def recover_apply(self,request,context):return self._execute(request,context,True)

    def validate(self,request,artifact,descriptor,context):
        request,text,operation=self._request(request,context)
        retained=self._retained(operation,text)
        if retained[2]!=artifact:raise PublicationError('Validation requires exact original applied artifact')
        _,original=_artifact(artifact,request)
        if descriptor!=original:raise PublicationError('Original proposed descriptor required')
        if self.validator(dict(request),artifact,descriptor,context) is not None:
            raise PublicationError('Complete native publication admission required')

    def acknowledge(self,request,descriptor,context):
        request,text,operation=self._request(request,context)
        artifact=self._retained(operation,text)[2]
        if artifact is None:raise PublicationError('Original applied artifact required for ACK')
        _,original=_artifact(artifact,request)
        if descriptor!=original:raise PublicationError('ACK descriptor differs from original proposal')
        if self.acknowledger(dict(request),descriptor,context) is not None:
            raise PublicationError('Complete current source ACK admission required')
