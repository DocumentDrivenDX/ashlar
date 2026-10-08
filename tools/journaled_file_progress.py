"""Private immutable-file consumer progress after native descriptor resolution.

This is durable local consumer progress, never a remote producer/Truss ACK.
The caller must hold admitted source/writer custody throughout resolution/commit.
"""
import hashlib
import json
import os
from pathlib import Path
from ashlar.publisher import PublicationError
from ashlar.schema import _json
from ashlar.source_checkpoint import bind_source_descriptor
from ashlar.staging import batch_row
from ashlar.publication import Descriptor


class LocalProgressOutcomeUnknown(PublicationError):
    """Local checkpoint COMMIT was attempted but acknowledgement remains unresolved."""


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)


class JournaledFileProgress:
    """Descriptor-bound local progress for an explicitly admitted immutable file.

    policy.admit(request,descriptor,context) renews source/writer/checkpoint
    authority and must return None. resolver(descriptor,context) is a context
    manager yielding the actual original native Descriptor under complete pins,
    protocol/retention/source admission. No permissive defaults are provided.
    """
    def __init__(self,journal,source,source_sha256,policy,resolver,*,stream,feed,epoch,
                 config):
        supported={('csv_consumer_scope','csv_consumer_progress','ashlar-single-line-csv/0.1'),
                   ('jsonl_consumer_scope','jsonl_consumer_progress','ashlar-immutable-jsonl/0.1')}
        if (self.scope_table,self.progress_table,self.checkpoint_profile) not in supported:
            raise PublicationError('Explicit supported immutable-file adapter required')
        self.db=journal.db;self.source=Path(source);self.source_sha256=source_sha256
        if not isinstance(source_sha256,str) or len(source_sha256)!=64 or any(c not in '0123456789abcdef' for c in source_sha256):
            raise PublicationError('Exact admitted original file digest required')
        if not isinstance(stream,str) or not stream:raise PublicationError('Explicit local consumer stream required')
        self.stream=stream;self.feed=feed;self.epoch=epoch;self.policy=policy;self.resolver=resolver
        self.config=dict(config)
        self._originals()
        with self.db:
            self.db.execute(f'CREATE TABLE IF NOT EXISTS {self.scope_table} (stream TEXT,feed TEXT,epoch TEXT,source_sha256 TEXT NOT NULL,config_json TEXT NOT NULL,PRIMARY KEY(stream,feed,epoch))')
            self.db.execute(f'INSERT OR IGNORE INTO {self.scope_table} VALUES (?,?,?,?,?)',(stream,feed,epoch,source_sha256,encoded(self.config)))
        self._scope()
        with self.db:self.db.execute(f'CREATE TABLE IF NOT EXISTS {self.progress_table} (stream TEXT,feed TEXT,epoch TEXT,position INTEGER,original_json TEXT NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(stream,feed,epoch,position))')

    def _scope(self):
        row=self.db.execute(f'SELECT source_sha256,config_json FROM {self.scope_table} WHERE stream=? AND feed=? AND epoch=?',(self.stream,self.feed,self.epoch)).fetchone()
        if row!=(self.source_sha256,encoded(self.config)):raise PublicationError('Original immutable file epoch/configuration scope conflict')

    def _originals(self):
        fd=os.open(self.source,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            status=os.fstat(fd)
            import stat
            if not stat.S_ISREG(status.st_mode) or status.st_uid!=os.getuid() or status.st_mode&0o022 or status.st_size>2*1024*1024:
                raise PublicationError('Bounded private original file file custody required')
            with os.fdopen(fd,'rb',closefd=False) as source:raw=source.read(2*1024*1024+1)
            if hashlib.sha256(raw).hexdigest()!=self.source_sha256:raise PublicationError('Original immutable file epoch content differs')
        finally:os.close(fd)
        batches=tuple(self.decode(raw))
        if not batches:raise PublicationError('Complete original file transactions required')
        originals={};previous='0'
        for batch in batches:
            checkpoint=_json(self.checkpoint(batch).encode())
            if checkpoint['previous']!=previous or checkpoint['profile']!=self.checkpoint_profile:
                raise PublicationError('Original complete checkpoint chain required')
            position=int(checkpoint['position'])
            if position in originals:raise PublicationError('Duplicate original checkpoint position')
            originals[position]=batch;previous=checkpoint['position']
        return originals

    def _records(self):
        self._scope()
        rows=self.db.execute(f'SELECT position,original_json,digest FROM {self.progress_table} WHERE stream=? AND feed=? AND epoch=? ORDER BY position',(self.stream,self.feed,self.epoch)).fetchall()
        originals=self._originals()
        if len(rows)>len(originals):raise PublicationError('Local progress inventory exceeds original source')
        predecessor=None
        for expected,(position,text,digest) in zip(originals,rows):
            if position!=expected or hashlib.sha256(text.encode()).hexdigest()!=digest:
                raise PublicationError('Missing or corrupt original local progress custody')
            value=_json(text.encode())
            if not isinstance(value,dict) or set(value)!={'request','descriptor'}:raise PublicationError('Exact retained progress proof required')
            self._bind(value['request'],position,originals)
            if predecessor is not None and value['request']['predecessor']!=predecessor:
                raise PublicationError('Retained publication predecessor chain differs')
            row=value['descriptor']
            from ashlar.manifest import validate_manifest_row
            from ashlar.publication import _decode,_freeze
            validate_manifest_row(row)
            descriptor=Descriptor(row['publication_id'],row['profile_version'],_freeze(_decode(row['table_versions_json'])),
                _freeze(_decode(row['schema_revisions_json'])),_freeze(_decode(row['source_progress_json'])),_freeze(_decode(row['validation_report_json'])),_freeze(row))
            bind_source_descriptor(value['request'],descriptor,expected_publication_id=descriptor.publication_id)
            predecessor=descriptor.publication_id
        return rows

    def _bind(self,request,position,originals):
        from ashlar.attempt_store import _request_digest
        _request_digest(request)
        if request['stream']!=self.stream or position not in originals:
            raise PublicationError('Request outside admitted immutable file consumer scope')
        batch=originals[position];row=batch_row(batch)
        if (request.get('source_checkpoint_json'),request['source_batch_json'],request['source_batch_digest'])!=(
                self.checkpoint(batch),row['batch_json'],row['batch_digest']):
            raise PublicationError('Progress differs from original file bytes/mapping/checkpoint')

    def position(self):
        """Observe retained local progress only; not native publication admission."""
        rows=self._records()
        return str(rows[-1][0] if rows else 0)

    def completed_batches(self):
        return len(self._records())

    def acknowledge(self,request,descriptor,context):
        request=_json(encoded(dict(request)).encode())
        if not isinstance(descriptor,Descriptor):raise PublicationError('Original committed descriptor required')
        from ashlar.attempt_store import _request_digest
        _request_digest(request)
        checkpoint=_json(request['source_checkpoint_json'].encode())
        if checkpoint.get('profile')!=self.checkpoint_profile:raise PublicationError('Original selected local file checkpoint profile required')
        position=int(checkpoint['position']);self._bind(request,position,self._originals())
        bind_source_descriptor(request,descriptor,expected_publication_id=descriptor.publication_id)
        def admit():
            self._originals()
            if self.policy.admit(_json(encoded(request).encode()),descriptor,context) is not None:
                raise PublicationError('Complete current local source progress admission required')
        admit()
        committed=False;commit_started=False;write_started=False
        try:
            with self.resolver(descriptor,context) as original:
                if not isinstance(original,Descriptor) or original!=descriptor:
                    raise PublicationError('Actual resolved original native publication required')
                admit()
                text=encoded({'request':request,'descriptor':dict(original.raw)})
                digest=hashlib.sha256(text.encode()).hexdigest()
                # Native resolver and source lane stay held across the durable local
                # transaction; closing refusal rolls back the new progress row.
                if self.db.in_transaction:raise PublicationError('Independent local progress transaction required')
                self.db.execute('BEGIN IMMEDIATE')
                try:
                    rows=self._records();existing={r[0]:(r[1],r[2]) for r in rows}
                    if position in existing:
                        if existing[position]!=(text,digest):raise PublicationError('Original local progress conflict')
                    else:
                        previous=rows[-1][0] if rows else 0
                        originals=self._originals()
                        if len(rows)>=len(originals) or position!=list(originals)[len(rows)] or checkpoint['previous']!=str(previous):raise PublicationError('Local source progress gap or regression')
                        if rows and request['predecessor']!=_json(rows[-1][1].encode())['descriptor']['publication_id']:
                            raise PublicationError('Publication predecessor differs from original local progress')
                        write_started=True
                        self.db.execute(f'INSERT INTO {self.progress_table} VALUES (?,?,?,?,?,?)',(self.stream,self.feed,self.epoch,position,text,digest))
                    admit()
                    if not self.db.in_transaction:
                        commit_started=write_started
                        raise PublicationError('Admission callback closed the independent local progress transaction')
                    commit_started=True
                    self.db.commit()
                    committed=True
                except BaseException:
                    if write_started and not self.db.in_transaction:commit_started=True
                    self.db.rollback();raise
            if not committed:raise PublicationError('Native resolver suppressed incomplete local progress admission')
        except BaseException as error:
            if commit_started:raise LocalProgressOutcomeUnknown('Local progress COMMIT was attempted before acknowledgement completed; reconcile the original retained checkpoint') from error
            raise
