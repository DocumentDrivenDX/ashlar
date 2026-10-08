"""Private immutable-file consumer progress after native descriptor resolution.

This is durable local consumer progress, never a remote producer/Truss ACK.
The caller must hold admitted source/writer custody throughout resolution/commit.
"""
import hashlib
import json
import os
from pathlib import Path
from ashlar.csv_source import csv_batches,validate_csv_batch
from ashlar.publisher import PublicationError
from ashlar.schema import _json
from ashlar.source_checkpoint import bind_source_descriptor,csv_checkpoint
from ashlar.staging import batch_row
from ashlar.publication import Descriptor


class LocalProgressOutcomeUnknown(PublicationError):
    """Local checkpoint COMMIT was attempted but acknowledgement remains unresolved."""


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)


class JournaledCsvProgress:
    """Descriptor-bound local progress for an explicitly admitted immutable CSV.

    policy.admit(request,descriptor,context) renews source/writer/checkpoint
    authority and must return None. resolver(descriptor,context) is a context
    manager yielding the actual original native Descriptor under complete pins,
    protocol/retention/source admission. No permissive defaults are provided.
    """
    def __init__(self,journal,source,source_sha256,policy,resolver,*,stream,feed,epoch,
                 source_system,schema_revision,type_id,properties):
        self.db=journal.db;self.source=Path(source);self.source_sha256=source_sha256
        if not isinstance(source_sha256,str) or len(source_sha256)!=64 or any(c not in '0123456789abcdef' for c in source_sha256):
            raise PublicationError('Exact admitted original file digest required')
        if not isinstance(stream,str) or not stream:raise PublicationError('Explicit local consumer stream required')
        self.stream=stream;self.feed=feed;self.epoch=epoch;self.policy=policy;self.resolver=resolver
        self.config=dict(feed=feed,epoch=epoch,source_system=source_system,schema_revision=schema_revision,
            type_id=type_id,properties=dict(properties))
        self._originals()
        with self.db:
            self.db.execute('CREATE TABLE IF NOT EXISTS csv_consumer_scope (stream TEXT,feed TEXT,epoch TEXT,source_sha256 TEXT NOT NULL,config_json TEXT NOT NULL,PRIMARY KEY(stream,feed,epoch))')
            self.db.execute('INSERT OR IGNORE INTO csv_consumer_scope VALUES (?,?,?,?,?)',(stream,feed,epoch,source_sha256,encoded(self.config)))
        self._scope()
        with self.db:self.db.execute('CREATE TABLE IF NOT EXISTS csv_consumer_progress (stream TEXT,feed TEXT,epoch TEXT,position INTEGER,original_json TEXT NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(stream,feed,epoch,position))')

    def _scope(self):
        row=self.db.execute('SELECT source_sha256,config_json FROM csv_consumer_scope WHERE stream=? AND feed=? AND epoch=?',(self.stream,self.feed,self.epoch)).fetchone()
        if row!=(self.source_sha256,encoded(self.config)):raise PublicationError('Original immutable CSV epoch/configuration scope conflict')

    def _originals(self):
        fd=os.open(self.source,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            status=os.fstat(fd)
            import stat
            if not stat.S_ISREG(status.st_mode) or status.st_uid!=os.getuid() or status.st_mode&0o022 or status.st_size>2*1024*1024:
                raise PublicationError('Bounded private original CSV file custody required')
            with os.fdopen(fd,'rb',closefd=False) as source:raw=source.read(2*1024*1024+1)
            if hashlib.sha256(raw).hexdigest()!=self.source_sha256:raise PublicationError('Original immutable CSV epoch content differs')
        finally:os.close(fd)
        batches=tuple(csv_batches(raw.splitlines(keepends=True),**self.config))
        if not batches:raise PublicationError('Complete original CSV rows required')
        for batch in batches:validate_csv_batch(batch,**self.config)
        return {int(_json(csv_checkpoint(batch).encode())['position']):batch for batch in batches}

    def _records(self):
        self._scope()
        rows=self.db.execute('SELECT position,original_json,digest FROM csv_consumer_progress WHERE stream=? AND feed=? AND epoch=? ORDER BY position',(self.stream,self.feed,self.epoch)).fetchall()
        originals=self._originals()
        if len(rows)>len(originals):raise PublicationError('Local progress inventory exceeds original source')
        predecessor=None
        for expected,(position,text,digest) in enumerate(rows,1):
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
            raise PublicationError('Request outside admitted immutable CSV consumer scope')
        batch=originals[position];row=batch_row(batch)
        if (request.get('source_checkpoint_json'),request['source_batch_json'],request['source_batch_digest'])!=(
                csv_checkpoint(batch),row['batch_json'],row['batch_digest']):
            raise PublicationError('Progress differs from original CSV bytes/mapping/checkpoint')

    def position(self):
        """Observe retained local progress only; not native publication admission."""
        rows=self._records()
        return str(rows[-1][0] if rows else 0)

    def acknowledge(self,request,descriptor,context):
        request=_json(encoded(dict(request)).encode())
        if not isinstance(descriptor,Descriptor):raise PublicationError('Original committed descriptor required')
        from ashlar.attempt_store import _request_digest
        _request_digest(request)
        checkpoint=_json(request['source_checkpoint_json'].encode())
        if checkpoint.get('profile')!='ashlar-single-line-csv/0.1':raise PublicationError('Only explicit local CSV consumer progress supported')
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
                        if position!=previous+1:raise PublicationError('Local source progress gap or regression')
                        if rows and request['predecessor']!=_json(rows[-1][1].encode())['descriptor']['publication_id']:
                            raise PublicationError('Publication predecessor differs from original local progress')
                        write_started=True
                        self.db.execute('INSERT INTO csv_consumer_progress VALUES (?,?,?,?,?,?)',(self.stream,self.feed,self.epoch,position,text,digest))
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
