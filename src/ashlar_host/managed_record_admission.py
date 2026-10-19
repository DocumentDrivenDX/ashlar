# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded original c45 UMF Record admission; no native/source authority."""
import base64
import hashlib
import json
import os
import stat
from pathlib import Path
import tempfile
from ashlar.schema import SchemaIntake, decode_retained_json
from ashlar.semantic_policy import StringRecordPolicy
from ashlar.whole_entity import changes_from_batch
from .config import ProducerConfig
from .evolution_producer import capture, snapshot
from .lifecycle import finish

RECORD_CHECK_PIN='c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3'
_CHECKER=Path(__file__).parent/'resources/managed_record_values.ts'

def _require(value):
    if not value:raise ValueError('Original managed Record admission refused')

def _hash(raw):return hashlib.sha256(raw).hexdigest()

def _environment():
    # No ambient Git configuration, executable discovery or producer settings.
    return {'LANG':'C.UTF-8','LC_ALL':'C.UTF-8','HOME':'/nonexistent',
            'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null',
            'GIT_TERMINAL_PROMPT':'0','GIT_OPTIONAL_LOCKS':'0'}

def _source(config):
    env=_environment()
    def git(*args):
        out,err=capture([str(config.git),'-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null','-C',str(config.source),*args],cwd=config.source,environment=env,timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
        _require(not err);return out
    _require(git('rev-parse','HEAD').strip()==RECORD_CHECK_PIN.encode()and not git('status','--porcelain','--untracked-files=normal'))
    identities=[]
    for path in (config.bun,config.git):
        item=path.lstat()
        _require(stat.S_ISREG(item.st_mode)and os.access(path,os.X_OK))
        identities.append((str(path),item.st_dev,item.st_ino,item.st_size,item.st_mtime_ns,item.st_ctime_ns))
    return tuple(identities),_hash(snapshot(_CHECKER,65536))

def admit_managed_records(config, intake, policy, batches):
    """Revalidate complete original source Record groups with retained raw receipts.

    The clean developer-selected Git producer and selected executables are held
    by opening/closing file identities and exact clean Git pin. This is a
    cooperative trusted-host profile, not OS/dependency isolation.
    """
    _require(type(config)is ProducerConfig and type(intake)is SchemaIntake and type(policy)is StringRecordPolicy and type(batches)is tuple and 1<=len(batches)<=4)
    config.__post_init__()
    _require(SchemaIntake.read(intake.artifact,intake.document_revision,trusted_validator_revision=intake.validator_revision)==intake and policy.source_sha256==intake.source_sha256 and policy.document_revision==intake.document_revision and len(intake.source)<=65536)
    groups={}
    for batch in batches:
        changes=changes_from_batch(batch);_require(len(changes)==len(batch.records))
        for change,record in zip(changes,batch.records):
            selected=policy.record_value_request(change)
            if change.operation=='delete':continue
            identity=selected['identity'];key=(identity['module'],identity['element'])
            groups.setdefault(key,[]).append({'deliveryId':record.delivery_id,'recordSha256':record.sha256,'values':selected['values']})
    _require(len(groups)<=128)
    baseline=_source(config);receipts=[];primary=None;total=0
    try:
        with tempfile.TemporaryDirectory(prefix='ashlar-managed-record-')as directory:
            root=Path(directory);os.chmod(root,0o700)
            schema=root/'schema.json';schema.write_bytes(intake.source);schema.chmod(0o600)
            for ordinal,(key,records)in enumerate(sorted(groups.items())):
                _require(len(records)<=1000)
                raw=json.dumps({'identity':{'module':key[0],'element':key[1]},'records':records},ensure_ascii=False,separators=(',',':')).encode();_require(len(raw)<=1048576)
                request=root/('request-'+str(ordinal)+'.json');request.write_bytes(raw);request.chmod(0o600)
                response,stderr=capture([str(config.bun),str(_CHECKER),str(config.source),RECORD_CHECK_PIN,str(schema),str(request),str(config.git)],cwd=config.source,environment=_environment(),timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
                total+=len(response);_require(not stderr and total<=config.maximum_receipt_bytes)
                result=decode_retained_json(response)
                _require(result.get('format')=='ashlar-umf-record-check/0.1'and result.get('producerRevision')==RECORD_CHECK_PIN and result.get('sourceSha256')==intake.source_sha256 and result.get('sourceBase64')==base64.b64encode(intake.source).decode()and result.get('requestSha256')==_hash(raw))
                actual=result.get('records');_require(type(actual)is list and len(actual)==len(records))
                for original,checked in zip(records,actual):
                    _require((checked.get('deliveryId'),checked.get('recordSha256'))==(original['deliveryId'],original['recordSha256']))
                    validation=checked['result']['validation'];_require(validation['valid']is True and validation['complete']is True)
                _require(snapshot(schema,65536)==intake.source and snapshot(request,1048576)==raw)
                receipts.append(response)
    except BaseException as exc:primary=exc
    def closing():_require(_source(config)==baseline)
    finish(primary,[closing])
    return tuple(receipts)
