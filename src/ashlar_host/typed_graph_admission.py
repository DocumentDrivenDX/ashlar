"""Bounded public073 typed Record and supplied-dataset admission transport.

Original JSONL/binding/config correspondence remains the core source owner's
obligation. Public receipts do not grant source, catalog, native or ACK authority.
"""
import base64
import hashlib
import re
import os
from pathlib import Path
import stat
import tempfile
from ashlar.schema import decode_retained_json
from ashlar.source import jsonl_batches
from .config import ProducerConfig
from .evolution_producer import capture,snapshot
from .lifecycle import finish,owned_context

UMF_REVISION='07357eadbda9cf8299b9d72d004cbb77f8091aee'
_CHECKER=Path(__file__).parent/'resources/typed_graph_values.ts'
_KEYS={'profile','configurationSha256','sourceSha256','bindingSha256','modelSha256','documentRevision','sourceSystem','feed','epoch','groups'}


def _require(value):
    if not value:raise ValueError('Original typed graph admission refused')


def _environment():
    return {'LANG':'C.UTF-8','LC_ALL':'C.UTF-8','HOME':'/nonexistent',
        'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0','GIT_OPTIONAL_LOCKS':'0'}


def _custody(config):
    identities=[]
    for path in (config.bun,config.git):
        item=path.lstat();_require(stat.S_ISREG(item.st_mode)and os.access(path,os.X_OK))
        identities.append((str(path),item.st_dev,item.st_ino,item.st_size,item.st_mtime_ns,item.st_ctime_ns))
    for args,wanted in ((('rev-parse','HEAD'),UMF_REVISION.encode()),(('status','--porcelain','--untracked-files=normal'),b'')):
        raw,stderr=capture([str(config.git),'-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null','-C',str(config.source),*args],cwd=config.source,environment=_environment(),timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
        _require(not stderr and raw.strip()==wanted)
    return tuple(identities),hashlib.sha256(snapshot(_CHECKER,65536)).hexdigest()



def _source_bytes(value):
    """Reuse the original public JSONL transaction parser; no UMF semantics."""
    for key in ('configurationSha256','sourceSha256','bindingSha256','modelSha256'):
        _require(type(value[key])is str and re.fullmatch('[0-9a-f]{64}',value[key])is not None)
    for key in ('documentRevision','sourceSystem','feed','epoch'):
        _require(type(value[key])is str and 0<len(value[key].encode())<=1024 and '\0'not in value[key])
    parts=[];size=0
    def original(text):
        nonlocal size
        _require(type(text)is str and len(text)<=1398104)
        raw=base64.b64decode(text,validate=True);size+=len(raw)
        _require(size<=1048576 and base64.b64encode(raw).decode()==text);return raw
    for group in value['groups']:
        batch=group['batch'];_require(type(batch)is dict and set(batch)=={'feed','epoch','batchId','recordCount','recordsSha256','cursorBefore','cursorAfter','beginBase64','commitBase64'})
        parts.append(original(batch['beginBase64']))
        for event in group['events']:
            _require(type(event)is dict and set(event)=={'deliveryId','recordSha256','rawBase64'})
            parts.append(original(event['rawBase64']))
        parts.append(original(batch['commitBase64']))
    raw=b''.join(parts);_require(hashlib.sha256(raw).hexdigest()==value['sourceSha256'])
    recovered=tuple(jsonl_batches(raw.splitlines(keepends=True),feed=value['feed'],epoch=value['epoch']))
    _require(len(recovered)==len(value['groups']))
    for batch,group in zip(recovered,value['groups']):
        declared=group['batch'];_require(type(declared['recordCount'])is int)
        _require((batch.feed,batch.epoch,batch.batch_id,len(batch.records),batch.records_sha256,batch.cursor_before,batch.cursor_after)==tuple(declared[k]for k in ('feed','epoch','batchId','recordCount','recordsSha256','cursorBefore','cursorAfter')))
        _require(len(batch.records)==len(group['events']))
        for record,event in zip(batch.records,group['events']):
            _require((record.delivery_id,record.sha256,base64.b64encode(record.raw).decode())==(event['deliveryId'],event['recordSha256'],event['rawBase64']))


def admit_typed_graph(config,model,request):
    """Return one unchanged public receipt after input and producer closings.

    The selected source is clean exact073 and executable/checker identities are
    renewed. This is a cooperative trusted-host profile, not dependency/OS
    isolation. Core proof verification does not replace public UMF validation.
    """
    _require(type(config)is ProducerConfig and type(model)is bytes and 0<len(model)<=65536
             and type(request)is bytes and 0<len(request)<=1048576)
    config.__post_init__();value=decode_retained_json(request)
    _require(type(value)is dict and set(value)==_KEYS and value['profile']=='ashlar-typed-graph-check-request/0.1'
        and value['modelSha256']==hashlib.sha256(model).hexdigest()and type(value['groups'])is list and 1<=len(value['groups'])<=4)
    _require(all(type(g)is dict and set(g)=={'batch','events','records','dataset'}and type(g['events'])is list and type(g['records'])is list for g in value['groups']))
    _require(sum(len(g['events'])for g in value['groups'])<=1000 and 0<sum(len(g['records'])for g in value['groups'])<=1000)
    _source_bytes(value)
    from ashlar.typed_graph_source import verify_typed_graph_receipt
    baseline=_custody(config);primary=None;response=None
    try:
        with owned_context(tempfile.TemporaryDirectory(prefix='ashlar-typed-graph-'))as directory:
            root=Path(directory);root.chmod(0o700)
            schema=root/'schema.json';_require(schema.write_bytes(model)==len(model));schema.chmod(0o600)
            query=root/'request.json';_require(query.write_bytes(request)==len(request));query.chmod(0o600)
            inner=None
            try:
                response,stderr=capture([str(config.bun),str(_CHECKER),str(config.source),str(schema),str(query),str(config.git)],cwd=config.source,environment=_environment(),timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
                _require(type(response)is bytes and 0<len(response)<=config.maximum_receipt_bytes and not stderr)
                _require(verify_typed_graph_receipt(model,request,response)is None)
            except BaseException as error:inner=error
            finish(inner,[lambda:_require(snapshot(schema,65536)==model),lambda:_require(snapshot(query,1048576)==request)])
    except BaseException as error:primary=error
    finish(primary,[lambda:_require(_custody(config)==baseline)])
    return response
