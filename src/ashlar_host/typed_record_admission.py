# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded public core0.8 Record checks; no source, catalog or native authority."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from ashlar.schema import decode_retained_json
from ashlar.typed_source_policy import TypedSourcePolicy, typed_values, RECORD_CHECK_PIN
from ashlar.whole_entity import changes_from_batch
from .config import ProducerConfig
from .evolution_producer import capture, snapshot
from .lifecycle import finish, owned_context

_CHECKER=Path(__file__).parent/'resources/typed_record_values.ts'


def _require(value):
    if not value:raise ValueError('Original typed Record admission refused')


def _environment():
    return {'LANG':'C.UTF-8','LC_ALL':'C.UTF-8','HOME':'/nonexistent',
            'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null',
            'GIT_TERMINAL_PROMPT':'0','GIT_OPTIONAL_LOCKS':'0'}


def _custody(config):
    identities=[]
    for path in (config.bun,config.git):
        item=path.lstat()
        _require(stat.S_ISREG(item.st_mode)and os.access(path,os.X_OK))
        identities.append((str(path),item.st_dev,item.st_ino,item.st_size,item.st_mtime_ns,item.st_ctime_ns))
    for args,wanted in ((('rev-parse','HEAD'),RECORD_CHECK_PIN.encode()),(('status','--porcelain','--untracked-files=normal'),b'')):
        out,err=capture([str(config.git),'-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null','-C',str(config.source),*args],cwd=config.source,environment=_environment(),timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
        _require(not err and out.strip()==wanted)
    return tuple(identities),hashlib.sha256(snapshot(_CHECKER,65536)).hexdigest()


def admit_typed_records(config,model,bindings,batches,*,source_system,schema_revision):
    """Return an original ordered public UMF receipt, including delete Records.

    Clean pinned source and regular selected executables are checked opening and
    closing. This cooperative trusted-host profile does not isolate dependencies
    or authenticate development bindings as accepted catalog allocations.
    """
    _require(type(config)is ProducerConfig and type(model)is bytes and 0<len(model)<=65536
             and type(bindings)is dict and 1<=len(bindings)<=128
             and type(batches)is tuple and 1<=len(batches)<=4)
    config.__post_init__()
    _require(all(type(v)is str and 0<len(v.encode())<=1024 and '\0'not in v for v in (source_system,schema_revision)))
    original_bindings=copy.deepcopy(bindings)
    for tid,binding in bindings.items():
        _require(type(tid)is int and 1<=tid<2**31 and type(binding)is dict and set(binding)=={'record','fields'})
        _require(type(binding['record'])in (tuple,list)and len(binding['record'])==2 and all(type(v)is str and v for v in binding['record']))
        _require(type(binding['fields'])is dict and 1<=len(binding['fields'])<=1000)
    changes=[];records=[];request_size=len(b'{"records":[]}')
    for batch in batches:
        _require(len(changes)+len(batch.records)<=1000)
        group=changes_from_batch(batch);_require(len(group)==len(batch.records))
        for change in group:
            state=change.state
            _require(state.key.kind=='object'and state.key.source==source_system and state.schema_revision==schema_revision and state.key.type_id in bindings)
            binding=bindings[state.key.type_id];module,element=binding['record']
            record={'deliveryId':change.delivery_id,'recordSha256':change.raw_digest,'typeId':state.key.type_id,
                            'propsSha256':hashlib.sha256(state.props_json.encode()).hexdigest(),
                            'identity':{'module':module,'element':element},'values':typed_values(state.props_json,binding['fields'])}
            request_size+=len(json.dumps(record,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode())+(1 if records else 0)
            _require(request_size<=1048576);records.append(record)
        changes.extend(group)
    _require(0<len(records)<=1000)
    request=json.dumps({'records':records},ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    _require(len(request)<=1048576)
    baseline=_custody(config);primary=None;response=None
    try:
        with owned_context(tempfile.TemporaryDirectory(prefix='ashlar-typed-record-'))as directory:
            root=Path(directory);root.chmod(0o700)
            schema=root/'schema.json';_require(schema.write_bytes(model)==len(model));schema.chmod(0o600)
            query=root/'request.json';_require(query.write_bytes(request)==len(request));query.chmod(0o600)
            inner=None
            try:
                response,stderr=capture([str(config.bun),str(_CHECKER),str(config.source),str(schema),str(query),str(config.git)],cwd=config.source,environment=_environment(),timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
                _require(type(response)is bytes and 0<len(response)<=config.maximum_receipt_bytes and not stderr)
                receipt=decode_retained_json(response)
                _require(receipt.get('requestBase64')==base64.b64encode(request).decode())
                policy=TypedSourcePolicy(model,response,bindings,source_system=source_system,schema_revision=schema_revision,trusted_producer_revision=RECORD_CHECK_PIN)
                for change in changes:policy(change)
            except BaseException as exc:inner=exc
            finish(inner,[lambda:_require(snapshot(schema,65536)==model),lambda:_require(snapshot(query,1048576)==request)])
    except BaseException as exc:primary=exc
    finish(primary,[lambda:_require(_custody(config)==baseline),lambda:_require(bindings==original_bindings)])
    return response
