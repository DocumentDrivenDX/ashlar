# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh public core0.8 scalar admission for a pinned typed JSONL source."""
import hashlib,json,os,stat
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch
from ashlar.apply import empty_state,plan_apply
from ashlar.typed_source_config import load_typed_jsonl_configuration
from .evolution_cli import InvocationFile
from .typed_record_admission import admit_typed_records,RECORD_CHECK_PIN
from .lifecycle import finish
from .configured_source_admission_config import ConfiguredSourceAdmissionConfig


def _require(value):
    if not value:raise ValueError('Configured typed source admission refused')


def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
    try:_require(os.write(fd,raw)==len(raw))
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])


def validate_configured_typed_source(config):
    """Return only after fresh complete carried-value checks and file closing."""
    _require(type(config)is ConfiguredSourceAdmissionConfig);config.__post_init__()
    parent=config.output.parent;s=parent.lstat()
    _require(stat.S_ISDIR(s.st_mode)and s.st_uid==os.getuid()and not s.st_mode&0o022)
    parent_identity=(s.st_dev,s.st_ino)
    original=InvocationFile.read(config.configuration,262144)
    configured=load_typed_jsonl_configuration(original.path)
    files=tuple(InvocationFile.read(path,262144)for path,_ in configured.references)
    _require(all(config.output!=f.path and config.output not in f.path.parents for f in files))
    def closing():
        def parent_check():
            current=parent.lstat();_require((current.st_dev,current.st_ino)==parent_identity and current.st_uid==os.getuid()and not current.st_mode&0o022)
        finish(None,[lambda:original.renew(262144),*(lambda f=f:f.renew(262144)for f in files),configured.verify,parent_check])
    raw=None;primary=None
    try:
        batches=tuple(jsonl_batches(configured.source_bytes.splitlines(keepends=True),feed=configured.feed,epoch=configured.epoch))
        _require(1<=len(batches)<=4)
        bindings={tid:{'record':tuple(b['record']),'fields':{pid:tuple(ref)for pid,ref in b['fields'].items()}}for tid,b in configured.bindings.items()}
        raw=admit_typed_records(config.producer,configured.model_source,bindings,batches,
                                source_system=configured.source_system,schema_revision=configured.document_revision)
        _require(type(raw)is bytes and len(raw)<=config.maximum_receipt_bytes)
        admitted=configured.with_receipt(raw);state=empty_state()
        for batch in batches:state=plan_apply(state,changes_from_batch(batch),schema_policy=admitted.policy)
        replay=state
        for batch in batches:replay=plan_apply(replay,changes_from_batch(batch),schema_policy=admitted.policy)
        _require(replay==state)
    except BaseException as error:primary=error
    finish(primary,[closing])
    config.output.mkdir(mode=0o700);opened=config.output.lstat();identity=(opened.st_dev,opened.st_ino)
    def output_check():
        current=config.output.lstat();_require(stat.S_ISDIR(current.st_mode)and(current.st_dev,current.st_ino)==identity and current.st_uid==os.getuid()and stat.S_IMODE(current.st_mode)==0o700)
    primary=None
    try:output_check();_write(config.output/'record-1.json',raw)
    except BaseException as error:primary=error
    finish(primary,[closing,output_check])
    report={'profile':'ashlar-configured-typed-source-admission-result/0.1','admitted':True,
            'configuration_sha256':hashlib.sha256(original.raw).hexdigest(),
            'source_sha256':hashlib.sha256(configured.source_bytes).hexdigest(),'model_sha256':configured.model_sha256,
            'producer_revision':RECORD_CHECK_PIN,'batches':len(batches),'records':sum(len(b.records)for b in batches),
            'receipts':[{'file':'record-1.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}],
            'published':False,'acknowledged':False,
            'qualification':'Original authored core0.8 scalar Record carried values, all events including deletes, and exact local source/apply/replay custody. No dataset key/relationship, native/catalog, publication, protected ACK or query authority.'}
    result=json.dumps(report,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode();_require(len(result)<=65536)
    _write(config.output/'report.json',result)
    return report
