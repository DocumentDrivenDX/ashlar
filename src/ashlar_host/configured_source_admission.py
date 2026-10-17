"""Installed original configured-source preflight; no database or source effects."""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from ashlar.schema import decode_retained_json
from ashlar.source import jsonl_batches
from ashlar.source_config import load_jsonl_configuration,inspect_configured_source
from .config import ProducerConfig
from .evolution_cli import InvocationFile
from .managed_record_admission import admit_managed_records,RECORD_CHECK_PIN
from .lifecycle import finish


def _require(value):
    if not value:raise ValueError('Configured source admission refused')

@dataclass(frozen=True)
class ConfiguredSourceAdmissionConfig:
    configuration:Path
    producer:ProducerConfig
    output:Path
    maximum_receipt_bytes:int
    def __post_init__(self):
        for path in (self.configuration,self.output):
            _require(type(path)is type(Path())and path.is_absolute()and path.parent.resolve()==path.parent)
        _require(type(self.producer)is ProducerConfig);self.producer.__post_init__()
        _require(type(self.maximum_receipt_bytes)is int and 1<=self.maximum_receipt_bytes<=4194304)
        _require(self.configuration!=self.output and self.output not in self.configuration.parents)
        _require(self.output!=self.producer.source and self.producer.source not in self.output.parents and self.output not in self.producer.source.parents)
        _require(all(self.output!=p and self.output not in p.parents for p in (self.producer.bun,self.producer.git)))


def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
    try:_require(type(n:=os.write(fd,raw))is int and n==len(raw))
    except BaseException as exc:primary=exc
    finish(primary,[lambda:os.close(fd)])


def validate_configured_source(config):
    """Retain original receipts and a closed report after complete input closing.

    Output is a fresh private directory. A failed invocation may retain partial
    receipts, but never a successful report. No native acceptance is supplied.
    """
    _require(type(config)is ConfiguredSourceAdmissionConfig);config.__post_init__()
    parent=config.output.parent;s=parent.lstat()
    _require(stat.S_ISDIR(s.st_mode)and s.st_uid==os.getuid()and not s.st_mode&0o022)
    parent_identity=(s.st_dev,s.st_ino)
    original=InvocationFile.read(config.configuration,262144)
    document=decode_retained_json(original.raw);references=[]
    for name in ('source','schema','intake','interpretation'):
        item=document[name];_require(type(item)is dict and set(item)=={'path','sha256'})
        _require(type(item['path'])is str and '\0'not in item['path'])
        path=(original.path.parent/item['path']).resolve();_require(config.output!=path and config.output not in path.parents)
        selected=InvocationFile.read(path,262144);_require(hashlib.sha256(selected.raw).hexdigest()==item['sha256']);references.append(selected)
    configured=load_jsonl_configuration(original.path)
    def closing():
        def parent_check():
            current=parent.lstat();_require((current.st_dev,current.st_ino)==parent_identity and current.st_uid==os.getuid()and not current.st_mode&0o022)
        finish(None,[lambda:original.renew(262144),*(lambda f=f:f.renew(262144)for f in references),configured.verify,parent_check])
    receipts=None;summary=None;primary=None
    try:
        summary=inspect_configured_source(configured)
        batches=tuple(jsonl_batches(references[0].raw.splitlines(keepends=True),feed=configured.feed,epoch=configured.epoch))
        receipts=admit_managed_records(config.producer,configured.intake,configured.policy,batches)
        _require(type(receipts)is tuple and len(receipts)<=128 and all(type(raw)is bytes for raw in receipts)and sum(map(len,receipts))<=config.maximum_receipt_bytes)
    except BaseException as exc:primary=exc
    finish(primary,[closing])
    config.output.mkdir(mode=0o700)
    output_stat=config.output.lstat();identity=(output_stat.st_dev,output_stat.st_ino)
    def output_check():
        current=config.output.lstat();_require(stat.S_ISDIR(current.st_mode)and(current.st_dev,current.st_ino)==identity and current.st_uid==os.getuid()and stat.S_IMODE(current.st_mode)==0o700)
    inventory=[];primary=None
    try:
        for ordinal,raw in enumerate(receipts,1):
            output_check();name='record-'+str(ordinal)+'.json';_write(config.output/name,raw)
            inventory.append({'file':name,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
    except BaseException as exc:primary=exc
    finish(primary,[closing,output_check])
    report={'profile':'ashlar-configured-source-admission-result/0.1','admitted':True,
            'configuration_sha256':hashlib.sha256(original.raw).hexdigest(),
            'source_sha256':summary['source_sha256'],'model_sha256':configured.intake.source_sha256,
            'producer_revision':RECORD_CHECK_PIN,'batches':len(batches),'records':sum(len(b.records)for b in batches),
            'receipts':inventory,'published':False,'acknowledged':False,
            'qualification':'Original configured string Record/binding/replay preflight and fresh public UMF logical-value receipts; no native acceptance, publication or source ACK.'}
    raw=json.dumps(report,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode();_require(len(raw)<=65536)
    _write(config.output/'report.json',raw)
    return report
