#!/usr/bin/env python3
"""Hash-bound two-stage Sol gate for one live-alias .53 continuation process.

This helper is called by the configured-local query owner while its Spark read
scope and ConfiguredQueryAliases context are open. It contains no Spark startup
or SQL execution. The owner supplies a live-custody callback and invokes its SQL
callback only through `execute_after_review`.
"""
from __future__ import annotations
import hashlib,json,os,stat,time
from pathlib import Path

PROFILE='ashlar-r02-weft053-live-alias-review/0.1'
STAGE1=('singleton','backend03-original','backend03-positive','backend02-page1')
STAGE2=('backend02-continuation',)
LIMIT_BYTES=16*1024*1024
LIMIT_ARTIFACT=1024*1024
EXPECTED_JARS={
 'delta-spark_4.2_2.13-4.4.1.jar':'2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be',
 'delta-storage-4.4.1.jar':'f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f',
 'spark-core_2.13-4.2.0.jar':'8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4',
 'spark-sql-api_2.13-4.2.0.jar':'d789409a56db600c5160300fcd2c9651bf6f32dd8db1035a9bda17b67543b372',
 'spark-sql_2.13-4.2.0.jar':'ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf',
}

def _require(value):
    if not value: raise PermissionError('R02 live alias review gate refused')
def _sha(raw): return hashlib.sha256(raw).hexdigest()
def _canonical(value): return (json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode()
def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        _require(os.write(fd,raw)==len(raw)); os.fsync(fd)
    finally: os.close(fd)

def _roles(value):
    _require(type(value)is dict and set(value)=={'publicationId','manifestUuid','tables'}
        and type(value['publicationId'])is str and type(value['manifestUuid'])is str)
    tables=value['tables'];_require(type(tables)is dict and set(tables)=={'edge_current','object_current','tombstone','whole_source_history'})
    for role,item in tables.items():
        _require(type(item)is dict and set(item)=={'uuid','version'} and type(item['uuid'])is str
                 and len(item['uuid'])==36 and type(item['version'])is int and item['version']>=0)

def _alias_map(value):
    _require(type(value)is dict and set(value)=={'edge_current','object_current','tombstone','whole_source_history'})
    parts={}
    for role,alias in value.items():
        _require(type(alias)is str and alias.startswith('spark_catalog.ashlar_configured_query_'))
        bits=alias.split('.'); _require(len(bits)==3 and bits[-1]==role)
        parts[role]=bits[-2]
    _require(len(set(parts.values()))==1 and len(next(iter(parts.values())))==56)

def _artifact_record(name,item):
    expected_fields={'request','expected'} if name=='singleton' else {'request','response','expected'}|({'after'} if name=='backend02-continuation' else set())
    _require(type(item)is dict and set(item)==expected_fields|{'publicationId','manifestUuid','vector','aliases'})
    request=item['request']; _require(type(request)is bytes and 0<len(request)<=LIMIT_ARTIFACT)
    result={'name':name,'requestSha256':_sha(request),'expected':item['expected']}
    if name!='singleton':
        response=item['response']; _require(type(response)is bytes and 0<len(response)<=LIMIT_ARTIFACT)
        request_value=json.loads(request); parsed=json.loads(response)
        if request_value.get('version')=='weft.engine-page/0.1':request_value=request_value.get('request')
        target=request_value.get('target',{});binding_raw=target.get('bindingJson')
        _require(type(binding_raw)is str and _sha(binding_raw.encode())==target.get('bindingSha256'))
        binding=json.loads(binding_raw);pub=binding.get('publication',{})
        _require(pub.get('id')==item['publicationId'] and pub.get('manifestUuid')==item['manifestUuid'])
        table_bindings=pub.get('tables');_require(type(table_bindings)is list and len(table_bindings)==4)
        by_name={tuple(t.get('name',())):t for t in table_bindings}
        _require(set(by_name)=={tuple(x.split('.'))for x in item['aliases'].values()})
        for role,alias in item['aliases'].items():
            table=by_name[tuple(alias.split('.'))];pinned=item['vector']['tables'][role]
            _require((table.get('uuid'),table.get('version'))==(pinned['uuid'],pinned['version']))
        _require(parsed.get('status')=='compiled')
        sql=parsed.get('sql') or parsed.get('artifact',{}).get('sql')
        _require(type(sql)is str and bool(sql))
        artifact=parsed.get('artifact',{})
        _require((parsed.get('bindingSha256')or artifact.get('bindingSha256'))==target['bindingSha256'])
        result.update(responseSha256=_sha(response),compiledSqlSha256=_sha(sql.encode()))
        if name=='backend02-continuation':
            _require(type(item['after'])is str and item['after']!='null')
            result['afterSha256']=_sha(item['after'].encode())
    else:
        _require(type(item['expected'])in (dict,list))
    return result

class R02LiveAliasReviewGate:
    def __init__(self,root,*,attempt_id,stage,source_revision,spark_application_id,vector,aliases,runtime,live_check,deadline_seconds=300):
        _require(type(root)is type(Path()) and root.is_absolute() and root.is_dir() and not root.is_symlink())
        st=root.lstat(); _require(stat.S_ISDIR(st.st_mode) and st.st_uid==os.getuid() and stat.S_IMODE(st.st_mode)==0o700)
        _require(type(attempt_id)is str and len(attempt_id)==32 and all(c in '0123456789abcdef' for c in attempt_id))
        _require(stage in ('pre-sql','post-page1') and type(source_revision)is str and len(source_revision)==40)
        _require(type(spark_application_id)is str and bool(spark_application_id) and callable(live_check))
        _roles(vector);_alias_map(aliases)
        _require(type(runtime)is dict and runtime.get('weftVersion')=='0.4.53'
            and runtime.get('weftWheelSha256')=='4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c'
            and runtime.get('weftSourceCommit')=='726a840e956e25217d6aeea2674fdfcf3f342a24'
            and type(runtime.get('weftIndexSha256'))is str and len(runtime['weftIndexSha256'])==64
            and runtime.get('cohortReceiptSha256')=='fe48796cead974bc06821bd296428f47b389e1016a7b43ff186ec15cb03f72e5'
            and runtime.get('pythonVersion')=='3.12.14' and runtime.get('jdkVersion')=='21'
            and runtime.get('sparkVersion')=='4.2.0' and runtime.get('deltaVersion')=='4.4.1'
            and runtime.get('jarSha256')==EXPECTED_JARS
            and type(deadline_seconds)is int and 1<=deadline_seconds<=300)
        self.root=root;self.attempt_id=attempt_id;self.stage=stage;self.source_revision=source_revision
        self.pid=os.getpid();self.app=spark_application_id;self.vector=vector;self.aliases=aliases
        self.runtime=runtime;self.live_check=live_check;self.deadline=time.monotonic()+deadline_seconds
        self.vector_sha=_sha(_canonical(vector));self.alias_sha=_sha(_canonical(aliases));self.runtime_sha=_sha(_canonical(runtime))
        self.approved=False;self.consumed=False;self.stage_succeeded=False
        self.executing_name=None
        self.prior_stage_sha=None;self.page1_after=None;self.stage1_expected=None

    def freeze(self,artifacts):
        names=STAGE1 if self.stage=='pre-sql' else STAGE2
        _require(type(artifacts)is dict and tuple(artifacts.keys())==names and not self.consumed)
        if self.stage=='pre-sql':_require(not any(p.name.startswith('pre-sql-')for p in self.root.iterdir()))
        else:_require(self.approved is False and self.prior_stage_sha is not None and not any(p.name.startswith('post-page1-')for p in self.root.iterdir()))
        if self.stage=='post-page1':_require(artifacts['backend02-continuation'].get('after')==self.page1_after)
        if self.stage=='pre-sql':
            expected=artifacts['backend02-page1']['expected']
            _require(type(expected)is dict and type(expected.get('continuationAfter'))is str and bool(expected['continuationAfter']))
            self.stage1_expected=expected
        self.live_check();normalized={n:{**artifacts[n],'publicationId':self.vector['publicationId'],
            'manifestUuid':self.vector['manifestUuid'],'vector':self.vector,'aliases':self.aliases}for n in names}
        records=[_artifact_record(n,normalized[n])for n in names]
        total=sum(len(artifacts[n]['request'])+(len(artifacts[n].get('response',b'')))for n in names)
        _require(total<=LIMIT_BYTES)
        for n in names:
            for suffix in ('request','response'):
                raw=artifacts[n].get(suffix)
                if raw is not None:_write(self.root/(self.stage+'-'+n+'-'+suffix+'.json'),raw)
        manifest={'profile':PROFILE,'attemptId':self.attempt_id,'stage':self.stage,'pid':self.pid,
            'sparkApplicationId':self.app,'sourceRevision':self.source_revision,'publicationVector':self.vector,
            'publicationVectorSha256':self.vector_sha,'aliases':self.aliases,'aliasMapSha256':self.alias_sha,
            'runtime':self.runtime,'runtimeSha256':self.runtime_sha,'artifacts':records,
            'limits':{'wholeProcessSeconds':300,'stageArtifactBytes':LIMIT_BYTES,'singleArtifactBytes':LIMIT_ARTIFACT},
            'state':'held-for-review'}
        if self.stage=='post-page1':manifest.update(priorStageManifestSha256=self.prior_stage_sha,
            priorStageReviewSha256=self.prior_review_sha,page1AfterSha256=_sha(self.page1_after.encode()))
        raw=_canonical(manifest);_require(len(raw)<=LIMIT_BYTES)
        path=self.root/(self.stage+'-manifest.json');_write(path,raw);self.manifest_sha=_sha(raw);return path,self.manifest_sha

    def execute_after_review(self,receipt_path,execute):
        _require(hasattr(self,'manifest_sha') and callable(execute) and not self.consumed and type(receipt_path)is type(Path())
                 and receipt_path.parent==self.root and receipt_path.name==self.stage+'-review.json')
        while time.monotonic()<self.deadline:
            self.live_check()
            if receipt_path.exists():break
            time.sleep(min(.1,max(0,self.deadline-time.monotonic())))
        _require(receipt_path.exists() and time.monotonic()<self.deadline)
        st=receipt_path.lstat();_require(stat.S_ISREG(st.st_mode) and not receipt_path.is_symlink() and st.st_size<=65536)
        receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
        _require(type(receipt)is dict and set(receipt)=={'profile','decision','reviewer','attemptId','stage','manifestSha256','pid','sparkApplicationId','publicationVectorSha256','aliasMapSha256','runtimeSha256'}
            and receipt=={'profile':'ashlar-r02-live-alias-review-receipt/0.1','decision':'approve','reviewer':'Sol',
                'attemptId':self.attempt_id,'stage':self.stage,'manifestSha256':self.manifest_sha,'pid':self.pid,
                'sparkApplicationId':self.app,'publicationVectorSha256':self.vector_sha,'aliasMapSha256':self.alias_sha,
                'runtimeSha256':self.runtime_sha})
        _require(time.monotonic()<self.deadline);self.live_check()
        self.approved=True;self.review_sha=_sha(receipt_path.read_bytes());self.consumed=True
        names=STAGE1 if self.stage=='pre-sql' else STAGE2; cursor=0; results={}
        def execute_one(name,operation):
            nonlocal cursor
            _require(cursor<len(names) and name==names[cursor] and callable(operation))
            _require(time.monotonic()<self.deadline);self.live_check()
            _require(self.executing_name is None)
            self.executing_name=name
            try:value=operation()
            finally:self.executing_name=None
            results[name]=value;cursor+=1
            return value
        try:
            outcome=execute(execute_one)
            _require(cursor==len(names))
            if self.stage=='pre-sql':
                _require(type(outcome)is dict and outcome.get('continuationAfter')==self.stage1_expected['continuationAfter'])
                self.page1_after=outcome['continuationAfter']
            self.stage_succeeded=True
            return outcome
        except BaseException:
            self.stage_succeeded=False
            raise

    def next_stage_after_page1(self):
        _require(self.stage=='pre-sql' and self.approved and self.consumed and self.stage_succeeded
            and type(self.page1_after)is str and 0<len(self.page1_after.encode())<=LIMIT_ARTIFACT)
        self.prior_stage_sha=self.manifest_sha;self.prior_review_sha=self.review_sha
        self.stage='post-page1';self.approved=False;self.consumed=False;self.stage_succeeded=False
        self.executing_name=None
