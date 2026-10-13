"""Existing original finite publication composition; same-process source only."""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import hashlib,json,os,stat,sys,weakref
from .supply_chain_producer import OriginalSupplyChainDatasetInvocation,reopen_original_supply_chain_source
from .evolution_verification_types import VerificationBounds
from .delta_custody import operation_capacity,encoded


def _canonical(path,*,directory=False):
    if type(path)is not type(Path('/'))or not path.is_absolute()or path.resolve()!=path or any(p.is_symlink()for p in (path,*path.parents)):
        raise ValueError('Explicit canonical existing path required')
    if directory and not path.is_dir():raise ValueError('Existing directory required')
    if not directory and not path.is_file():raise ValueError('Existing file required')
    if any(c in str(path)for c in ('`',"'",'\\','\x00','\n','\r')):raise ValueError('Closed local SQL path required')

@dataclass(frozen=True)
class SupplyChainCommonRuntime:
    python:Path
    java_home:Path
    directory:Path
    def __post_init__(self):
        _canonical(self.python);_canonical(self.java_home,directory=True);_canonical(self.directory,directory=True)
        info=self.directory.stat()
        if info.st_uid!=os.getuid()or info.st_mode&0o077:raise PermissionError('Owned private ephemeral runtime directory required')

@dataclass(frozen=True)
class SupplyChainCommonReadConfig:
    invocation:OriginalSupplyChainDatasetInvocation
    root:Path
    installation:Path
    journal:Path
    runtime:SupplyChainCommonRuntime
    jars:Path
    pack:Path
    capacity:object
    bounds:VerificationBounds
    def __post_init__(self):
        if type(self.invocation)is not OriginalSupplyChainDatasetInvocation or type(self.runtime)is not SupplyChainCommonRuntime or type(self.bounds)is not VerificationBounds:
            raise ValueError('Exact same-process invocation, runtime and bounds required')
        self.runtime.__post_init__();self.bounds.__post_init__()
        for p in (self.root,self.jars):_canonical(p,directory=True)
        for p in (self.installation,self.journal,self.pack):_canonical(p)
        if self.installation!=self.root/'registry.json'or self.journal!=self.root/'operations.sqlite':raise ValueError('Exact original installation/journal selections required')
        if self.runtime.directory==self.root or self.root in self.runtime.directory.parents or self.runtime.directory in self.root.parents:
            raise ValueError('Separate ephemeral runtime and original native installation required')
        object.__setattr__(self,'capacity',operation_capacity(self.capacity))
        if self.capacity is None:raise ValueError('Explicit original operation capacity required')
    @property
    def output(self):return self.runtime.directory

_OPENED=weakref.WeakKeyDictionary()

@dataclass(frozen=True,init=False,eq=False)
class OpenedSupplyChainCommonRead:
    config:SupplyChainCommonReadConfig
    source:object
    driver:object
    provider:object
    context:object
    manifest:dict
    vector:dict
    aliases:dict
    model:bytes
    graph:bytes
    bindings:dict
    columns:dict
    registry:dict
    pack:bytes
    original_native_files:dict
    def __init__(self):raise TypeError('Only the existing publication owner opens a reader')
    def require_held_correspondence(self,context):
        """Pure original port/view checks inside this owner's acquired interval."""
        record=_OPENED.get(self)
        if record is None or context is not record['context']:
            raise PermissionError('Original live common reader required')
        record['guard'](self,context)
        if record.get('candidate') is not None or not record['provider'].active or not self.driver.held:
            raise PermissionError('Actual original common graph interval required')

    def require_current(self,context):
        record=_OPENED.get(self)
        if record is None or context is not record['context']:raise PermissionError('Original live common reader required')
        record['guard'](self,context)
        if record.get('candidate') is not None or record['provider'].active:
            record['require'](self,context)
        else:
            from .lifecycle import owned_context
            with owned_context(record['interval'](context)):record['require'](self,context)
    def query_provider(self,context,*,family,part,ordinal):
        record=_OPENED.get(self)
        if record is None or context is not record['context']:raise PermissionError('Original live common reader required')
        record['guard'](self,context)
        if record.get('candidate') is not None or record['provider'].active:raise PermissionError('Nonnested fixed query provider required')
        if type(ordinal)is not int or ordinal<0:raise ValueError('Exact fixed query ordinal required')
        from .supply_chain_common_count_requests import prepare_supply_chain_common_count_requests
        from .supply_chain_reader import FiniteSupplyChainProvider
        from .supply_chain_request import source_snapshot
        from .lifecycle import owned_context
        values=record['values']
        prepared=prepare_supply_chain_common_count_requests(values['pack'],values['model'],values['graph'],values['bindings'],values['manifest'],values['registry']['targets'],values['aliases'],family=family,part=part)
        selected=[q for q in prepared['queries']if q['ordinal']==ordinal]
        if len(selected)!=1:raise ValueError('Exactly one original fixed query required')
        request=source_snapshot(selected[0]['request']);original_request=source_snapshot(request);owner=self
        class OriginalQueryProvider(FiniteSupplyChainProvider):
            @contextmanager
            def interval(candidate,supplied):
                if candidate is not original_candidate:raise PermissionError('Exact owner-created query provider required')
                if _OPENED.get(owner)is not record or supplied is not record['context']:raise PermissionError('Original live query owner required')
                record['guard'](owner,supplied)
                if record.get('candidate') is not None or record['provider'].active:raise PermissionError('Nonnested original query interval required')
                if request!=original_request:raise ValueError('Original fixed query changed')
                primary=None
                try:
                    with owned_context(super().interval(supplied)):
                        record['candidate']=candidate
                        try:
                            owner.require_current(supplied);yield
                        except BaseException as error:primary=error
                        def current():
                            if request!=original_request:raise ValueError('Original fixed query changed')
                            owner.require_current(supplied)
                        _finish(primary,[current])
                finally:
                    if record.get('candidate')is candidate:record.pop('candidate',None)
        original_candidate=OriginalQueryProvider(values['driver'],source_snapshot(values['aliases']),_json(request['target']['bindingJson']))
        return original_candidate,request
    def native_files(self):
        record=_OPENED.get(self)
        if record is None:raise PermissionError('Original live common reader required')
        return record['native_files']()
    @contextmanager
    def read_interval(self,context):
        record=_OPENED.get(self)
        if record is None or context is not record['context']:raise PermissionError('Original live common reader required')
        from .lifecycle import owned_context
        record['guard'](self,context)
        with owned_context(record['interval'](context)):
            self.require_current(context);primary=None
            try:yield
            except BaseException as error:primary=error
            _finish(primary,[lambda:self.require_current(context)])


def _json(raw):
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise ValueError('Duplicate original selection member')
            result[k]=v
        return result
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Finite original selection required')))

class _HeldFile:
    def __init__(self,path,maximum):
        self.path=path;self.maximum=maximum;self.fd=None
        try:
            _canonical(path);self.fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);self.info=os.fstat(self.fd)
            if not stat.S_ISREG(self.info.st_mode)or self.info.st_nlink!=1 or not 0<self.info.st_size<=maximum:raise ValueError('Original bounded regular file required')
            self.raw=self.read()
        except BaseException as primary:
            fd=self.fd;self.fd=None
            _finish(primary,[lambda:os.close(fd)]if fd is not None else[])
    def read(self):
        _canonical(self.path);before=os.fstat(self.fd);named=self.path.lstat()
        facts=lambda s:(s.st_dev,s.st_ino,s.st_mode,s.st_nlink,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        if facts(before)!=facts(self.info)or facts(named)!=facts(self.info):raise ValueError('Original held/named file changed')
        os.lseek(self.fd,0,os.SEEK_SET);parts=[];size=0
        while True:
            part=os.read(self.fd,min(65536,self.maximum-size+1))
            if not part:break
            size+=len(part)
            if size>self.maximum:raise ValueError('Original file grew past selected budget')
            parts.append(part)
        raw=b''.join(parts)
        if size!=self.info.st_size or facts(os.fstat(self.fd))!=facts(before)or facts(self.path.lstat())!=facts(before):raise ValueError('Original complete bytes changed')
        return raw
    def renew(self):
        if self.read()!=self.raw:raise ValueError('Original bytes changed')
    def close(self):
        if self.fd is not None:fd=self.fd;self.fd=None;os.close(fd)

def _finish(primary,callbacks):
    from .lifecycle import finish
    try:finish(None,callbacks)
    except BaseException:
        if primary is None:raise
        try:setattr(primary,'cleanup_failed',True)
        except BaseException:pass
    if primary is not None:raise primary

@contextmanager
def open_supply_chain_common_read(config):
    """Existing-only original reader; ephemeral session aliases confer no grant."""
    from .supply_chain_native import PrivateFiniteLease,finite_runtime,open_finite_spark,finite_current_admission,finite_original_request,finite_materialization_columns
    from .read_only_transport import ReadOnlyTransport
    from .delta_custody import DeltaTarget,LocalDeltaTransport
    from .driver import PrivatePolicy
    from .supply_chain_publication import FiniteSupplyChainDriver,GRAPH_ROLES
    from .supply_chain_reader import FiniteSupplyChainProvider
    from .supply_chain_request import supply_chain_count_star_request,source_snapshot,supply_chain_cases
    from .lifecycle import owned_context
    from ashlar.native import quote_table_identifier
    if type(config)is not SupplyChainCommonReadConfig:raise ValueError('Exact existing common read configuration required')
    config.__post_init__();original_config=(config.invocation,config.root,config.installation,config.journal,config.runtime,config.jars,config.pack,config.capacity,config.bounds)
    source=lease=spark=transport=interval=opened=None;query_port=sql_port=spark_stop=transport_close=source_renew=source_metadata=history_port=None;files=[];primary=None;targets=();context=object();root_info=config.root.stat();runtime_info=config.runtime.directory.stat();runtime_selection=(config.runtime.python,config.runtime.java_home,config.runtime.directory);bound_values=tuple(getattr(config.bounds,k)for k in ('input_bytes','rows','cell_bytes','total_bytes','receipt_bytes','capture_bytes'))
    def config_current():
        config.__post_init__()
        if (config.invocation,config.root,config.installation,config.journal,config.runtime,config.jars,config.pack,config.capacity,config.bounds)!=original_config or (config.runtime.python,config.runtime.java_home,config.runtime.directory)!=runtime_selection or tuple(getattr(config.bounds,k)for k in ('input_bytes','rows','cell_bytes','total_bytes','receipt_bytes','capture_bytes'))!=bound_values:raise ValueError('Original read configuration changed')
        if Path(sys.executable).resolve()!=runtime_selection[0]or os.environ.get('JAVA_HOME')!=str(runtime_selection[1]):raise ValueError('Original runtime environment changed')
        for path,info in ((original_config[1],root_info),(runtime_selection[2],runtime_info)):
            now=path.stat()
            if (now.st_dev,now.st_ino,now.st_mode)!=(info.st_dev,info.st_ino,info.st_mode):raise ValueError('Original selected directory changed')
    def journal_current():LocalDeltaTransport.require_existing_journal(original_config[3],'original-finite-supplychain',targets,capacity=original_config[7])
    try:
        lease=PrivateFiniteLease(config.root,create=False)
        _canonical(config.root/'.finite-source-writer.lock')
        for p in (config.installation,config.root/'request.json',config.pack):files.append(_HeldFile(p,config.bounds.input_bytes))
        registry=_json(files[0].raw);request=_json(files[1].raw);pack=files[2].raw
        if type(registry)is not dict or set(registry)!={'installation','tables','targets','operation_capacity','publication_id'}or registry['installation']!='original-finite-supplychain'or registry['publication_id']!='original-finite-supplychain-publication':raise ValueError('Closed original supply-chain registry required')
        roles=GRAPH_ROLES|{'attempts','manifest'};tables=registry['tables']
        if tables!={r:'local.supplychainfinite.'+r for r in roles}or type(registry['targets'])is not list or len(registry['targets'])!=6 or registry['operation_capacity']!=dict(config.capacity):raise ValueError('Complete original target/capacity selection required')
        selected=[]
        for row in registry['targets']:
            if type(row)is not dict or set(row)!={'table','path','uuid'}or row['table'] not in tables.values():raise ValueError('Closed original target required')
            role=next(r for r in tables if tables[r]==row['table']);path=Path(row['path'])
            if path!=config.root/role:raise ValueError('Original native target path differs')
            _canonical(path,directory=True);_canonical(path/'_delta_log',directory=True);_canonical(path/'.ashlar-local-installation-reservation.json')
            if not any((path/'_delta_log').glob('*.json')):raise ValueError('Existing native Delta log required')
            selected.append(DeltaTarget(row['table'],path,row['uuid']))
        if len({t.table for t in selected})!=6 or len({t.uuid for t in selected})!=6:raise ValueError('Distinct original six target UUIDs required')
        targets=tuple(selected);journal_current()
        source=reopen_original_supply_chain_source(config.invocation);source_close=source.current_dataset.close;source_renew=source.renew;source_metadata=source.metadata
        if request!=finite_original_request(source.batch):raise ValueError('Original complete retained request differs')
        supply_chain_cases(pack,source.model,source.graph)
        if Path(sys.executable).resolve()!=config.runtime.python or os.environ.get('JAVA_HOME')!=str(config.runtime.java_home):raise ValueError('Actual selected Python/JDK environment required')
        java_release=_HeldFile(config.runtime.java_home/'release',65536);files.append(java_release)
        if not any(line.startswith('JAVA_VERSION="21.')for line in java_release.raw.decode('utf8').splitlines()):raise ValueError('Explicit JDK21 selection required')
        jars=finite_runtime(config);spark=open_finite_spark(config,jars);spark_stop=spark.stop
        policy=PrivatePolicy(context,targets);policy.initializing=False
        transport=ReadOnlyTransport.open(spark,config.journal,registry['installation'],targets,policy,capacity=config.capacity,verification_bounds=config.bounds)
        query_port=transport.query;sql_port=spark.sql;transport_close=transport.close
        interval=transport.bounded_observations(config.bounds);interval.__enter__()
        columns=finite_materialization_columns();driver=FiniteSupplyChainDriver(transport,policy,context,tables,columns,source,registry['publication_id'],current_admission=finite_current_admission(lease,context,tables,targets))
        descriptor=driver.restore_committed_read_only(request)
        history_port=transport.original_history
        histories={}
        for target in targets:histories[target.table]=history_port(target)
        original_records=driver.original_committed_records
        aliases={tables[r]:'spark_catalog.supplychainfinite.'+r for r in GRAPH_ROLES}
        # Fresh ephemeral catalog only. All persistent Delta/registry/reservation
        # owners have already admitted the original existing installation.
        spark.sql('CREATE DATABASE supplychainfinite LOCATION '+"'"+str(config.runtime.directory/'warehouse/supplychainfinite.db')+"'")
        for native,alias in aliases.items():spark.sql('CREATE TABLE '+quote_table_identifier(alias)+" USING DELTA LOCATION '"+str(transport.targets[native].path)+"'")
        binding=_json(supply_chain_count_star_request('split-excursion',pack,source.model,source.graph,source.bindings,driver.manifest,registry['targets'],aliases)['target']['bindingJson'])
        provider=FiniteSupplyChainProvider(driver,aliases,binding);manifest=source_snapshot(driver.manifest);vector={t:(transport.targets[t].uuid,v)for t,v in descriptor.versions.items()}
        def native_files():
            source_metadata();return {k:hashlib.sha256(v).hexdigest()for k,v in {'model':source.model,'graph':source.graph,'finite_source_receipt':source.receipt,'source_transaction':source.raw}.items()}
        values={'config':config,'source':source,'driver':driver,'provider':provider,'context':context,'manifest':manifest,'vector':vector,'aliases':source_snapshot(aliases),'model':source.model,'graph':source.graph,'bindings':source_snapshot(source.bindings),'columns':{r:tuple(schema)for r,schema in columns.items()},'registry':source_snapshot(registry),'pack':pack,'original_native_files':native_files()}
        original_views={k:source_snapshot(v)if k not in ('vector','columns')else {n:tuple(tuple(pair)if isinstance(pair,tuple)else pair for pair in vv)for n,vv in v.items()}for k,v in values.items()if k in ('manifest','vector','aliases','bindings','columns','registry','original_native_files')}
        original_driver_ports=(driver.source,driver.transport,driver.policy,driver.context,driver.current_admission,transport.query,transport.spark,spark.sql,transport.original_history)
        original_targets={t.table:(t.path,t.uuid)for t in targets}
        def same(current,original):
            if type(current)is not type(original):return False
            if type(original)is dict:return len(current)==len(original)and all(k in current and same(current[k],v)for k,v in original.items())
            if type(original)in (tuple,list):return len(current)==len(original)and all(same(a,b)for a,b in zip(current,original))
            return current==original
        def guard(owner,supplied):
            if supplied is not context:raise PermissionError('Original common reader context required')
            if source.renew!=source_renew or source.metadata!=source_metadata or spark.stop!=spark_stop or transport.close!=transport_close or transport.observation_bounds is not config.bounds or (driver.source,driver.transport,driver.policy,driver.context,driver.current_admission,transport.query,transport.spark,spark.sql,transport.original_history)!=original_driver_ports or {n:(t.path,t.uuid)for n,t in transport.targets.items()}!=original_targets:raise ValueError('Original common reader native ports changed')
            for key,value in values.items():
                if key in original_views:
                    if not same(getattr(owner,key),original_views[key]):raise ValueError('Original common reader view changed')
                elif getattr(owner,key)is not value:raise ValueError('Original common reader owner changed')
        def require(owner,supplied):
            guard(owner,supplied)
            active_provider=_OPENED.get(owner,{}).get('candidate',provider)
            if not active_provider.active or not driver.held:raise PermissionError('Actual held original common interval required')
            config_current();lease.renew();source_renew()
            if driver.original_committed_records!=original_records:raise ValueError('Original observed committed phase records changed')
            if native_files()!=values['original_native_files']:raise ValueError('Original complete source bytes changed')
            driver.validate_manifest(manifest,context)
        opened=object.__new__(OpenedSupplyChainCommonRead)
        for key,value in values.items():object.__setattr__(opened,key,value)
        _OPENED[opened]={'context':context,'provider':provider,'guard':guard,'require':require,'native_files':native_files,'interval':provider.interval,'values':values}
        with owned_context(provider.interval(context)):opened.require_current(context)
        yield opened
    except BaseException as error:primary=error
    callbacks=[]
    if opened is not None:
        original_provider=values['provider'];original_driver=values['driver']
        def final_read():
            # A separate unexposed nominal driver reads original committed phases
            # through the same public existing-only owner; no field reset/fake grant.
            closing_driver=FiniteSupplyChainDriver(transport,policy,context,tables,columns,source,registry['publication_id'],current_admission=finite_current_admission(lease,context,tables,targets))
            closing=closing_driver.restore_committed_read_only(request)
            if dict(closing.raw)!=manifest or closing_driver.original_committed_records!=original_records:raise ValueError('Original committed phase/descriptor bytes changed')
            with owned_context(original_provider.interval(context)):require(opened,context)
        callbacks.append(final_read)
    if transport is not None and query_port is not None:
        original_spark=spark;original_query=query_port;original_sql=sql_port;closing_targets=tuple(targets)
        def native_closing(target):
            if transport.spark is not original_spark or original_spark.sql!=original_sql or {n:(t.path,t.uuid)for n,t in transport.targets.items()}!={t.table:(t.path,t.uuid)for t in closing_targets}:raise ValueError('Original native closing port changed')
            rows=original_query('DESCRIBE DETAIL '+quote_table_identifier(target.table),{}).rows
            if len(rows)!=1 or rows[0]['id']!=target.uuid or rows[0]['location'].removeprefix('file:').rstrip('/')!=str(target.path):raise ValueError('Original native closing identity changed')
        callbacks.extend([lambda target=target:native_closing(target)for target in closing_targets])
    if source is not None:callbacks.append(source_renew if source_renew is not None else lambda:type(source).renew(source))
    if targets:callbacks.append(journal_current)
    if transport is not None and history_port is not None:
        original_history=history_port
        def unchanged_history(target):
            closing=original_history(target)
            if target.table in histories and closing!=histories[target.table]:raise ValueError('Read composition changed original native history')
        callbacks.extend([lambda target=target:unchanged_history(target)for target in targets])
    callbacks.extend([f.renew for f in files]);callbacks.append(config_current)
    if spark is not None:callbacks.append(lambda:finite_runtime(config))
    if lease is not None:callbacks.append(lease.renew)
    if interval is not None:callbacks.append(lambda:interval.__exit__(None,None,None))
    if transport is not None:callbacks.append(transport_close if transport_close is not None else lambda:type(transport).close(transport))
    if spark is not None:callbacks.append(spark_stop if spark_stop is not None else lambda:type(spark).stop(spark))
    if source is not None:callbacks.append(source_close if 'source_close' in locals()else lambda:source.current_dataset.close())
    callbacks.extend([f.close for f in files])
    if lease is not None:callbacks.append(lease.close)
    def release():
        if opened is not None:_OPENED.pop(opened,None)
    callbacks.append(release);_finish(primary,callbacks)
