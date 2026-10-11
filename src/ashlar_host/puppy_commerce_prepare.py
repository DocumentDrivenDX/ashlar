"""Private exact-commerce carrier preparation, never activation authority."""
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path
import hashlib,json,os,base64
from .lifecycle import owned_context,finish
from .puppy_commerce_projection import original_projection,exact_scenario_expected,PROFILE_EXACT
from .puppy_commerce_projection import commerce_snapshot as source_snapshot

@dataclass(frozen=True)
class PuppyCommercePrepareConfig:
    release: bytes
    custody: bytes
    release_sha256: str
    custody_sha256: str
    pack: bytes
    output: Path
    def __post_init__(self):
        if any(type(v)is not bytes or not 0<len(v)<=16*1024*1024 for v in (self.release,self.custody,self.pack)) or type(self.output)is not type(Path()):
            raise ValueError('Exact bounded original preparation inputs required')
        if not self.output.is_absolute():raise ValueError('Explicit absolute private output required')

@contextmanager
def _admitted(policy,facts,context,source_admission,original):
    from .puppy_commerce_projection import renew_original_commerce_admission
    renew_original_commerce_admission(source_admission,original)
    policy.admit_exact_commerce(source_snapshot(facts),context)
    primary=None
    try:yield
    except BaseException as error:primary=error
    finish(primary,(lambda:renew_original_commerce_admission(source_admission,original),lambda:policy.renew_exact_commerce(source_snapshot(facts),context)))

def prepare_exact_commerce(config,policy,context,*,source_admission):
    """policy admits genuine current original source before any SDK effects.

    Its hold/admit_exact_commerce/renew_exact_commerce must establish independently
    owned original source/public producer custody. Digests alone never do so.
    """
    if type(config)is not PuppyCommercePrepareConfig:raise ValueError('Typed preparation required')
    config.__post_init__()
    original=original_projection(config.release,config.custody,config.release_sha256,config.custody_sha256)
    expected=exact_scenario_expected(original['model'],original['graph'],config.pack)
    facts={k:original[k]for k in ('nodes','edges','numeric_input_guards','all_unfiltered_intermediate_guards')}
    facts.update(format=PROFILE_EXACT,release_sha256=config.release_sha256,custody_sha256=config.custody_sha256,expected=expected,original_model_base64=base64.b64encode(original['model']).decode(),original_graph_base64=base64.b64encode(original['graph']).decode(),original_bindings_base64=base64.b64encode(original['bindings']).decode(),original_pack_sha256=hashlib.sha256(config.pack).hexdigest())
    with owned_context(policy.hold(context)), owned_context(_admitted(policy,facts,context,source_admission,original)):
        from .puppy_commerce_projection import renew_original_commerce_admission
        def renew():
            renew_original_commerce_admission(source_admission,original)
            policy.renew_exact_commerce(source_snapshot(facts),context)
        if config.output.exists()or config.output.is_symlink():raise ValueError('Fresh private output required')
        parent=config.output.parent
        if parent.resolve()!=parent or parent.is_symlink()or not parent.is_dir():raise ValueError('Existing ordinary unaliased output parent required')
        parent_identity=(parent.stat().st_dev,parent.stat().st_ino)
        import duckdb
        config.output.mkdir(mode=0o700)
        identity=(config.output.stat().st_dev,config.output.stat().st_ino)
        def output_gate():
            if parent.is_symlink()or(parent.stat().st_dev,parent.stat().st_ino)!=parent_identity or config.output.is_symlink()or(config.output.stat().st_dev,config.output.stat().st_ino)!=identity or config.output.stat().st_uid!=os.getuid()or config.output.stat().st_mode&0o777!=0o700:
                raise ValueError('Owned preparation directory custody changed')
        output_gate()
        connection=None;primary=None;commit_attempted=False;database_identity=None
        database=config.output/'commerce-exact.duckdb'
        try:
            output_gate();connection=duckdb.connect(str(database))
            if database.is_symlink()or not database.is_file()or database.stat().st_uid!=os.getuid():raise ValueError('Owned ordinary prepared database required')
            database_identity=(database.stat().st_dev,database.stat().st_ino)
            connection.execute('BEGIN TRANSACTION');connection.execute('CREATE SCHEMA carrier')
            numeric={'quantity_long','amount_coefficient','unit_price_coefficient'}
            for role in ('nodes','edges'):
                output_gate();rows=original[role];names=list(rows[0])
                connection.execute('CREATE TABLE carrier.'+role+'('+','.join('"'+n+'" '+('BIGINT'if n in numeric else'VARCHAR')for n in names)+',carrier_key VARCHAR,carrier_id VARCHAR)')
                connection.executemany('INSERT INTO carrier.'+role+' VALUES('+','.join('?'for _ in names+['carrier_key','carrier_id'])+')',[[r[n]for n in names]+[r['graph_id'],r['id']]for r in rows])
                actual=[dict(zip(names,r))for r in connection.execute('SELECT '+','.join('"'+n+'"'for n in names)+' FROM carrier.'+role).fetchall()]
                from .puppy_commerce_scenarios import bag
                if bag(actual)!=bag(rows):raise ValueError('Complete exact numeric/raw carrier readback differs')
            renew()
            output_gate();commit_attempted=True
            connection.execute('COMMIT')
        except BaseException as error:primary=error
        callbacks=[]
        if connection is not None:
            if primary is not None and not commit_attempted:callbacks.append(lambda:connection.execute('ROLLBACK'))
            callbacks.append(connection.close)
        callbacks.append(lambda:renew())
        finish(primary,callbacks)
        output_gate()
        if database.is_symlink()or not database.is_file()or(database.stat().st_dev,database.stat().st_ino)!=database_identity or database.stat().st_uid!=os.getuid()or database.stat().st_size>32*1024*1024:raise ValueError('Owned bounded prepared database custody differs')
        database.chmod(0o444)
        before=database.stat();database_raw=database.read_bytes();after=database.stat()
        if database.is_symlink()or(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)or len(database_raw)!=after.st_size:raise ValueError('Closing prepared database bytes changed')
        facts['database_sha256']=hashlib.sha256(database_raw).hexdigest()
        # Full digest remains authoritative correspondence; short catalog label
        # is only a native spelling and never grants release selection.
        catalog='ashlar_'+config.release_sha256[:24]
        model={'catalogs':[{'name':catalog,'type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:'+str(database)}}],'node':[],'edge':[]}
        for kind,role in [('node','nodes'),('edge','edges')]:
            names=list(original[role][0]);attrs=[n for n in names if n not in ('id','graph_id')]+['carrier_key','carrier_id'];key=kind+'_key'
            entry={'label':'ExactCommerce'+('Node'if kind=='node'else'Edge'),'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':catalog,'schema':'carrier','table':role,'mappedField':[{'sourceFieldName':n,'targetFieldName':n}for n in attrs]+[{'sourceFieldName':'graph_id','targetFieldName':key}]}},'id':[{'name':key,'type':'STRING'}],'attribute':[{'name':n,'type':'LONG'if n in numeric else'STRING'}for n in attrs]}
            if kind=='edge':entry.update(fromNodeLabel='ExactCommerceNode',toNodeLabel='ExactCommerceNode',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
            model[kind].append(entry)
        for name,value in [('model.json',model),('receipt.json',facts)]:
            output_gate()
            raw=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
            stream=None;error=None
            try:
                stream=(config.output/name).open('xb')
                if stream.write(raw)!=len(raw):raise ValueError('Incomplete owned preparation receipt')
                stream.flush();os.fsync(stream.fileno())
            except BaseException as caught:error=caught
            finish(error,(()if stream is None else(stream.close,)))
            output_gate();(config.output/name).chmod(0o444)
        renew()
    return facts
