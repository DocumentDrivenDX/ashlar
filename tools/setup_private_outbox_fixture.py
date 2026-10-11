"""Separately authorized fresh local fixture, never evolution-provider bootstrap.

Only the explicitly selected cached PostgreSQL image and new loopback container
are supported. Authentication is intentionally trust for this isolated test
fixture. No existing credentials, resources, production authority or Truss are
read or inferred. Creation is an explicit operator action; importing is inert.
"""
import argparse
from dataclasses import dataclass, asdict
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import time
from ashlar_host.ack import AckScope, render_ddl
from ashlar_host.lifecycle import finish
from ashlar_host.resources import RESOURCE_ROOT

IMAGE = 'postgres@sha256:2a0d0fe14825b0939f78a8cad5cd4e6aa68bf94d0e5dd96e24b6d23af4315545'
LIMIT = 1024 * 1024


def identifier(value, prefix=None):
    if (type(value) is not str or not re.fullmatch('[a-z][a-z0-9_]{0,62}', value)
            or prefix is not None and not value.startswith(prefix)):
        raise ValueError('Explicit safe fixture identifier required')


@dataclass(frozen=True)
class Source:
    scope: AckScope
    schema: str
    writer: str
    reader: str
    ack: str

    def __post_init__(self):
        if type(self.scope) is not AckScope:
            raise ValueError('Exact original source scope required')
        values=asdict(self.scope)
        if any(type(v) is not str for v in values.values()):raise ValueError('Exact scope text required')
        object.__setattr__(self,'scope',AckScope(**values))
        identifier(self.schema,'ashlar_ack_source_')
        for role in (self.writer,self.reader,self.ack):identifier(role,'ashlar_ack_')
        identifier(self.scope.service_schema,'ashlar_ack_')
        if len(set((self.writer,self.reader,self.ack,self.schema,self.scope.service_schema)))!=5:
            raise ValueError('Separate original namespaces and ordinary roles required')


@dataclass(frozen=True)
class Settings:
    docker: Path
    docker_host: str
    image: str
    container: str
    port: int
    database: str
    authentication: str
    sources: tuple[Source,...]
    output: Path

    def __post_init__(self):
        if type(self.docker) is not type(Path()) or not self.docker.is_absolute():raise ValueError('Explicit Docker executable required')
        if (type(self.docker_host) is not str or not self.docker_host.startswith('unix:///')
                or any(c in self.docker_host for c in ('\x00','\n','\r','?','#'))
                or str(Path(self.docker_host[7:]))!=self.docker_host[7:]
                or '..' in Path(self.docker_host[7:]).parts):raise ValueError('Explicit local Docker Unix socket required')
        if type(self.image) is not str or self.image!=IMAGE:raise ValueError('Exact cached PG17.9 image required')
        if type(self.container) is not str or not re.fullmatch('ashlar-private-outbox-[a-z0-9-]{1,40}',self.container):raise ValueError('New dedicated fixture container required')
        if type(self.port) is not int or not 1024<=self.port<=65535:raise ValueError('Exact loopback port required')
        identifier(self.database)
        if self.database in ('postgres','template0','template1'):raise ValueError('Dedicated empty fixture database required')
        if type(self.authentication) is not str or self.authentication!='isolated-loopback-trust':raise ValueError('Explicit test authentication required')
        if type(self.sources) is not tuple or len(self.sources)!=2 or any(type(s) is not Source for s in self.sources):raise ValueError('Two exact independent source definitions required')
        for source in self.sources:source.__post_init__()
        identities=[s.scope.feed for s in self.sources]
        names=[n for s in self.sources for n in (s.schema,s.writer,s.reader,s.ack,s.scope.service_schema)]
        if len(set(identities))!=2 or len(set(names))!=10 or len({s.scope.scope_id for s in self.sources})!=2 or len({s.scope.installation_id for s in self.sources})!=2:raise ValueError('Distinct original source scopes and roles required')
        if (type(self.output) is not type(Path()) or not self.output.is_absolute()
                or self.output.parent.resolve()!=self.output.parent or not self.output.parent.is_dir()):raise ValueError('Explicit canonical exclusive inventory directory required')


def ddl(source):
    if type(source) is not Source:raise ValueError('Exact source required')
    source.__post_init__()
    original=(RESOURCE_ROOT/'sql/ashlar-outbox/01-postgresql.sql').read_text()
    replacements={'ashlar_outbox_writer':source.writer,'ashlar_outbox_reader':source.reader,'ashlar_outbox':source.schema}
    outbox=re.sub(r'\bashlar_outbox(?:_writer|_reader)?\b',lambda m:replacements[m.group()],original)
    ack=render_ddl(source.scope.service_schema,source.ack)
    # Complete role attributes are set before installation/signature capture.
    login='\n'.join('ALTER ROLE '+role+' LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;' for role in (source.writer,source.reader,source.ack))
    return outbox,ack,login


class Runtime:
    """Explicit subprocess and ordinary direct-login connection ports."""
    def command(self, argv, timeout=30):
        selector=None;primary=None;data={'stdout':bytearray(),'stderr':bytearray()}
        child=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        try:
            selector=selectors.DefaultSelector()
            for stream,name in ((child.stdout,'stdout'),(child.stderr,'stderr')):
                os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,name)
            end=time.monotonic()+timeout
            while selector.get_map():
                if time.monotonic()>=end:raise TimeoutError('Fixture command deadline')
                for key,_ in selector.select(min(.1,end-time.monotonic())):
                    raw=os.read(key.fileobj.fileno(),65536)
                    if not raw:selector.unregister(key.fileobj);continue
                    if len(data[key.data])+len(raw)>LIMIT:raise ValueError('Fixture command output capacity')
                    data[key.data].extend(raw)
            child.wait(timeout=max(.001,end-time.monotonic()))
            if child.returncode!=0:raise ValueError('Fixture command refused')
        except BaseException as error:primary=error
        finally:
            def stop():
                if child.poll() is None:
                    try:os.killpg(child.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
            finish(primary,[stop,lambda:child.wait(timeout=5)] + ([selector.close] if selector is not None else []) + [child.stdout.close,child.stderr.close])
        return bytes(data['stdout'])

    def connect(self, settings, role):
        import psycopg
        return psycopg.connect(host='127.0.0.1',port=settings.port,dbname=settings.database,
            user=role,password='',passfile='/dev/null',sslmode='disable',gssencmode='disable',
            connect_timeout=2,options='-c statement_timeout=5000 -c lock_timeout=1000')


def connection_operation(connection, operation, *, commit=False):
    primary=None;value=None
    try:
        value=operation(connection)
        if commit:connection.commit()
    except BaseException as error:primary=error
    finish(primary,([connection.rollback] if primary is not None or not commit else [])+[connection.close])
    return value


def write_inventory(directory_fd, name, value):
    body=json.dumps(value,sort_keys=True,separators=(',',':')).encode()+b'\n'
    if len(body)>LIMIT:raise ValueError('Fixture inventory capacity')
    fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory_fd)
    primary=None
    try:
        offset=0
        while offset<len(body):
            written=os.write(fd,body[offset:])
            if written<=0:raise OSError('Fixture inventory incomplete')
            offset+=written
        os.fsync(fd)
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])
    os.fsync(directory_fd)


def ordinary_identity(connection, role, database):
    row=connection.execute('SELECT current_user,session_user,current_database(),rolcanlogin,'
        'rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls '
        'FROM pg_catalog.pg_roles WHERE rolname=current_user').fetchone()
    expected=(role,role,database,True,False,False,False,False,False)
    if type(row) is not tuple or len(row)!=len(expected) or any(type(a) is not type(b) or a!=b for a,b in zip(row,expected)):
        raise ValueError('Direct nonprivileged ordinary LOGIN facts differ')
    return {'role':role,'database':database,'login':True,'privileged':False}


def setup(settings, runtime):
    if type(settings) is not Settings or type(runtime) is not Runtime:raise ValueError('Exact fixture settings/runtime required')
    settings.__post_init__()
    if settings.output.exists() or settings.output.is_symlink():raise ValueError('Exclusive new fixture output required')
    parent_fd=os.open(settings.output.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    directory_fd=None;directory_identity=None;parent_identity=None
    primary=None;container_id=None;complete=False;result=None
    docker=[str(settings.docker),'--host',settings.docker_host]
    try:
        parent_identity=os.fstat(parent_fd)
        def renew_output():
            current=settings.output.parent.lstat();held=os.fstat(parent_fd)
            original=(parent_identity.st_dev,parent_identity.st_ino)
            if (not stat.S_ISDIR(current.st_mode) or settings.output.parent.resolve()!=settings.output.parent
                    or (current.st_dev,current.st_ino)!=original or (held.st_dev,held.st_ino)!=original):
                raise ValueError('Original canonical output parent changed')
            if directory_fd is not None:
                current=settings.output.lstat();held=os.fstat(directory_fd)
                relative=os.stat(settings.output.name,dir_fd=parent_fd,follow_symlinks=False)
                original=(directory_identity.st_dev,directory_identity.st_ino)
                if (not stat.S_ISDIR(current.st_mode) or stat.S_IMODE(current.st_mode)!=0o700
                        or current.st_uid!=os.getuid() or (current.st_dev,current.st_ino)!=original
                        or (relative.st_dev,relative.st_ino)!=original or (held.st_dev,held.st_ino)!=original):
                    raise ValueError('Original private output changed')
        renew_output()
        # Inspect only nonsecret cached image fields on the selected local socket.
        image=json.loads(runtime.command(docker+['image','inspect',settings.image,'--format',
            '{"digests":{{json .RepoDigests}},"architecture":{{json .Architecture}},"os":{{json .Os}}}']))
        if type(image) is not dict or set(image)!= {'digests','architecture','os'} or type(image['digests']) is not list or settings.image not in image['digests'] or image['architecture']!='arm64' or image['os']!='linux':raise ValueError('Exact cached image identity required')
        renew_output()
        os.mkdir(settings.output.name,mode=0o700,dir_fd=parent_fd)
        directory_identity=os.stat(settings.output.name,dir_fd=parent_fd,follow_symlinks=False)
        if (not stat.S_ISDIR(directory_identity.st_mode) or stat.S_IMODE(directory_identity.st_mode)!=0o700
                or directory_identity.st_uid!=os.getuid()):
            raise ValueError('Original private created output required')
        renew_output()
        directory_fd=os.open(settings.output.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent_fd)
        renew_output()
        raw=runtime.command(docker+['run','--detach','--pull=never','--cpus','1','--memory','512m','--name',settings.container,
            '--publish','127.0.0.1:'+str(settings.port)+':5432','--label','ashlar.purpose=private-outbox-fixture',
            '--env','POSTGRES_HOST_AUTH_METHOD=trust','--env','POSTGRES_DB='+settings.database,settings.image])
        candidate=raw.decode('ascii').strip()
        if not re.fullmatch('[0-9a-f]{64}',candidate):raise ValueError('Exact newly created container identity required')
        container_id=candidate
        cleanup=docker+['rm','--force','--volumes',container_id]
        renew_output()
        write_inventory(directory_fd,'created-container.json',{'container':settings.container,'container_id':container_id,'image':settings.image,'cleanup_argv':cleanup,'qualification':'Owned new fixture handle only; setup not yet complete.'})
        admin=None;deadline=time.monotonic()+30
        while admin is None:
            try:admin=runtime.connect(settings,'postgres')
            except Exception:
                if time.monotonic()>=deadline:raise TimeoutError('Fresh fixture readiness deadline') from None
                time.sleep(.1)
        def initialize(c):
            if c.autocommit is not False:raise ValueError('Owned non-autocommit setup transaction required')
            if c.execute('SELECT current_user,session_user,current_database()').fetchone()!=('postgres','postgres',settings.database):raise ValueError('Fresh setup session identity differs')
            version=c.execute("SELECT current_setting('server_version')").fetchone()
            if type(version) is not tuple or len(version)!=1 or type(version[0]) is not str or not re.fullmatch(r'17\.9(?: .*)?',version[0]):
                raise ValueError('Selected PostgreSQL17.9 server required')
            count=c.execute("SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'").fetchone()
            if type(count) is not tuple or len(count)!=1 or type(count[0]) is not int or count!=(0,):raise ValueError('New empty installation required')
            ddls=[]
            for source in settings.sources:
                for statement in ddl(source):c.execute(statement);ddls.append(hashlib.sha256(statement.encode()).hexdigest())
                scope=source.scope;prefix=scope.service_schema
                c.execute('SELECT '+prefix+'.register_source(%s::uuid,%s::name)',(scope.installation_id,source.schema))
                c.execute('SELECT '+prefix+'.register_consumer(%s::uuid,%s::uuid,%s,%s,%s)',(scope.scope_id,scope.installation_id,scope.consumer,scope.feed,scope.epoch))
            return ddls
        ddl_hashes=connection_operation(admin,initialize,commit=True)
        # Admin is closed before any ordinary provider connection can be used.
        sources=[];identities=[]
        for source in settings.sources:
            identities.append(connection_operation(runtime.connect(settings,source.writer),lambda c:ordinary_identity(c,source.writer,settings.database)))
            def observe(c):
                if c.autocommit is not False:raise ValueError('Fresh ordinary transaction required')
                identities.append(ordinary_identity(c,source.ack,settings.database))
                binding=c.execute('SELECT '+source.scope.service_schema+'.scope_binding(%s::uuid)',(source.scope.scope_id,)).fetchone()[0]
                expected=asdict(source.scope);expected.pop('service_schema');expected['source_schema']=source.schema
                if (type(binding) is not dict or set(binding)!=set(expected)|{'source_signature_sha256'} or any(type(v) is not str for v in binding.values()) or any(binding[k]!=v for k,v in expected.items()) or not re.fullmatch('[0-9a-f]{64}',binding['source_signature_sha256'])):raise ValueError('Actual original source binding differs')
                return binding
            binding=connection_operation(runtime.connect(settings,source.ack),observe)
            def read(c):
                identities.append(ordinary_identity(c,source.reader,settings.database))
                head=c.execute('SELECT position::text FROM '+source.schema+'.head WHERE id=1').fetchone()
                count=c.execute('SELECT count(*) FROM '+source.schema+'.batch').fetchone()
                if type(head) is not tuple or len(head)!=1 or type(head[0]) is not str or head!=('0',) or type(count) is not tuple or len(count)!=1 or type(count[0]) is not int or count!=(0,):raise ValueError('Fresh source is not empty')
            connection_operation(runtime.connect(settings,source.reader),read)
            sources.append({'scope':asdict(source.scope),'schema':source.schema,'writer_role':source.writer,'reader_role':source.reader,'ack_role':source.ack,'binding':binding})
        result={'format':'ashlar-private-outbox-fixture/0.1','container':settings.container,'container_id':container_id,
            'image':image,'docker_host':settings.docker_host,'capacity':{'cpus':1,'memory_bytes':512*1024*1024},'endpoint':{'host':'127.0.0.1','port':settings.port,'database':settings.database},
            'authentication':settings.authentication,'cleanup_argv':cleanup,'ordinary_sessions':identities,'sources':sources,'ddl_sha256':ddl_hashes,
            'qualification':'Fresh empty private local development source setup only. Trust login establishes local session identity, not remotely authenticated role authority; no Delta publication, ACK progress, Truss or production authority.'}
        renew_output()
        write_inventory(directory_fd,'inventory.json',result)
        renew_output()
        complete=True
    except BaseException as error:primary=error
    finish(primary,([lambda:runtime.command(docker+['rm','--force','--volumes',container_id])] if container_id is not None and not complete else [])+([lambda:os.close(directory_fd)] if directory_fd is not None else [])+[lambda:os.close(parent_fd)])
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--configuration',required=True,type=Path)
    args=parser.parse_args()
    stream=args.configuration.open('rb');primary=None;raw=None
    try:
        raw=stream.read(65537)
        if len(raw)>65536:raise ValueError('Fixture settings capacity')
    except BaseException as error:primary=error
    finish(primary,[stream.close])
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise ValueError('Duplicate fixture configuration key')
            result[k]=v
        return result
    def integer(text):
        if not re.fullmatch('[0-9]{1,5}',text):raise ValueError('Bounded fixture integer required')
        return int(text)
    value=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite fixture configuration')))
    if type(value) is not dict or set(value)!= {'docker','docker_host','image','container','port','database','authentication','sources','output'}:raise ValueError('Closed fixture settings required')
    sources=[]
    if type(value['sources']) is not list or len(value['sources'])!=2:raise ValueError('Two source definitions required')
    for selected in value['sources']:
        if type(selected) is not dict or set(selected)!= {'scope','schema','writer','reader','ack'}:raise ValueError('Closed source settings required')
        sources.append(Source(AckScope(**selected['scope']),selected['schema'],selected['writer'],selected['reader'],selected['ack']))
    settings=Settings(Path(value['docker']),value['docker_host'],value['image'],value['container'],value['port'],value['database'],value['authentication'],tuple(sources),Path(value['output']))
    setup(settings,Runtime())
    print('ashlar-private-outbox: fixture inventory retained')


if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('ashlar-private-outbox: refused') from None
