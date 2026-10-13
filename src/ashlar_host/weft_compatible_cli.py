"""One observed local-source candidate CLI; original bytes confer no authority."""
from dataclasses import dataclass
from pathlib import Path
import hashlib,os,platform,selectors,stat,subprocess,sys,tempfile,time
PROFILE='ashlar-local-source-compatible-compiler/0.1'
SOURCE_REVISION='cc17246e8b7e05ff61262baa95d373ea0ca640a1'
BINARY_SHA256='23cd1b3c713faca6119ad189341f03ca035b482c34e507f19dd60adfdf9b1e82'
BINARY_BYTES=16838512
PROTOCOL_LIMIT=16*1048576

class CompatibleCompilerError(ValueError):
    """Payload-free local-candidate failure category."""

@dataclass(frozen=True)
class CompatibleCompiler:
    binary:Path

def _binary(path):
    if not isinstance(path,Path) or not path.is_absolute()or path.resolve()!=path:raise CompatibleCompilerError('invalid-compiler')
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(descriptor,'rb')as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode)or before.st_size!=BINARY_BYTES:raise CompatibleCompilerError('compiler-refused')
        raw=stream.read(BINARY_BYTES+1);after=os.fstat(stream.fileno())
    if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)or len(raw)!=BINARY_BYTES or hashlib.sha256(raw).hexdigest()!=BINARY_SHA256:raise CompatibleCompilerError('compiler-refused')
    return raw

def _compile(binary,request):
    process=None
    try:
        process=subprocess.Popen([str(binary)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout=bytearray();stderr=bytearray();position=0;deadline=time.monotonic()+15
        with selectors.DefaultSelector()as selector:
            for stream,target in ((process.stdout,stdout),(process.stderr,stderr)):
                os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,target)
            os.set_blocking(process.stdin.fileno(),False)
            if request:selector.register(process.stdin,selectors.EVENT_WRITE,None)
            else:process.stdin.close()
            while selector.get_map():
                remaining=deadline-time.monotonic()
                if remaining<=0:raise CompatibleCompilerError('compiler-timeout')
                for key,mask in selector.select(remaining):
                    stream=key.fileobj
                    if mask&selectors.EVENT_WRITE:
                        count=os.write(stream.fileno(),request[position:position+65536]);position+=count
                        if position==len(request):selector.unregister(stream);stream.close()
                    else:
                        target=key.data;maximum=PROTOCOL_LIMIT if target is stdout else 4096
                        chunk=os.read(stream.fileno(),min(65536,maximum+1-len(target)))
                        if not chunk:selector.unregister(stream);stream.close()
                        else:
                            target.extend(chunk)
                            if len(target)>maximum:raise CompatibleCompilerError('compiler-output-bound')
            remaining=deadline-time.monotonic()
            if remaining<=0:raise CompatibleCompilerError('compiler-timeout')
            if process.wait(timeout=remaining)!=0 or stderr:raise CompatibleCompilerError('compiler-refused')
        raw=bytes(stdout)
        if not raw.endswith(b'\n')or raw.count(b'\n')!=1:raise CompatibleCompilerError('compiler-protocol-refused')
        from .private_graph_custody import decode_custody_bytes
        if type(decode_custody_bytes(raw))is not dict:raise CompatibleCompilerError('compiler-protocol-refused')
        return raw
    finally:
        if process is not None:
            if process.poll()is None:process.kill();process.wait(timeout=5)
            for stream in (process.stdin,process.stdout,process.stderr):
                if stream is not None and not stream.closed:stream.close()

def compile_compatible(compiler,request_bytes):
    """Compile exact bytes with a private pinned executable; no guard discharge."""
    try:
        if type(compiler)is not CompatibleCompiler or type(request_bytes)is not bytes or len(request_bytes)>PROTOCOL_LIMIT:raise CompatibleCompilerError('invalid-request')
        if sys.platform!='darwin'or platform.machine()!='arm64'or platform.mac_ver()[0]!='27.0.1':raise CompatibleCompilerError('unsupported-platform')
        raw=_binary(compiler.binary)
        with tempfile.TemporaryDirectory(prefix='ashlar-compatible-compiler-')as directory:
            root=Path(directory).resolve();root.chmod(0o700);binary=root/'weft-runtime'
            with os.fdopen(os.open(binary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o700),'wb')as stream:
                if stream.write(raw)!=len(raw):raise CompatibleCompilerError('compiler-copy-refused')
                stream.flush();os.fsync(stream.fileno())
            if _binary(binary)!=raw:raise CompatibleCompilerError('compiler-copy-refused')
            result=_compile(binary,request_bytes)
            if _binary(binary)!=raw:raise CompatibleCompilerError('compiler-copy-refused')
            return result
    except CompatibleCompilerError:raise
    except (OSError,ValueError,TypeError,RecursionError,subprocess.SubprocessError):raise CompatibleCompilerError('compiler-refused')from None
