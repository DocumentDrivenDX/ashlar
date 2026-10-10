"""Bounded root execution of the separately reviewed offline packaging command."""
import hashlib,json,os,selectors,signal,stat,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
FREEZE_SHA='2cb8af4fd48c45c089fb8010d0c32cf7bc1f1edc4546f62d695621f379d1a24b'
def digest(data):return hashlib.sha256(data).hexdigest()
def verify():
    raw=(ROOT/'freeze.json').read_bytes()
    assert digest(raw)==FREEZE_SHA
    metadata=json.loads(raw)
    for row in metadata['files']:
        data=Path(row['path']).read_bytes()
        assert len(data)==row['bytes'] and digest(data)==row['sha256']
    for row in json.loads(Path(metadata['resources']['path']).read_bytes()):
        size=row['bytes'];assert type(size)is int and 0<=size<=32*1024*1024
        fd=None;primary=None;sha=hashlib.sha256();count=0
        try:
            fd=os.open(row['path'],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            opening=os.fstat(fd);assert stat.S_ISREG(opening.st_mode)and opening.st_size==size
            while True:
                chunk=os.read(fd,min(65536,size+1-count))
                if not chunk:break
                count+=len(chunk);assert count<=size;sha.update(chunk)
            closing=os.fstat(fd)
            assert (opening.st_dev,opening.st_ino,opening.st_size,opening.st_mtime_ns)==(closing.st_dev,closing.st_ino,closing.st_size,closing.st_mtime_ns)
            assert count==size and sha.hexdigest()==row['sha256']
        except BaseException as exc:primary=exc
        finally:
            if fd is not None:
                try:os.close(fd)
                except BaseException as exc:
                    if primary is None:primary=exc
        if primary is not None:raise primary
def phase(command,config):
    name=command['phase'];limits=config['limits'];files={};streams=[];process=None;selector=None;primary=None;result=None
    started=time.monotonic()
    try:
        for key in ('stdout','stderr'):files[key]=(ROOT/(name+'.'+key)).open('xb')
        selector=selectors.DefaultSelector()
        process=subprocess.Popen(command['argv'],cwd=command['cwd'],env=command.get('environment',config['environment']),stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        streams=[process.stdout,process.stderr]
        for stream,key in zip(streams,('stdout','stderr')):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,key)
        counts={'stdout':0,'stderr':0};deadline=started+limits['phaseSeconds']
        while selector.get_map():
            if time.monotonic()>=deadline:raise TimeoutError('phase-limit')
            for entry,_ in selector.select(.1):
                key=entry.data
                try:data=os.read(entry.fileobj.fileno(),min(65536,limits[key+'Bytes']+1-counts[key]))
                except BlockingIOError:continue
                if not data:selector.unregister(entry.fileobj);continue
                counts[key]+=len(data)
                if counts[key]>limits[key+'Bytes']:raise ValueError('capture-limit')
                files[key].write(data)
        code=process.wait(timeout=max(.001,deadline-time.monotonic()))
        result={'phase':name,'exitCode':code,'durationSeconds':time.monotonic()-started,'captureBytes':counts,'captureLimitBytes':{key:limits[key+'Bytes']for key in counts},'environmentKeys':sorted(command.get('environment',config['environment'])),'scope':'Root five-phase offline packaging execution; ordinary bounded files, no durability/OTel/native guarantee.'}
    except BaseException as exc:primary=exc
    finally:
        if process is not None:
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            except BaseException as exc:
                if primary is None:primary=exc
            try:process.wait(timeout=2)
            except BaseException as exc:
                if primary is None:primary=exc
        for item in streams+list(files.values())+([selector]if selector else[]):
            try:item.close()
            except BaseException as exc:
                if primary is None:primary=exc
    if primary is not None:raise primary
    with (ROOT/(name+'-process.json')).open('x')as out:json.dump(result,out,indent=2);out.write('\n')
    if result['exitCode']!=0:raise ValueError('phase-failed')
    print(json.dumps({'phase':name,'exitCode':0,'durationSeconds':result['durationSeconds']}),flush=True)
def main():
    verify();config=json.loads((ROOT/'command.json').read_bytes())
    for name in config['freshPaths']:assert not Path(name).exists()and not Path(name).is_symlink()
    primary=None
    try:
        for command in config['commands']:phase(command,config)
    except BaseException as exc:primary=exc
    finally:
        try:verify()
        except BaseException as exc:
            if primary is None:primary=exc
    if primary is not None:raise primary
if __name__=='__main__':main()
