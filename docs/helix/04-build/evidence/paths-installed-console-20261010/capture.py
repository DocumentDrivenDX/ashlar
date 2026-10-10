"""Root-granted installed CLI capture; no engine or native support inference."""
import hashlib,json,os,pathlib,selectors,signal,stat,subprocess,time,sys
ROOT=pathlib.Path(__file__).resolve().parent

def digest(path):
    h=hashlib.sha256();size=0
    fd=None;primary=None
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(fd).st_mode):raise ValueError('resource-kind')
        while True:
            raw=os.read(fd,65536)
            if not raw:break
            size+=len(raw)
            if size>32*1024*1024:raise ValueError('resource-bound')
            h.update(raw)
    except BaseException as exc:primary=exc
    finally:
        if fd is not None:
            try:os.close(fd)
            except BaseException as exc:
                if primary is None:primary=exc
                else:
                    try:setattr(primary,'cleanup_failed',True)
                    except BaseException:pass
    if primary is not None:raise primary
    return h.hexdigest(),size

def capture(argv,cwd,env,stdout_path,stderr_path,timeout,maximum,stdin_path=None):
    # No inherited environment. Every stream/process shares the same finite deadline.
    opened=[]; proc=None; sel=None; primary=None; cleanup=None; totals=[0,0]; stdin_fd=None
    try:
        for path in [stdout_path,stderr_path]: opened.append(open(path,'xb'))
        if stdin_path is not None:
            stdin_fd=os.open(stdin_path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
            if not stat.S_ISREG(os.fstat(stdin_fd).st_mode) or os.fstat(stdin_fd).st_size>16*1024*1024:raise ValueError('stdin-bound')
        proc=subprocess.Popen(argv,cwd=cwd,env=dict(env),stdin=stdin_fd if stdin_fd is not None else subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        sel=selectors.DefaultSelector()
        for i,s in enumerate([proc.stdout,proc.stderr]): os.set_blocking(s.fileno(),False); sel.register(s,selectors.EVENT_READ,i)
        deadline=time.monotonic()+timeout
        while sel.get_map() or proc.poll() is None:
            left=deadline-time.monotonic()
            if left<=0: raise TimeoutError('Build deadline exceeded')
            for key,_ in sel.select(min(left,.1)):
                b=os.read(key.fileobj.fileno(),65536)
                if not b: sel.unregister(key.fileobj); continue
                i=key.data; totals[i]+=len(b)
                if totals[i]>maximum[i]: raise ValueError('Build output bound exceeded')
                opened[i].write(b)
        code=proc.wait(timeout=max(.001,deadline-time.monotonic()))
    except BaseException as e: primary=e
    finally:
        if stdin_fd is not None:
            try:os.close(stdin_fd)
            except BaseException as exc:
                if cleanup is None:cleanup=exc
        if proc is not None:
            try: os.killpg(proc.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            except BaseException as exc:
                if cleanup is None:cleanup=exc
            try: proc.wait(timeout=1)
            except BaseException as exc:
                if cleanup is None:cleanup=exc
            for stream in [proc.stdout,proc.stderr]:
                try: stream.close()
                except BaseException as exc:
                    if cleanup is None:cleanup=exc
        if sel is not None:
            try: sel.close()
            except BaseException as exc:
                if cleanup is None:cleanup=exc
        for f in opened:
            try: f.close()
            except BaseException as exc:
                if cleanup is None:cleanup=exc
    if primary is not None:
        try: primary.cleanup_failed=cleanup is not None
        except BaseException: pass
        raise primary
    if cleanup is not None: raise cleanup
    return {'exitCode':code,'stdoutBytes':totals[0],'stderrBytes':totals[1]}

def main():
    raw=(ROOT/'command.json').read_bytes()
    if len(raw)>4*1024*1024 or hashlib.sha256(raw).hexdigest()!=sys.argv[1]:raise ValueError('command-pin')
    command=json.loads(raw);primary=None;result={'status':'incomplete'}
    def verify():
        for d in command['resources']:
            if digest(d['path'])!=(d['sha256'],d['bytes']):raise ValueError('resource-drift')
    try:
        verify()
        for output in command['freshOutputs']:
            p=pathlib.Path(output)
            if p.exists()or p.is_symlink():raise ValueError('fresh-output')
        records=[];installed_opening=None
        def installed_files():
            names=['weft-paths','ready.json','provenance.json','backend-manifest.json']+['schemas/'+name for name in ['compile-request-v0.4.schema.json','compile-response-v0.4.schema.json','logical-plan-v0.4.schema.json','application-result-v0.4.schema.json','backend-manifest-v0.3.schema.json']]
            result=[]
            for name in names:
                hashed,size=digest(pathlib.Path(command['installation'])/name)
                result.append({'path':name,'sha256':hashed,'bytes':size})
            return result
        for phase in command['phases']:
            stdout=str(ROOT/(phase['id']+'.stdout'));stderr=str(ROOT/(phase['id']+'.stderr'))
            outcome=capture(phase['argv'],str(ROOT),command['environment'],stdout,stderr,command['limits']['seconds'],(command['limits']['stdoutBytes'],command['limits']['stderrBytes']),phase.get('stdin'))
            if outcome['exitCode']!=0:raise ValueError('cli-refusal')
            actual=pathlib.Path(stdout).read_bytes();diagnostic=pathlib.Path(stderr).read_bytes()
            if phase['id']=='install':
                if actual or diagnostic!=b'ashlar-weft-paths: installed\n':raise ValueError('installation-outcome')
            elif diagnostic or actual!=pathlib.Path(phase['expected']).read_bytes():raise ValueError('exact-response')
            files=installed_files()
            if installed_opening is None:installed_opening=files
            elif files!=installed_opening:raise ValueError('installation-drift')
            records.append({'id':phase['id'],**outcome,'stdoutSha256':hashlib.sha256(actual).hexdigest(),'stderrSha256':hashlib.sha256(diagnostic).hexdigest(),'exactExpected':True})
        result={'status':'pending-closing','phases':records,'openingClosingCustody':False,'installedFilesOpeningClosing':installed_opening,'scope':'Actual installed CLI/indexed compiler transport only; no native engine/source/publication/ACK qualification.'}
    except BaseException as exc:primary=exc
    finally:
        try:verify()
        except BaseException as exc:
            result['status']='incomplete'
            result['closingCustodyFailed']=True
            if primary is None:primary=exc
            else:
                try:setattr(primary,'cleanup_failed',True)
                except BaseException:pass
    if primary is None:
        result['status']='passed'
        result['openingClosingCustody']=True
    else:result['status']='incomplete'
    try:
        payload=(json.dumps(result,sort_keys=True,indent=2)+'\n').encode()
        with (ROOT/'outcome.json').open('xb')as stream:stream.write(payload)
    except BaseException:
        if primary is None:raise
        try:setattr(primary,'cleanup_failed',True)
        except BaseException:pass
    if primary is not None:raise primary
    print(json.dumps({'status':'passed','phases':len(result['phases'])}))
if __name__=='__main__':main()
