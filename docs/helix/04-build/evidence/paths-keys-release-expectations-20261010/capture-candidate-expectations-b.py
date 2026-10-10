"""Bounded expectation capture only; output requires independent acceptance."""
import argparse, hashlib, json, os, pathlib, selectors, signal, subprocess, time
def capture(argv,cwd,env,stdout_path,stderr_path,timeout,maximum,stdin,maximum_total):
    # No inherited environment. Every stream/process shares the same finite deadline.
    opened=[]; proc=None; sel=None; primary=None; cleanup_error=None; totals=[0,0]
    try:
        for path in [stdout_path,stderr_path]: opened.append(open(path,'xb'))
        proc=subprocess.Popen(argv,cwd=cwd,env=dict(env),stdin=stdin,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        sel=selectors.DefaultSelector()
        for i,s in enumerate([proc.stdout,proc.stderr]): os.set_blocking(s.fileno(),False); sel.register(s,selectors.EVENT_READ,i)
        deadline=time.monotonic()+timeout
        while sel.get_map() or proc.poll() is None:
            left=deadline-time.monotonic()
            if left<=0: raise TimeoutError('Build deadline exceeded')
            for key,_ in sel.select(min(left,.1)):
                b=os.read(key.fileobj.fileno(),min(65536,maximum+1-totals[key.data],maximum_total+1-sum(totals)))
                if not b: sel.unregister(key.fileobj); continue
                i=key.data; totals[i]+=len(b)
                if totals[i]>maximum or sum(totals)>maximum_total: raise ValueError('Capture output bound exceeded')
                opened[i].write(b)
        code=proc.wait(timeout=max(.001,deadline-time.monotonic()))
    except BaseException as e: primary=e
    finally:
        if proc is not None:
            try: os.killpg(proc.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            except BaseException as e:
                if cleanup_error is None: cleanup_error=e
            try: proc.wait(timeout=1)
            except BaseException as e:
                if cleanup_error is None: cleanup_error=e
            for stream in [proc.stdout,proc.stderr]:
                try: stream.close()
                except BaseException as e:
                    if cleanup_error is None: cleanup_error=e
        if sel is not None:
            try: sel.close()
            except BaseException as e:
                if cleanup_error is None: cleanup_error=e
        for f in opened:
            try: f.close()
            except BaseException as e:
                if cleanup_error is None: cleanup_error=e
    if primary is not None:
        try: primary.cleanup_failed=cleanup_error is not None
        except BaseException: pass
        raise primary
    if cleanup_error is not None: raise cleanup_error
    return {'exitCode':code,'stdoutBytes':totals[0],'stderrBytes':totals[1]}

def sha(path):
    with path.open('rb') as stream:
        digest=hashlib.sha256()
        for block in iter(lambda:stream.read(1048576),b''):digest.update(block)
    return digest.hexdigest()
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--command',type=pathlib.Path,required=True);parser.add_argument('--sha256',required=True);args=parser.parse_args()
    if sha(args.command)!=args.sha256:raise ValueError('command pin')
    plan=json.loads(args.command.read_bytes())
    resources=plan['resources']
    def verify():
        for item in resources:
            path=pathlib.Path(item['path'])
            if path.is_symlink() or not path.is_file() or path.stat().st_size!=item['bytes'] or sha(path)!=item['sha256']:raise ValueError('resource custody')
    verify();output=pathlib.Path(plan['output']);output.mkdir(exist_ok=False)
    primary=None;records=[];total=0;deadline=time.monotonic()+plan['totalDeadlineSeconds']
    try:
        for item in plan['requests']:
            request=pathlib.Path(item['path']);stdout=output/(item['name']+'.response.json');stderr=output/(item['name']+'.stderr')
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('total capture deadline')
            stream=None;input_primary=None;input_cleanup=None
            try:
                stream=request.open('rb')
                result=capture([plan['binary']],plan['cwd'],plan['environment'],stdout,stderr,min(remaining,plan['perCaseDeadlineSeconds']),plan['maximumStreamBytes'],stream,plan['maximumTotalOutputBytes']-total)
            except BaseException as error:input_primary=error
            finally:
                if stream is not None:
                    try:stream.close()
                    except BaseException as error:input_cleanup=error
            if input_primary is not None:
                if input_cleanup is not None:
                    try:input_primary.input_cleanup_failed=True
                    except BaseException:pass
                raise input_primary
            if input_cleanup is not None:raise input_cleanup
            total+=result['stdoutBytes']+result['stderrBytes']
            if total>plan['maximumTotalOutputBytes']:raise ValueError('total capture bound')
            if result['exitCode']!=0 or result['stderrBytes']!=0:raise ValueError('compiler transport')
            records.append(dict(item,result=result,responseSha256=sha(stdout),scope='Unaccepted candidate expectation; requires independent payload review'))
    except BaseException as error:primary=error
    finally:
        try:verify()
        except BaseException as error:
            if primary is None:primary=error
            else:
                try:primary.closing_custody_failed=True
                except BaseException:pass
    if primary is not None:raise primary
    with (output/'capture.json').open('x') as stream:json.dump({'commandSha256':args.sha256,'records':records,'openingClosingCustody':True,'environmentInherited':False,'scope':'Actual candidate responses only, no qualification acceptance/native support/index authority'},stream,indent=2)
if __name__=='__main__':main()
