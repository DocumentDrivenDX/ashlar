import os,selectors,signal,subprocess,time,stat
def select_failure(primary,closing):
    if primary is None:return closing
    if isinstance(primary,Exception)and not isinstance(closing,Exception):return closing
    return primary

def capture(argv,cwd,env,stdout_path,stderr_path,timeout,maximum,stdin_path=None):
    # No inherited environment. Every stream/process shares the same finite deadline.
    opened=[]; proc=None; sel=None; primary=None; cleanup_error=None; totals=[0,0];input_fd=None;input_stream=None
    try:
        for path in [stdout_path,stderr_path]: opened.append(open(path,'xb'))
        if stdin_path is not None:
            input_fd=os.open(stdin_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            info=os.fstat(input_fd)
            if not stat.S_ISREG(info.st_mode)or info.st_size>16*1024*1024:raise ValueError('input-file-bound')
            input_stream=os.fdopen(input_fd,'rb');input_fd=None;opened.append(input_stream)
        proc=subprocess.Popen(argv,cwd=cwd,env=dict(env),stdin=input_stream if input_stream is not None else subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
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
                if totals[i]>maximum: raise ValueError('Build output bound exceeded')
                opened[i].write(b)
        code=proc.wait(timeout=max(.001,deadline-time.monotonic()))
    except BaseException as e: primary=e
    finally:
        if proc is not None:
            try: os.killpg(proc.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            except BaseException as e:
                cleanup_error=select_failure(cleanup_error,e)
            try: proc.wait(timeout=1)
            except BaseException as e:
                cleanup_error=select_failure(cleanup_error,e)
            for stream in [proc.stdout,proc.stderr]:
                try: stream.close()
                except BaseException as e:
                    cleanup_error=select_failure(cleanup_error,e)
        if sel is not None:
            try: sel.close()
            except BaseException as e:
                cleanup_error=select_failure(cleanup_error,e)
        for f in opened:
            try: f.close()
            except BaseException as e:
                cleanup_error=select_failure(cleanup_error,e)
    if input_fd is not None:
        try:os.close(input_fd)
        except BaseException as e:cleanup_error=select_failure(cleanup_error,e)
    failure=select_failure(primary,cleanup_error) if cleanup_error is not None else primary
    if failure is not None:raise failure
    return {'exitCode':code,'stdoutBytes':totals[0],'stderrBytes':totals[1]}
