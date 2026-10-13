"""Foreground ownership of an existing initialized local Truss PostgreSQL server."""
import json,os,re,signal,threading
from pathlib import Path
from .lifecycle import finish


def selected_directory(data_directory):
    path=Path(data_directory)
    version=path/'PG_VERSION'
    if not path.is_absolute()or path.resolve()!=path or not path.is_dir()or version.is_symlink()or not version.is_file():raise ValueError('Existing canonical initialized PostgreSQL data directory required')
    with version.open('rb')as stream:raw=stream.read(33)
    if len(raw)>32 or re.fullmatch(rb'[0-9]+(?:\.[0-9]+)?\n?',raw)is None:raise ValueError('Bounded original PostgreSQL version marker required')
    if (path/'postmaster.pid').exists()or (path/'postmaster.pid').is_symlink():raise ValueError('Selected data directory already has a server marker; inspect ownership before recovery')
    return path


def _serve(directory,runtime_factory,wait,*,connection_output=None):
    """Own one public runtime; test seams do not create a background service."""
    runtime=runtime_factory(directory);entered=False;primary=None
    try:
        runtime.__enter__();entered=True
        if connection_output is None:
            print(json.dumps({'connection_uri':runtime.info.connection_uri},separators=(',',':')),flush=True)
        else:
            raw=(json.dumps({'profile':'ashlar-local-truss-connection/0.1','data_directory':str(directory),'connection_uri':runtime.info.connection_uri})+'\n').encode()
            with os.fdopen(os.open(connection_output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb')as stream:
                if stream.write(raw)!=len(raw):raise OSError('Incomplete private connection output')
                stream.flush();os.fsync(stream.fileno())
            print(json.dumps({'output':str(connection_output)},separators=(',',':')),flush=True)
        try:wait()
        except KeyboardInterrupt:pass # Normal foreground user shutdown.
    except BaseException as error:primary=error
    finish(primary,[lambda:runtime.__exit__(None,None,None)]if entered else [])


def run_serve_truss_local_command(data_directory,*,connection_output=None):
    directory=selected_directory(data_directory)
    if connection_output is not None:
        connection_output=Path(connection_output)
        if (not connection_output.is_absolute()or connection_output.parent.resolve()!=connection_output.parent
                or not connection_output.parent.is_dir()or connection_output.exists()or connection_output.is_symlink()
                or connection_output==directory or directory in connection_output.parents or connection_output in directory.parents):
            raise ValueError('Fresh canonical private connection output separate from data required')
    from truss import LocalPostgres
    stopped=threading.Event();previous={};primary=None
    def stop(signum,frame):stopped.set()
    try:
        for selected in (signal.SIGINT,signal.SIGTERM):
            previous[selected]=signal.getsignal(selected);signal.signal(selected,stop)
        _serve(directory,LocalPostgres,stopped.wait,connection_output=connection_output)
    except BaseException as error:primary=error
    finish(primary,[lambda selected=selected,handler=handler:signal.signal(selected,handler)for selected,handler in previous.items()])
