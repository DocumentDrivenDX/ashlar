"""Foreground ownership of an existing initialized local Truss PostgreSQL server."""
import json,re,signal,threading
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


def _serve(directory,runtime_factory,wait):
    """Own one public runtime; test seams do not create a background service."""
    runtime=runtime_factory(directory);entered=False;primary=None
    try:
        runtime.__enter__();entered=True
        print(json.dumps({'connection_uri':runtime.info.connection_uri},separators=(',',':')),flush=True)
        try:wait()
        except KeyboardInterrupt:pass # Normal foreground user shutdown.
    except BaseException as error:primary=error
    finish(primary,[lambda:runtime.__exit__(None,None,None)]if entered else [])


def run_serve_truss_local_command(data_directory):
    directory=selected_directory(data_directory)
    from truss import LocalPostgres
    stopped=threading.Event();previous={};primary=None
    def stop(signum,frame):stopped.set()
    try:
        for selected in (signal.SIGINT,signal.SIGTERM):
            previous[selected]=signal.getsignal(selected);signal.signal(selected,stop)
        _serve(directory,LocalPostgres,stopped.wait)
    except BaseException as error:primary=error
    finish(primary,[lambda selected=selected,handler=handler:signal.signal(selected,handler)for selected,handler in previous.items()])
