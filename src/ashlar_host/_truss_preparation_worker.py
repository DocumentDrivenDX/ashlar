"""Closed isolated installed preparation child; no database or provider extension."""
import hashlib
from pathlib import Path
import sys

# -I -S deliberately excludes site startup and ambient import paths. This
# package path is the selected installed worker's own location, not discovery.
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ashlar_host.truss_preparation_configuration import PinnedFile, parse_truss_preparation_configuration
from ashlar_host.truss_preparation import prepare_installed_views, emit_preparation_transport
from ashlar_host.lifecycle import finish


def main():
    if len(sys.argv)!=3:raise ValueError('Closed preparation child arguments required')
    path=Path(sys.argv[1]);pin=PinnedFile(path,path.stat().st_size,sys.argv[2]);primary=None
    config=None
    try:
        raw=pin.read(1048576);config=parse_truss_preparation_configuration(raw)
        metadata,files=prepare_installed_views(config)
        finish(None,[pin.renew,config.renew])
        emit_preparation_transport(config,metadata,files,sys.stdout.buffer)
    except BaseException as error:primary=error
    callbacks=[pin.renew]
    if config is not None:callbacks.append(config.renew)
    finish(primary,callbacks)


if __name__=='__main__':main()
