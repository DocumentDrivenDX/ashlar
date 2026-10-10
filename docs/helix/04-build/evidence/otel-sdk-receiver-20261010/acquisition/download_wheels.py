"""Candidate command only: fetch fixed official wheel bytes; never install/import."""
import hashlib
import json
import os
from pathlib import Path
import signal
import ssl
import stat
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import zipfile

ROOT = Path('/private/tmp/ashlar-otel-wheel-resolution-20261010-c')
LOCK = ROOT / 'wheel-lock.json'
LOCK_SHA = '512a9e4891765720686c3c60ea30b767b1cdc9ec69ddc62f4616d7798c5b00f4'
PYTHON = Path('/Users/erik/.local/share/uv/python/cpython-3.11.17-macos-aarch64-none/bin/python3.11')
PYTHON_SHA = '5ebf120b62e8d02ab22485ca328da471db27fd278069df3de01c957cc02d9cd6'
OUTPUT = Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c')


def finish(primary, callbacks):
    for callback in callbacks:
        try:
            callback()
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
    if primary is not None:
        raise primary


def read_regular(path, maximum):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    primary = None; raw = b''
    try:
        initial = os.fstat(fd)
        if not stat.S_ISREG(initial.st_mode) or initial.st_size > maximum:
            raise ValueError('input refused')
        while len(raw) <= maximum:
            chunk = os.read(fd, min(65536, maximum + 1 - len(raw)))
            if not chunk:
                break
            raw += chunk
        final = os.fstat(fd)
        identity = lambda info: (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        if len(raw) > maximum or identity(initial) != identity(final) or len(raw) != initial.st_size:
            raise ValueError('input refused')
    except BaseException as error:
        primary = error
    finish(primary, [lambda: os.close(fd)])
    return raw


def verified_lock():
    raw = read_regular(LOCK, 65536)
    if hashlib.sha256(raw).hexdigest() != LOCK_SHA:
        raise ValueError('lock refused')
    value = json.loads(raw)
    if value['fileCount'] != 16 or value['totalBytes'] != 1955024:
        raise ValueError('lock refused')
    return raw, value


def write_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    primary = None
    try:
        offset = 0
        while offset < len(raw):
            count = os.write(fd, raw[offset:offset+65536])
            if count <= 0:
                raise ValueError('write refused')
            offset += count
    except BaseException as error:
        primary = error
    finish(primary, [lambda: os.close(fd)])


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect refused')


def worker():
    original, lock = verified_lock()
    OUTPUT.mkdir(mode=0o700, exist_ok=False)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()), NoRedirect())
    deadline = time.monotonic() + 50
    for item in lock['packages']:
        uri = urllib.parse.urlsplit(item['url'])
        if (uri.scheme != 'https' or uri.netloc != 'files.pythonhosted.org'
                or uri.query or uri.fragment or Path(uri.path).name != item['filename']
                or not item['filename'].endswith('.whl') or item['yanked']):
            raise ValueError('wheel URL refused')
        metadata = read_regular(Path(item['metadata']['path']), 262144)
        if (len(metadata) != item['metadata']['bytes']
                or hashlib.sha256(metadata).hexdigest() != item['metadata']['sha256']):
            raise ValueError('metadata refused')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError('deadline refused')
        request = urllib.request.Request(item['url'], headers={
            'Accept': 'application/octet-stream', 'Accept-Encoding': 'identity',
            'User-Agent': 'Ashlar-reviewed-wheel-acquisition/0.1'})
        response = opener.open(request, timeout=min(5, remaining))
        primary = None; raw = bytearray()
        try:
            if (response.status != 200 or response.geturl() != item['url']
                    or response.headers.get('Content-Encoding') not in (None, 'identity')
                    or response.headers.get('Content-Length') != str(item['bytes'])):
                raise ValueError('wheel response refused')
            while len(raw) <= item['bytes']:
                if time.monotonic() >= deadline:
                    raise ValueError('deadline refused')
                chunk = response.read(min(65536, item['bytes'] + 1 - len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
            if len(raw) != item['bytes'] or hashlib.sha256(raw).hexdigest() != item['sha256']:
                raise ValueError('wheel bytes refused')
        except BaseException as error:
            primary = error
        finish(primary, [response.close])
        path = OUTPUT / item['filename']
        write_new(path, raw)
        verify_wheel_metadata(path, metadata)
    if read_regular(LOCK, 65536) != original:
        raise ValueError('closing lock refused')
    write_new(OUTPUT/'.download-provisional.json', receipt_bytes(lock, provisional=True))


def verify_wheel_metadata(path, metadata):
    """Inspect inert ZIP metadata while preserving every primary failure."""
    wheel = None; stream = None; primary = None
    try:
        wheel = zipfile.ZipFile(path)
        members = [info for info in wheel.infolist() if info.filename.endswith('.dist-info/METADATA')]
        if len(members) != 1 or members[0].file_size != len(metadata):
            raise ValueError('wheel metadata refused')
        stream = wheel.open(members[0])
        if stream.read(len(metadata)+1) != metadata:
            raise ValueError('wheel metadata refused')
    except BaseException as error:
        primary = error
    callbacks = []
    if stream is not None:
        callbacks.append(stream.close)
    if wheel is not None:
        callbacks.append(wheel.close)
    finish(primary, callbacks)


def receipt_bytes(lock, *, provisional):
    receipt = {
        'format': 'ashlar-otel-wheel-download-provisional/0.1' if provisional else 'ashlar-otel-wheel-download/0.1',
        'lockSha256': LOCK_SHA,
        'files': [{key: item[key] for key in ('name', 'version', 'filename', 'bytes', 'sha256')}
                  for item in lock['packages']],
        'bytes': sum(item['bytes'] for item in lock['packages']),
        'scope': ('Worker verification only; parent closure pending' if provisional else
                  'Exact wheels and embedded METADATA after parent closure; no installation, SDK import or runtime qualification'),
    }
    return (json.dumps(receipt, sort_keys=True, separators=(',', ':'))+'\n').encode()


def publish_download(original, lock):
    """Publish only after worker exit, group cleanup, and closing file checks.

    The final exclusive link is the availability commit. A cancellation after
    that link can retain the complete receipt. Stage unlink is housekeeping;
    no failed operation rolls back or overwrites a visible final receipt.
    """
    if read_regular(LOCK, 65536) != original:
        raise ValueError('closing lock refused')
    provisional = read_regular(OUTPUT/'.download-provisional.json', 65536)
    if provisional != receipt_bytes(lock, provisional=True):
        raise ValueError('provisional receipt refused')
    for item in lock['packages']:
        raw = read_regular(OUTPUT/item['filename'], item['bytes'])
        if len(raw) != item['bytes'] or hashlib.sha256(raw).hexdigest() != item['sha256']:
            raise ValueError('closing wheel refused')
    raw = receipt_bytes(lock, provisional=False)
    stage = OUTPUT/'.download-stage.json'
    write_new(stage, raw)
    if read_regular(stage, 65536) != raw:
        raise ValueError('receipt stage refused')
    os.link(stage, OUTPUT/'download.json')
    try:
        stage.unlink()
    except OSError:
        pass



def main():
    original, lock = verified_lock()
    binary = read_regular(PYTHON, 17277120)
    if len(binary) != 17277120 or hashlib.sha256(binary).hexdigest() != PYTHON_SHA:
        raise ValueError('interpreter refused')
    process = None; primary = None
    try:
        process = subprocess.Popen([str(PYTHON), '-I', '-B', str(ROOT/'download_wheels.py'), '--worker'],
            cwd=ROOT, env={'PATH': '/usr/bin:/bin'}, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        if process.wait(timeout=60) != 0:
            raise ValueError('wheel download refused')
    except BaseException as error:
        primary = error
    callbacks = []
    if process is not None:
        def stop():
            failure = None
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except BaseException as error:
                failure = error
            finish(failure, [lambda: process.wait(timeout=2)])
        callbacks.append(stop)
    finish(primary, callbacks)
    publish_download(original, lock)


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--worker']:
            worker()
        elif sys.argv[1:] == []:
            main()
        else:
            raise ValueError('arguments refused')
    except Exception:
        print('Wheel acquisition refused; retain partial directory for review.', file=sys.stderr)
        raise SystemExit(2) from None
