"""SDK-free singleton from selected saved artifacts; no live publication authority."""
import json
import os
import stat
from pathlib import Path
from ashlar.graph_release import decode_graph_key, validate_release_carrier
from .graph_export_diff import parse_graph_export_selection
from .local_graph_export import LocalGraphExportSelection, open_local_graph_export
from .private_graph_custody import decode_custody_bytes
from .evolution_cli import InvocationFile
from .lifecycle import owned_context, finish

MAX_REPORT_BYTES = 16 * 1024 * 1024


def read_graph_export(selection: LocalGraphExportSelection, graph_id: str) -> dict:
    """Return an exact saved carrier or missing after original file-use closure."""
    if (type(selection) is not LocalGraphExportSelection or type(graph_id) is not str
            or not 0 < len(graph_id) <= 65536):
        raise ValueError('Explicit export selection and canonical graph key required')
    key = decode_graph_key(graph_id)
    kind = key[0]
    answer = None
    with owned_context(open_local_graph_export(selection)) as owner:
        with owned_context(owner.writer(owner.context)):
            def admit():
                owner.admit_original_export(owner.release, owner.custody_bytes, owner.source, owner.context)
            primary = None
            try:
                admit()
                value = decode_custody_bytes(owner.release.payload)
                found = []
                for row in value['nodes' if kind == 'node' else 'edges']:
                    derived = {'graph_id', 'src', 'dst'} if kind == 'edge' else {'graph_id'}
                    checked = validate_release_carrier({k: v for k, v in row.items() if k not in derived}, kind)
                    if checked != row:
                        raise ValueError('Exact canonical saved carrier required')
                    if row['graph_id'] == graph_id:
                        found.append(dict(row))
                if len(found) > 1:
                    raise ValueError('Unique saved graph identity required')
                answer = {'profile': 'ashlar-local-graph-export-singleton/0.1',
                          'publication_id': value['publication']['publication_id'],
                          'release_sha256': selection.release_sha256,
                          'custody_sha256': selection.custody_sha256,
                          'graph_id': graph_id, 'kind': kind,
                          'found': bool(found), 'carrier': found[0] if found else None,
                          'qualification': 'Saved private artifact only; no live publication, current source permission, Unity Catalog or ACK authority.'}
            except BaseException as error:
                primary = error
            finish(primary, [admit])
    return answer


def _encoded(value: dict) -> bytes:
    raw = bytearray()
    for part in json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).iterencode(value):
        block = part.encode('utf8')
        if len(raw) + len(block) + 1 > MAX_REPORT_BYTES:
            raise ValueError('Bounded saved singleton report required')
        raw.extend(block)
    raw.extend(b'\n')
    return bytes(raw)


def run_read_graph_export_command(configuration: Path, graph_id: str, output: Path) -> dict:
    """Publish a fresh private report after configuration and artifact closure."""
    captured = InvocationFile.read(configuration, 65536)
    selection = parse_graph_export_selection(captured.raw)
    destination = Path(output)
    protected = (captured.path, selection.release_path, selection.custody_path)
    if (not destination.is_absolute() or destination.parent.resolve() != destination.parent
            or not destination.parent.is_dir() or destination.exists() or destination.is_symlink()
            or any(destination == p or destination in p.parents or p in destination.parents for p in protected)):
        raise ValueError('Fresh separate canonical report required')
    primary = None
    answer = None
    try:
        captured.renew(65536)
        answer = read_graph_export(selection, graph_id)
    except BaseException as error:
        primary = error
    finish(primary, [lambda: captured.renew(65536)])
    payload = _encoded(answer)
    parent = destination.parent
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    parent_info = os.fstat(parent_fd)
    fd = None
    stream = None
    identity = None
    staging_name = destination.name + '.pending'
    published = False
    linked_identity = None
    primary = None

    def parent_same():
        named = parent.lstat()
        if (parent.resolve() != parent or named.st_uid != os.geteuid()
                or stat.S_IMODE(named.st_mode) != 0o700
                or (named.st_dev, named.st_ino, named.st_mode) !=
                   (parent_info.st_dev, parent_info.st_ino, parent_info.st_mode)):
            raise PermissionError('Original private report parent required')

    def file_same(name):
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (not stat.S_ISREG(named.st_mode) or
                (named.st_dev, named.st_ino) != identity or named.st_size != len(payload)
                or stat.S_IMODE(named.st_mode) != 0o600
                or os.pread(fd, len(payload) + 1, 0) != payload):
            raise PermissionError('Original owned report bytes required')
        parent_same()

    def remove_owned(name, expected=None):
        try:
            named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        selected = identity if expected is None else expected
        if selected is not None and (named.st_dev, named.st_ino) == selected:
            os.unlink(name, dir_fd=parent_fd)

    try:
        parent_same()
        fd = os.open(staging_name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent_fd)
        held = os.fstat(fd)
        identity = (held.st_dev, held.st_ino)
        stream = os.fdopen(os.dup(fd), 'wb')
        body = None
        try:
            if stream.write(payload) != len(payload):
                raise OSError('Complete saved report required')
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException as error:
            body = error
        finish(body, [stream.close])
        stream = None
        file_same(staging_name)
        os.link(staging_name, destination.name, src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd, follow_symlinks=False)
        linked = os.stat(destination.name, dir_fd=parent_fd, follow_symlinks=False)
        linked_identity = (linked.st_dev, linked.st_ino)
        file_same(destination.name)
        published = True
    except BaseException as error:
        primary = error
    callbacks = []
    if stream is not None:
        callbacks.append(stream.close)
    if not published:
        callbacks.append(lambda: remove_owned(destination.name, linked_identity))
    callbacks.append(lambda: remove_owned(staging_name))
    if fd is not None:
        callbacks.append(lambda: os.close(fd))
    callbacks.append(lambda: os.close(parent_fd))
    finish(primary, callbacks)
    return answer
