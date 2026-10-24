"""Fail-closed metadata and byte accounting for retained R05 runtime trees."""
from __future__ import annotations

import hashlib
import os
import posixpath
import stat
from pathlib import Path
from typing import Callable


class AccountingError(ValueError):
    pass


def inventory_roots(roots: list[Path], *, allowed_links: dict[str, dict[str, str]] | None = None,
                    hash_paths: set[str] | None = None,
                    known_dangling_links: set[str] | None = None,
                    check: Callable[[], None] | None = None) -> dict:
    """Inventory every entry, count unique regular-file bytes, hash selected pins.

    Directories are walked without following links. Every link must exactly match
    its recorded lexical target. Device/inode/size/mtime/ctime are rechecked after
    traversal. Only explicitly selected files receive content SHA-256 custody.
    """
    allowed_links = allowed_links or {}
    hash_paths = hash_paths or set()
    known_dangling_links = known_dangling_links or set()
    canonical_roots = []
    for root in roots:
        if root.is_symlink() or not (root.is_dir() or root.is_file()):
            raise AccountingError(f"regular file or directory root required: {root}")
        if root.resolve(strict=True) != root:
            raise AccountingError(f"root is not canonical: {root}")
        canonical_roots.append(root)
    entries: dict[str, dict] = {}
    identities: dict[tuple[int, int], tuple[str, dict]] = {}
    selected_found: set[str] = set()

    def record_file(path: Path, info: os.stat_result) -> None:
        identity = (info.st_dev, info.st_ino)
        row = {"path": str(path), "type": "file", "device": info.st_dev,
               "inode": info.st_ino, "size": info.st_size,
               "mtimeNs": info.st_mtime_ns, "ctimeNs": info.st_ctime_ns}
        if str(path) in hash_paths:
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            row["sha256"] = digest.hexdigest()
            selected_found.add(str(path))
        old = identities.get(identity)
        if old is None:
            identities[identity] = (str(path), row)
        elif str(path) in hash_paths and "sha256" not in old[1]:
            old[1]["sha256"] = row["sha256"]
            selected_found.add(str(path))
        entries[str(path)] = row

    for root in canonical_roots:
        if check:
            check()
        if root.is_file():
            record_file(root, os.lstat(root))
            continue
        stack = [root]
        while stack:
            if check:
                check()
            directory = stack.pop()
            dstat = os.lstat(directory)
            entries[str(directory)] = {"path": str(directory), "type": "directory",
                "device": dstat.st_dev, "inode": dstat.st_ino,
                "mtimeNs": dstat.st_mtime_ns, "ctimeNs": dstat.st_ctime_ns}
            with os.scandir(directory) as children:
                for child in children:
                    if check and len(entries) % 128 == 0:
                        check()
                    path = Path(child.path)
                    info = child.stat(follow_symlinks=False)
                    if stat.S_ISLNK(info.st_mode):
                        raw_target = os.readlink(path)
                        target = (posixpath.normpath(raw_target) if os.path.isabs(raw_target)
                                  else posixpath.normpath(os.path.join(str(path.parent), raw_target)))
                        if allowed_links.get(str(path)) != {"rawTarget": raw_target, "target": target}:
                            raise AccountingError(f"undeclared or changed symlink: {path}")
                        contained = any(
                            (root.is_dir() and (Path(target) == root or root in Path(target).parents))
                            or (root.is_file() and Path(target) == root)
                            for root in canonical_roots)
                        if not contained and (str(path) not in known_dangling_links
                                              or Path(target).exists()):
                            raise AccountingError(f"symlink target leaves selected roots: {path}")
                        entries[str(path)] = {"path": str(path), "type": "symlink",
                            "rawTarget": raw_target, "target": target,
                            "device": info.st_dev, "inode": info.st_ino,
                            "mtimeNs": info.st_mtime_ns, "ctimeNs": info.st_ctime_ns}
                    elif stat.S_ISDIR(info.st_mode):
                        stack.append(path)
                    elif stat.S_ISREG(info.st_mode):
                        record_file(path, info)
                    else:
                        raise AccountingError(f"special file in retained tree: {path}")
    missing = hash_paths - selected_found
    if missing:
        raise AccountingError(f"selected hash paths absent from retained tree: {sorted(missing)}")
    if check:
        check()
    for index, (path_text, item) in enumerate(entries.items()):
        if check and index % 128 == 0:
            check()
        info = os.lstat(path_text)
        if (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns) != (
                item["device"], item["inode"], item["mtimeNs"], item["ctimeNs"]):
            raise AccountingError(f"entry changed during inventory: {path_text}")
        if item["type"] == "file" and info.st_size != item["size"]:
            raise AccountingError(f"file size changed during inventory: {path_text}")
    if check:
        check()
    files = list(identities.values())
    return {"roots": [str(p) for p in canonical_roots],
            "entries": sorted(entries.values(), key=lambda x: x["path"]),
            "knownDanglingLinks": sorted(known_dangling_links),
            "uniqueFileCount": len(files), "uniqueBytes": sum(x[1]["size"] for x in files)}
