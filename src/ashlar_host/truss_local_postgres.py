# SPDX-License-Identifier: MIT OR Apache-2.0
"""Effect-free admission for an externally supplied PostgreSQL 18.6 tool.

Truss's ``[local]`` extra installs its own LocalPostgres runtime. Ashlar's local
R16 path instead owns the PostgreSQL process and supplies authenticated
connections to Truss, so its supervisor must bind the external ``pg_ctl`` it
will later use. This helper only inspects the executable; it never starts or
stops a server.
"""
import hashlib
import re
import stat
import subprocess
from pathlib import Path


def inspect_external_postgres_18_6_pg_ctl(path, expected_sha256):
    """Return a receipt for one exact external PostgreSQL 18.6 ``pg_ctl``.

    The caller supplies the path and digest frozen into its attempt packet. The
    path must name a canonical regular file, its bytes must match the digest,
    and the read-only ``--version`` probe must identify PostgreSQL 18.6.
    """
    if (type(path) is not str or not Path(path).is_absolute()
            or type(expected_sha256) is not str
            or re.fullmatch(r'[0-9a-f]{64}', expected_sha256) is None):
        raise ValueError('Absolute external pg_ctl path and exact SHA-256 required')
    executable = Path(path)
    if executable.resolve() != executable or executable.is_symlink():
        raise ValueError('Canonical external pg_ctl path required')
    try:
        details = executable.stat()
    except OSError as error:
        raise ValueError('External PostgreSQL 18.6 pg_ctl is unavailable') from error
    if not stat.S_ISREG(details.st_mode):
        raise ValueError('External PostgreSQL 18.6 pg_ctl must be a regular file')
    digest = hashlib.sha256(executable.read_bytes()).hexdigest()
    if digest != expected_sha256:
        raise ValueError('External PostgreSQL pg_ctl digest differs from frozen packet')
    try:
        result = subprocess.run([str(executable), '--version'], stdin=subprocess.DEVNULL,
                                capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ValueError('External PostgreSQL pg_ctl version probe failed') from error
    version = result.stdout.strip()
    if (result.returncode != 0
            or re.fullmatch(r'pg_ctl \(PostgreSQL\) 18\.6(?: \(Homebrew\))?', version) is None):
        raise ValueError('Exact PostgreSQL 18.6 external pg_ctl required')
    return {'path': str(executable), 'sha256': digest,
            'bytes': details.st_size, 'version': version,
            'probe': '--version', 'server_started': False}
