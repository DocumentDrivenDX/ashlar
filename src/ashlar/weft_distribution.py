"""Trusted application composition for one independently admitted realization.

No network discovery or caller-selected trust pins. Compiler output remains an
unmodified protocol artifact; it does not discharge publication/host obligations.
"""
from dataclasses import dataclass
from pathlib import Path
import platform
import sys
from typing import BinaryIO, Optional

from . import weft_installation

INDEX_REVISION = '41a42ed5fc3c780ce900c61144733ad93359dc76'
INDEX_SHA256 = '71e441ce80b36a50cb8fc668431fefddec71e5e83908eb7fe78abe40c41ba2de'
REALIZATION_ID = 'weft-362a9c6-ab899150-aarch64-apple-darwin-candidate'
REQUEST_LIMIT = 16 * 1024 * 1024


class DistributionError(ValueError):
    """Payload-free public failure category, safe for CLI stderr."""


@dataclass(frozen=True)
class DistributionPaths:
    index: Path
    output: Path
    package: Optional[Path] = None


def _configuration(paths: DistributionPaths, *, installing: bool) -> weft_installation.InstallationConfig:
    if not isinstance(paths, DistributionPaths):
        raise DistributionError('invalid-configuration')
    for value in (paths.index, paths.output):
        if not isinstance(value, Path):
            raise DistributionError('invalid-configuration')
    if installing and not isinstance(paths.package, Path):
        raise DistributionError('package-required')
    if paths.package is not None and not isinstance(paths.package, Path):
        raise DistributionError('invalid-configuration')
    # These are host observations, never command-line overrides. The first
    # independently qualified realization supports exactly this observed host.
    if sys.platform != 'darwin' or platform.machine() != 'arm64':
        raise DistributionError('unsupported-platform')
    observed_os = platform.mac_ver()[0]
    if observed_os != '27.0.1':
        raise DistributionError('unsupported-platform')
    # Reopening deliberately omits package; installation requires its Path.
    return weft_installation.InstallationConfig(
        index_path=paths.index, index_revision=INDEX_REVISION,
        index_sha256=INDEX_SHA256, package=paths.package,
        realization_id=REALIZATION_ID, output=paths.output,
        observed_target='aarch64-apple-darwin', observed_os=observed_os,
    )


def install_distribution(paths: DistributionPaths) -> weft_installation.Installation:
    """Install through the public verifier; cleanup warning is nonauthority."""
    config = _configuration(paths, installing=True)
    try:
        return weft_installation.install(config)
    except weft_installation.InstallationError:
        raise DistributionError('installation-refused') from None


def open_distribution(paths: DistributionPaths) -> weft_installation.Installation:
    """Open retained bytes and trusted index without an original package."""
    config = _configuration(paths, installing=False)
    try:
        return weft_installation.open_installation(config)
    except weft_installation.InstallationError:
        raise DistributionError('installation-refused') from None


def read_request(stream: BinaryIO) -> bytes:
    """Bound allocation even for a stream containing unlimited input."""
    result = bytearray()
    while len(result) <= REQUEST_LIMIT:
        chunk = stream.read(min(65536, REQUEST_LIMIT + 1 - len(result)))
        if type(chunk) is not bytes:
            raise DistributionError('invalid-request')
        if not chunk:
            return bytes(result)
        result.extend(chunk)
    raise DistributionError('request-too-large')


def compile_distribution(paths: DistributionPaths, request: bytes) -> bytes:
    if type(request) is not bytes or len(request) > REQUEST_LIMIT:
        raise DistributionError('invalid-request')
    installation = open_distribution(paths)
    try:
        return weft_installation.compile_request(installation, request)
    except weft_installation.InstallationError:
        raise DistributionError('compilation-refused') from None


def diagnostic(installation: weft_installation.Installation) -> str:
    """Safe nonauthoritative maintenance projection; no raw paths/content."""
    return 'cleanup-pending' if installation.cleanup_pending else 'installed'
