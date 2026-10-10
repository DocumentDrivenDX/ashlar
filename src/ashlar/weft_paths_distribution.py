"""Trusted composition for the separately admitted Paths530 realization.

Operator paths never select trust pins, platforms, profiles or fallback compilers.
Original protocol bytes retain all publication and source host obligations.
"""
from dataclasses import dataclass
from pathlib import Path
import platform
import sys
import subprocess
from typing import Optional

from . import weft_paths_installation

INDEX_REVISION = 'c8d825808a27e15517c1fc93c3fba0faffc2406e'
INDEX_SHA256 = 'a7571d2bd141bf182ce171acc16f64f1276cca29a855dbf4b49a92283fc71864'
REALIZATION_ID = 'weft-530ae35-paths-aarch64-apple-darwin-candidate'
REQUEST_LIMIT = 16 * 1024 * 1024


class PathsDistributionError(ValueError):
    """Payload-free public outcome; no compiler fallback is selected."""


@dataclass(frozen=True)
class PathsDistributionPaths:
    index: Path
    output: Path
    package: Optional[Path] = None


def _configuration(paths: PathsDistributionPaths, *, installing: bool) -> weft_paths_installation.PathsInstallationConfig:
    if type(paths) is not PathsDistributionPaths:
        raise PathsDistributionError('invalid-configuration')
    if any(not isinstance(value, Path) or not value.is_absolute()
           for value in (paths.index, paths.output)):
        raise PathsDistributionError('absolute-path-required')
    if installing and paths.package is None:
        raise PathsDistributionError('package-required')
    if paths.package is not None and (not isinstance(paths.package, Path) or not paths.package.is_absolute()):
        raise PathsDistributionError('absolute-path-required')
    if sys.platform != 'darwin' or platform.machine() != 'arm64':
        raise PathsDistributionError('unsupported-platform')
    observed_os = platform.mac_ver()[0]
    if observed_os != '27.0.1':
        raise PathsDistributionError('unsupported-platform')
    return weft_paths_installation.PathsInstallationConfig(
        index_path=paths.index, index_revision=INDEX_REVISION,
        index_sha256=INDEX_SHA256, realization_id=REALIZATION_ID,
        package=paths.package if installing else None, output=paths.output,
        observed_target='aarch64-apple-darwin', observed_os=observed_os,
    )


def install_paths_distribution(paths: PathsDistributionPaths) -> weft_paths_installation.PathsInstallation:
    config = _configuration(paths, installing=True)
    try:
        return weft_paths_installation.install(config)
    except weft_paths_installation.PathsInstallationError:
        raise PathsDistributionError('installation-refused') from None


def open_paths_distribution(paths: PathsDistributionPaths) -> weft_paths_installation.PathsInstallation:
    config = _configuration(paths, installing=False)
    try:
        return weft_paths_installation.open_installation(config)
    except weft_paths_installation.PathsInstallationError:
        raise PathsDistributionError('installation-refused') from None


def compile_paths_distribution(paths: PathsDistributionPaths, request: bytes) -> bytes:
    if type(request) is not bytes or len(request) > REQUEST_LIMIT:
        raise PathsDistributionError('invalid-request')
    installation = open_paths_distribution(paths)
    try:
        return weft_paths_installation.compile_request(installation, request)
    except (weft_paths_installation.PathsInstallationError, subprocess.SubprocessError):
        raise PathsDistributionError('compilation-refused') from None


def installed_paths_schema_bundle(paths: PathsDistributionPaths) -> tuple[tuple[str, bytes], ...]:
    installation = open_paths_distribution(paths)
    try:
        return weft_paths_installation.installed_schema_bundle(installation)
    except weft_paths_installation.PathsInstallationError:
        raise PathsDistributionError('installation-refused') from None
