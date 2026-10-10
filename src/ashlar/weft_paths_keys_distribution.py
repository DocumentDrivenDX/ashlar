"""Trusted composition for the separately admitted PathsKeys3a realization.

Operator paths never select trust pins, platforms, profiles or fallback compilers.
Original protocol bytes retain all publication and source host obligations.
"""
from dataclasses import dataclass
from pathlib import Path
import platform
import sys
import subprocess
from typing import Optional

from . import weft_paths_keys_installation

INDEX_REVISION = '6d82b7e89e43319c2a175b9895527298ff66ef36'
INDEX_SHA256 = 'bd541aa8cb261376f160661d3df67065b36fcba4f6caf4c0f42e33d710c4b9dc'
REALIZATION_ID = 'weft-3a2a79c-paths-keys-aarch64-apple-darwin-candidate'
REQUEST_LIMIT = 16 * 1024 * 1024


class PathsKeysDistributionError(ValueError):
    """Payload-free public outcome; no compiler fallback is selected."""


@dataclass(frozen=True)
class PathsKeysDistributionPaths:
    index: Path
    output: Path
    package: Optional[Path] = None


def _configuration(paths: PathsKeysDistributionPaths, *, installing: bool) -> weft_paths_keys_installation.PathsKeysInstallationConfig:
    if type(paths) is not PathsKeysDistributionPaths:
        raise PathsKeysDistributionError('invalid-configuration')
    if any(not isinstance(value, Path) or not value.is_absolute()
           for value in (paths.index, paths.output)):
        raise PathsKeysDistributionError('absolute-path-required')
    if installing and paths.package is None:
        raise PathsKeysDistributionError('package-required')
    if paths.package is not None and (not isinstance(paths.package, Path) or not paths.package.is_absolute()):
        raise PathsKeysDistributionError('absolute-path-required')
    if sys.platform != 'darwin' or platform.machine() != 'arm64':
        raise PathsKeysDistributionError('unsupported-platform')
    observed_os = platform.mac_ver()[0]
    if observed_os != '27.0.1':
        raise PathsKeysDistributionError('unsupported-platform')
    return weft_paths_keys_installation.PathsKeysInstallationConfig(
        index_path=paths.index, index_revision=INDEX_REVISION,
        index_sha256=INDEX_SHA256, realization_id=REALIZATION_ID,
        package=paths.package if installing else None, output=paths.output,
        observed_target='aarch64-apple-darwin', observed_os=observed_os,
    )


def install_paths_keys_distribution(paths: PathsKeysDistributionPaths) -> weft_paths_keys_installation.PathsKeysInstallation:
    config = _configuration(paths, installing=True)
    try:
        return weft_paths_keys_installation.install(config)
    except (weft_paths_keys_installation.PathsKeysInstallationError, OSError):
        raise PathsKeysDistributionError('installation-refused') from None


def open_paths_keys_distribution(paths: PathsKeysDistributionPaths) -> weft_paths_keys_installation.PathsKeysInstallation:
    config = _configuration(paths, installing=False)
    try:
        return weft_paths_keys_installation.open_installation(config)
    except (weft_paths_keys_installation.PathsKeysInstallationError, OSError):
        raise PathsKeysDistributionError('installation-refused') from None


def compile_paths_keys_distribution(paths: PathsKeysDistributionPaths, request: bytes) -> bytes:
    if type(request) is not bytes or len(request) > REQUEST_LIMIT:
        raise PathsKeysDistributionError('invalid-request')
    installation = open_paths_keys_distribution(paths)
    try:
        return weft_paths_keys_installation.compile_request(installation, request)
    except (weft_paths_keys_installation.PathsKeysInstallationError, subprocess.SubprocessError, OSError):
        raise PathsKeysDistributionError('compilation-refused') from None


def installed_paths_keys_schema_bundle(paths: PathsKeysDistributionPaths) -> tuple[tuple[str, bytes], ...]:
    installation = open_paths_keys_distribution(paths)
    try:
        return weft_paths_keys_installation.installed_schema_bundle(installation)
    except (weft_paths_keys_installation.PathsKeysInstallationError, OSError):
        raise PathsKeysDistributionError('installation-refused') from None
