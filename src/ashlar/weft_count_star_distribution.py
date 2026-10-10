"""Trusted composition for the independently indexed 5dd/041 count-star realization.

Operator paths never select trust pins, platforms, profiles or fallback compilers.
Original protocol bytes retain all publication and source host obligations.
"""
from dataclasses import dataclass
from pathlib import Path
import platform
import sys
import subprocess
from typing import Optional

from . import weft_count_star_installation

INDEX_REVISION = 'bd490abc877a1992666c7b45412b14a0ca6c2cac'
INDEX_SHA256 = '7eddc39661f6cb400afae897aa0a7d8cd5dee0aec026ed45a4e197fa7c9558a6'
REALIZATION_ID = 'weft-5ddcebd-count-star-aarch64-apple-darwin-candidate'
REQUEST_LIMIT = 16 * 1024 * 1024


class CountStarDistributionError(ValueError):
    """Payload-free public outcome; no compiler fallback is selected."""


@dataclass(frozen=True)
class CountStarDistributionPaths:
    index: Path
    output: Path
    package: Optional[Path] = None


def _configuration(paths: CountStarDistributionPaths, *, installing: bool) -> weft_count_star_installation.CountStarInstallationConfig:
    if type(paths) is not CountStarDistributionPaths:
        raise CountStarDistributionError('invalid-configuration')
    if any(not isinstance(value, Path) or not value.is_absolute()
           for value in (paths.index, paths.output)):
        raise CountStarDistributionError('absolute-path-required')
    if installing and paths.package is None:
        raise CountStarDistributionError('package-required')
    if paths.package is not None and (not isinstance(paths.package, Path) or not paths.package.is_absolute()):
        raise CountStarDistributionError('absolute-path-required')
    if sys.platform != 'darwin' or platform.machine() != 'arm64':
        raise CountStarDistributionError('unsupported-platform')
    observed_os = platform.mac_ver()[0]
    if observed_os != '27.0.1':
        raise CountStarDistributionError('unsupported-platform')
    return weft_count_star_installation.CountStarInstallationConfig(
        index_path=paths.index, index_revision=INDEX_REVISION,
        index_sha256=INDEX_SHA256, realization_id=REALIZATION_ID,
        package=paths.package if installing else None, output=paths.output,
        observed_target='aarch64-apple-darwin', observed_os=observed_os,
    )


def install_count_star_distribution(paths: CountStarDistributionPaths) -> weft_count_star_installation.CountStarInstallation:
    config = _configuration(paths, installing=True)
    try:
        return weft_count_star_installation.install(config)
    except (weft_count_star_installation.CountStarInstallationError, OSError):
        raise CountStarDistributionError('installation-refused') from None


def open_count_star_distribution(paths: CountStarDistributionPaths) -> weft_count_star_installation.CountStarInstallation:
    config = _configuration(paths, installing=False)
    try:
        return weft_count_star_installation.open_installation(config)
    except (weft_count_star_installation.CountStarInstallationError, OSError):
        raise CountStarDistributionError('installation-refused') from None


def compile_count_star_distribution(paths: CountStarDistributionPaths, request: bytes) -> bytes:
    if type(request) is not bytes or len(request) > REQUEST_LIMIT:
        raise CountStarDistributionError('invalid-request')
    installation = open_count_star_distribution(paths)
    try:
        return weft_count_star_installation.compile_request(installation, request)
    except (weft_count_star_installation.CountStarInstallationError, subprocess.SubprocessError, OSError):
        raise CountStarDistributionError('compilation-refused') from None


def installed_count_star_schema_bundle(paths: CountStarDistributionPaths) -> tuple[tuple[str, bytes], ...]:
    installation = open_count_star_distribution(paths)
    try:
        return weft_count_star_installation.installed_schema_bundle(installation)
    except (weft_count_star_installation.CountStarInstallationError, OSError):
        raise CountStarDistributionError('installation-refused') from None
