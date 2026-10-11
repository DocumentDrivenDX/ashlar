"""Trusted composition for the independently indexed f36/042 implicit-arithmetic realization.

Operator paths never select trust pins, platforms, profiles or fallback compilers.
Original protocol bytes retain all publication and source host obligations.
"""
from dataclasses import dataclass
from pathlib import Path
import platform
import sys
import subprocess
from typing import Optional

from . import weft_implicit_arithmetic_installation

INDEX_REVISION = 'cdd8fa698683cd87bb126916a75200e80e19d831'
INDEX_SHA256 = '6aa24c395dc721ef6cfef281f34628051a041530ba426d8dc31e9126e9510399'
REALIZATION_ID = 'weft-f36f206-implicit-arithmetic-aarch64-apple-darwin-candidate'
REQUEST_LIMIT = 16 * 1024 * 1024


class ImplicitArithmeticDistributionError(ValueError):
    """Payload-free public outcome; no compiler fallback is selected."""


@dataclass(frozen=True)
class ImplicitArithmeticDistributionPaths:
    index: Path
    output: Path
    package: Optional[Path] = None


def _configuration(paths: ImplicitArithmeticDistributionPaths, *, installing: bool) -> weft_implicit_arithmetic_installation.ImplicitArithmeticInstallationConfig:
    if type(paths) is not ImplicitArithmeticDistributionPaths:
        raise ImplicitArithmeticDistributionError('invalid-configuration')
    if any(not isinstance(value, Path) or not value.is_absolute()
           for value in (paths.index, paths.output)):
        raise ImplicitArithmeticDistributionError('absolute-path-required')
    if installing and paths.package is None:
        raise ImplicitArithmeticDistributionError('package-required')
    if paths.package is not None and (not isinstance(paths.package, Path) or not paths.package.is_absolute()):
        raise ImplicitArithmeticDistributionError('absolute-path-required')
    if sys.platform != 'darwin' or platform.machine() != 'arm64':
        raise ImplicitArithmeticDistributionError('unsupported-platform')
    observed_os = platform.mac_ver()[0]
    if observed_os != '27.0.1':
        raise ImplicitArithmeticDistributionError('unsupported-platform')
    return weft_implicit_arithmetic_installation.ImplicitArithmeticInstallationConfig(
        index_path=paths.index, index_revision=INDEX_REVISION,
        index_sha256=INDEX_SHA256, realization_id=REALIZATION_ID,
        package=paths.package if installing else None, output=paths.output,
        observed_target='aarch64-apple-darwin', observed_os=observed_os,
    )


def install_implicit_arithmetic_distribution(paths: ImplicitArithmeticDistributionPaths) -> weft_implicit_arithmetic_installation.ImplicitArithmeticInstallation:
    config = _configuration(paths, installing=True)
    try:
        return weft_implicit_arithmetic_installation.install(config)
    except (weft_implicit_arithmetic_installation.ImplicitArithmeticInstallationError, OSError):
        raise ImplicitArithmeticDistributionError('installation-refused') from None


def open_implicit_arithmetic_distribution(paths: ImplicitArithmeticDistributionPaths) -> weft_implicit_arithmetic_installation.ImplicitArithmeticInstallation:
    config = _configuration(paths, installing=False)
    try:
        return weft_implicit_arithmetic_installation.open_installation(config)
    except (weft_implicit_arithmetic_installation.ImplicitArithmeticInstallationError, OSError):
        raise ImplicitArithmeticDistributionError('installation-refused') from None


def compile_implicit_arithmetic_distribution(paths: ImplicitArithmeticDistributionPaths, request: bytes) -> bytes:
    if type(request) is not bytes or len(request) > REQUEST_LIMIT:
        raise ImplicitArithmeticDistributionError('invalid-request')
    installation = open_implicit_arithmetic_distribution(paths)
    try:
        return weft_implicit_arithmetic_installation.compile_request(installation, request)
    except (weft_implicit_arithmetic_installation.ImplicitArithmeticInstallationError, subprocess.SubprocessError, OSError):
        raise ImplicitArithmeticDistributionError('compilation-refused') from None


def installed_implicit_arithmetic_schema_bundle(paths: ImplicitArithmeticDistributionPaths) -> tuple[tuple[str, bytes], ...]:
    installation = open_implicit_arithmetic_distribution(paths)
    try:
        return weft_implicit_arithmetic_installation.installed_schema_bundle(installation)
    except (weft_implicit_arithmetic_installation.ImplicitArithmeticInstallationError, OSError):
        raise ImplicitArithmeticDistributionError('installation-refused') from None
