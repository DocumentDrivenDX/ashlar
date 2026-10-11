"""Explicit host settings for the separately indexed original-commerce profile."""
from dataclasses import dataclass
from pathlib import Path
from ashlar.weft_path_decode import PathDecodeConfig
from .config import HostError, ProducerConfig, PrivatePostgresConfig
from .path_capture import PathCaptureConfig
from .implicit_arithmetic_profile import PROFILE


@dataclass(frozen=True)
class QueryCommerceImplicitArithmeticConfig:
    index: Path
    installation: Path
    publication: Path
    output: Path
    jars: Path
    model: Path
    graph: Path
    producer: ProducerConfig
    postgres: PrivatePostgresConfig
    maximum_artifact_bytes: int
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    profile: str = PROFILE.name

    def __post_init__(self):
        if type(self.profile) is not str or self.profile != PROFILE.name:
            raise HostError('explicit-implicit-arithmetic-profile-required')
        for path in (self.index, self.installation, self.publication, self.output,
                     self.jars, self.model, self.graph):
            if not isinstance(path, Path) or not path.is_absolute():
                raise HostError('absolute-path-required')
        if (type(self.producer) is not ProducerConfig
                or type(self.postgres) is not PrivatePostgresConfig
                or type(self.capture) is not PathCaptureConfig
                or type(self.decoder) is not PathDecodeConfig):
            raise HostError('invalid-configuration')
        self.producer.__post_init__()
        self.postgres.__post_init__()
        self.capture.__post_init__()
        self.decoder.__post_init__()
        for value, limit in ((self.maximum_artifact_bytes, 16 * 1024 * 1024),
                             (self.capture.maximum_rows, 1000),
                             (self.capture.maximum_cell_bytes, 16 * 1024 * 1024),
                             (self.capture.maximum_total_cell_bytes, 64 * 1024 * 1024)):
            if type(value) is not int or not 1 <= value <= limit:
                raise HostError('finite-bound-required')
        if self.decoder.maximum_cell_bytes > self.capture.maximum_cell_bytes:
            raise HostError('decoder-exceeds-capture-bound')
