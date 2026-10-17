"""Shared immutable file/producer selections; source families own admission."""
from dataclasses import dataclass
from pathlib import Path
from .config import ProducerConfig


@dataclass(frozen=True)
class ConfiguredSourceAdmissionConfig:
    configuration:Path
    producer:ProducerConfig
    output:Path
    maximum_receipt_bytes:int
    def __post_init__(self):
        for path in (self.configuration,self.output):
            if not(type(path)is type(Path())and path.is_absolute()and path.parent.resolve()==path.parent):raise ValueError('Configured source admission refused')
        if type(self.producer)is not ProducerConfig:raise ValueError('Configured source admission refused')
        self.producer.__post_init__()
        if not(type(self.maximum_receipt_bytes)is int and 1<=self.maximum_receipt_bytes<=4194304):raise ValueError('Configured source admission refused')
        if not(self.configuration!=self.output and self.output not in self.configuration.parents
               and self.output!=self.producer.source and self.producer.source not in self.output.parents and self.output not in self.producer.source.parents
               and all(self.output!=p and self.output not in p.parents for p in (self.producer.bun,self.producer.git))):raise ValueError('Configured source admission refused')
