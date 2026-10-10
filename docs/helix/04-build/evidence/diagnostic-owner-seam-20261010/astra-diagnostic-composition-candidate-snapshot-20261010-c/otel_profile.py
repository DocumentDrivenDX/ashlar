"""Public immutable dependency pins for the selected host signal profile."""
from types import MappingProxyType

SDK_VERSIONS = MappingProxyType({
    'opentelemetry-api': '1.45.1', 'opentelemetry-sdk': '1.45.1',
    'opentelemetry-proto': '1.45.1',
    'opentelemetry-exporter-otlp-proto-common': '1.45.1',
    'opentelemetry-exporter-otlp-proto-http': '1.45.1',
    'opentelemetry-semantic-conventions': '0.66b1'})
