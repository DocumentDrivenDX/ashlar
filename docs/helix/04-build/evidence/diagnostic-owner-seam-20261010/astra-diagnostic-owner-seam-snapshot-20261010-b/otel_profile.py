"""Public immutable dependency pins for the selected host signal profile."""
from types import MappingProxyType

SDK_VERSIONS = MappingProxyType({
    'opentelemetry-api': '1.45.1', 'opentelemetry-sdk': '1.45.1',
    'opentelemetry-proto': '1.45.1',
    'opentelemetry-exporter-otlp-proto-common': '1.45.1',
    'opentelemetry-exporter-otlp-proto-http': '1.45.1',
    'opentelemetry-semantic-conventions': '0.66b1',
    'opentelemetry-exporter-http-transport': '0.66b1',
    'opentelemetry-exporter-otlp-common': '0.66b1',
    'protobuf': '6.33.6', 'requests': '2.33.0', 'urllib3': '2.8.0',
    'idna': '3.15', 'certifi': '2025.10.5', 'charset-normalizer': '3.4.3',
    'googleapis-common-protos': '1.75.0', 'typing-extensions': '4.15.0'})
