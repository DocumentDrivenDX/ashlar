# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public immutable dependency pins for the selected host signal profile."""
from types import MappingProxyType

SDK_VERSIONS = MappingProxyType({
    'opentelemetry-api': '1.45.2', 'opentelemetry-sdk': '1.45.2',
    'opentelemetry-proto': '1.45.2',
    'opentelemetry-exporter-otlp-proto-common': '1.45.2',
    'opentelemetry-exporter-otlp-proto-http': '1.45.2',
    'opentelemetry-semantic-conventions': '0.66b2',
    'opentelemetry-exporter-http-transport': '0.66b2',
    'opentelemetry-exporter-otlp-common': '0.66b2',
    'protobuf': '7.36.2', 'requests': '2.34.2', 'urllib3': '2.8.0',
    'idna': '3.20', 'certifi': '2026.10.23', 'charset-normalizer': '3.5.2',
    'googleapis-common-protos': '1.76.0', 'typing-extensions': '4.16.0'})
