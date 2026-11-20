"""OpenTelemetry helpers: init tracer, inject/extract W3C traceparent for Kafka."""

from __future__ import annotations

import logging
import os
from typing import Iterable

from opentelemetry import context, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

log = logging.getLogger(__name__)

_initialized = False


def init_tracing(service_name: str | None = None) -> trace.Tracer:
    """Idempotent tracer setup. No-op exporter if OTEL_SDK_DISABLED=true."""
    global _initialized
    name = service_name or os.getenv('OTEL_SERVICE_NAME', 'purchase-order')
    if not _initialized:
        if os.getenv('OTEL_SDK_DISABLED', '').lower() in ('1', 'true', 'yes'):
            trace.set_tracer_provider(TracerProvider())
            log.info('OpenTelemetry disabled (OTEL_SDK_DISABLED)')
        else:
            endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://127.0.0.1:4318')
            # HTTP exporter expects base URL without /v1/traces sometimes — SDK appends it.
            resource = Resource.create({'service.name': name})
            provider = TracerProvider(resource=resource)
            base = endpoint.rstrip('/')
            # Constructor endpoint must include the signal path (env-only form auto-appends).
            traces_endpoint = base if base.endswith('/v1/traces') else f'{base}/v1/traces'
            exporter = OTLPSpanExporter(endpoint=traces_endpoint)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(provider)
            log.info('OpenTelemetry tracing → %s (service=%s)', endpoint, name)
        _initialized = True
    return trace.get_tracer(name)


def kafka_inject_headers() -> list[tuple[str, bytes]]:
    """Build Kafka headers carrying the current trace context (W3C traceparent)."""
    carrier: dict[str, str] = {}
    inject(carrier)
    return [(key, value.encode()) for key, value in carrier.items()]


def attach_from_kafka_headers(headers: Iterable[tuple[str, bytes | None]] | None):
    """Extract trace context from Kafka headers and attach it. Returns a detach token."""
    carrier: dict[str, str] = {}
    if headers:
        for key, value in headers:
            if value is None:
                continue
            carrier[key] = value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
    ctx = extract(carrier)
    return context.attach(ctx)


def detach_context(token) -> None:
    context.detach(token)


def set_span_error(span: trace.Span, exc: BaseException) -> None:
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))


def force_flush(timeout_millis: int = 5000) -> None:
    """Flush pending spans (needed for short-lived CLI scripts)."""
    provider = trace.get_tracer_provider()
    flush = getattr(provider, 'force_flush', None)
    if callable(flush):
        flush(timeout_millis)
