# Copyright 2022 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
# file except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.
"""FIXME Docstring."""

from __future__ import annotations

import contextlib
import logging
import os
import time
from typing import Sequence

from opentelemetry.exporter.otlp.proto.common._internal import trace_encoder
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor  # type: ignore
from opentelemetry.instrumentation.urllib import URLLibInstrumentor  # type: ignore
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import get_tracer_provider, set_tracer_provider

import ops
import ops._tracing.buffer
import ops.jujucontext
import ops.log

logger = logging.getLogger(__name__)
# Trace `sqlite3` usage by ops storage component
SQLite3Instrumentor().instrument()
# Trace `urllib` usage when talking to Pebble
URLLibInstrumentor().instrument()

_OTLP_SPAN_EXPORTER_TIMEOUT = 1  # seconds
"""How much to give OTLP span exporter has to push traces to the backend."""

SENDOUT_FACTOR = 2
"""How much buffered chunks to send out for each incoming chunk."""

# FIXME: this creates a separate file next to the CHARM_STATE_FILE
# We could stuff both kinds of data into the same file, I guess?
BUFFER_FILE = '.tracing-data.db'
# Currently ops.storage keeps one long transaction open for the duration of the
# the dispatch, which means we can't use the same file from another thread.
# BUFFER_FILE = '.unit-state.db'


_exporter: ProxySpanExporter | None = None


class ProxySpanExporter(SpanExporter):
    real_exporter: SpanExporter | None = None

    def __init__(self, buffer_path: str):
        self.buffer = ops._tracing.buffer.Buffer(buffer_path)

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Export a batch of telemetry data.

        Note: to avoid data loops or recursion, this function cannot be instrumented.
        """
        with suppress_juju_log_handler():
            # Note:
            # this is called in a helper thread, which is daemonic,
            # the MainThread will wait at most 10s for this thread.
            # Margins:
            # - 1s safety margin
            # - 1s for buffered data time overhang
            # - 2s for live data
            deadline = time.monotonic() + 6

            assert spans  # the BatchSpanProcessor won't call us if there's no data
            # TODO:  this will change in the JSON experiment
            data: bytes = trace_encoder.encode_spans(spans).SerializePartialToString()
            logging.debug(f'FIXME export {len(spans)=} {len(data)=}')
            rv = self.buffer.pump(data)
            logging.debug('FIXME saved')
            assert rv
            self.do_export(*rv)

            for _ in range(SENDOUT_FACTOR - 1):
                if time.monotonic() > deadline:
                    break
                if not (rv := self.buffer.pump()):
                    break
                self.do_export(*rv)

            return SpanExportResult.SUCCESS

    def do_export(self, buffered_id: int, data: bytes) -> None:
        """Export buffered data and remove it from the buffer on success."""
        logging.debug(f'FIXME asked {buffered_id=} {len(data)=}')
        # TODO:  this will change in the JSON experiment
        if self.real_exporter and self.real_exporter._export(data).ok:  # type: ignore
            logging.debug('FIXME removing')
            self.buffer.remove(buffered_id)

    def shutdown(self) -> None:
        """Shut down the exporter."""
        if self.real_exporter:
            self.real_exporter.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """No-op, as the real exporter doesn't buffer."""
        return True

    def set_real_exporter(self, exporter: SpanExporter) -> None:
        self.real_exporter = exporter


@contextlib.contextmanager
def suppress_juju_log_handler():
    handlers = [h for h in logging.root.handlers if isinstance(h, ops.log.JujuLogHandler)]
    if not handlers:
        yield
        return

    juju_log_handler = handlers[0]
    token = juju_log_handler.drop.set(True)
    try:
        yield
    finally:
        juju_log_handler.drop.reset(token)


def setup_tracing(charm_class_name: str) -> None:
    global _exporter
    # FIXME would it be better to pass Juju context explicitly?
    juju_context = ops.jujucontext._JujuContext.from_dict(os.environ)
    app_name = '' if juju_context.unit_name is None else juju_context.unit_name.split('/')[0]
    service_name = f'{app_name}-charm'  # only one COS charm sets custom value

    resource = Resource.create(
        attributes={
            'service.name': service_name,
            'compose_service': service_name,  # FIXME why is this copy needed?
            'charm_type': charm_class_name,
            # juju topology
            'juju_unit': juju_context.unit_name,
            'juju_application': app_name,
            'juju_model': juju_context.model_name,
            'juju_model_uuid': juju_context.model_uuid,
        }
    )
    provider = TracerProvider(resource=resource)

    # How

    buffer_path = str(juju_context.charm_dir / BUFFER_FILE)
    _exporter = ProxySpanExporter(buffer_path)
    span_processor = BatchSpanProcessor(_exporter)
    provider.add_span_processor(span_processor)
    logging.debug('FIXME setup_tracing')
    set_tracer_provider(provider)


# FIXME make it very cheap to call this method a second time with same arguments
def set_tracing_destination(
    *,
    url: str | None,
    ca: str | None,
) -> None:
    # FIXME needs a threading.Lock
    # or access to underlying BatchXXX lock
    #
    # - check if settings are exactly same, do nothing in that case
    # - replace current exported with a new exporter
    if ca is not None and not ca.startswith('/'):
        raise ValueError(f'{ca=} must be an absolute path')
    assert _exporter

    # real exporter, hardcoded for now
    real_exporter = OTLPSpanExporter(url, timeout=1)
    # This is actually the max delay value in the sequence 1, 2, ..., MAX
    # Set to 1 to disable sending live data (buffered data is still eventually sent)
    # Set to 2 (or more) to enable sending live data (after buffered)
    #
    # _MAX_RETRY_TIMEOUT = 2 with timeout=1 means:
    # - 1 attempt to send live, 1s sleep in the worst case
    # _MAX_RETRY_TIMEOUT = 3 or 4 with timeout=1 means:
    # - 1st attempt, 1s sleep, 2nd attempt, 1s sleep in the worst case
    real_exporter._MAX_RETRY_TIMEOUT = 2  # type: ignore
    _exporter.set_real_exporter(real_exporter)
    _exporter.buffer.mark_observed()


def shutdown_tracing() -> None:
    """Shutdown tracing, which is expected to flush the buffered data out."""
    logging.debug('FIXME shutdown tracing')
    get_tracer_provider().shutdown()  # type: ignore
