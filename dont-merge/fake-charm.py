#!/usr/bin/env python
# Copyright 2025 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""FIXME dummy_load docstring."""

from __future__ import annotations

import time

import opentelemetry.trace

import ops

tracer = opentelemetry.trace.get_tracer(__name__)


class DatabaseReadyEvent(ops.charm.EventBase):
    """Event representing that the database is ready."""


class DatabaseRequirerEvents(ops.framework.ObjectEvents):
    """Container for Database Requirer events."""

    ready = ops.charm.EventSource(DatabaseReadyEvent)


class DatabaseRequirer(ops.framework.Object):
    """Dummy docstring."""

    on = DatabaseRequirerEvents()  # type: ignore

    def __init__(self, charm: ops.CharmBase):
        """Dummy docstring."""
        super().__init__(charm, 'foo')
        self.framework.observe(charm.on.start, self._on_db_changed)

    def _on_db_changed(self, event: ops.StartEvent) -> None:
        """Dummy docstring."""
        self.on.ready.emit()


class FakeCharm(ops.CharmBase):
    """Dummy docstring."""

    def __init__(self, framework: ops.Framework):
        """Dummy docstring."""
        super().__init__(framework)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.collect_app_status, self._on_collect_app_status)
        self.db_requirer = DatabaseRequirer(self)
        self.framework.observe(self.db_requirer.on.ready, self._on_db_ready)

    def _on_start(self, event: ops.StartEvent) -> None:
        """Dummy docstring."""
        ops.set_tracing_destination(url='http://localhost:4318/v1/traces')
        self.dummy_load(event, 0.0025)

    def _on_db_ready(self, event: DatabaseReadyEvent) -> None:
        self.dummy_load(event, 0.001)

    def _on_collect_app_status(self, event: ops.CollectStatusEvent) -> None:
        """Dummy docstring."""
        self.dummy_load(event)
        event.add_status(ops.ActiveStatus('app seems ready'))

    def _on_collect_unit_status(self, event: ops.CollectStatusEvent) -> None:
        """Dummy docstring."""
        self.dummy_load(event)
        event.add_status(ops.ActiveStatus('unit ready'))

    @tracer.start_as_current_span('FakeCharm.dummy_load')  # type: ignore
    def dummy_load(self, event: ops.EventBase, duration: float = 0.001) -> None:
        """Dummy docstring."""
        print(event)
        time.sleep(duration)


if __name__ == '__main__':
    ops.main(FakeCharm)
