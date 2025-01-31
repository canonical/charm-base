# Copyright 2025 Canonical Ltd.
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
"""Buffer for tracing data."""

from __future__ import annotations

import contextlib
import functools
import logging
import sqlite3
from typing import Callable

from typing_extensions import ParamSpec, TypeVar
from typing_extensions import reveal_type as reveal_type  # FIXME

# Approximate safety limit for the database file size
BUFFER_SIZE = 40 * 1024 * 1024

# Default priority for tracing data.
# Dispatch invocation that doesn't result in any event being observed
# by charm or its charm lib produces data at this priority.
DEFAULT_PRIORITY = 10

# Higher priority for data from invocation with observed events.
OBSERVED_PRIORITY = 50

# Must have a short timeout when terminating
# May want to have a longer timeout otherwise
DB_TIMEOUT = 5
LONG_DB_TIMEOUT = 3600

# Must use isolation_level=None for consistency between Python 3.8 and 3.12
# Can't use the STRICT keyword for tables, requires sqlite 3.37.0
# Can't use the octet_length() either, requires sqlite 3.43.0
#
# Ubuntu 20.04  Python  3.8.2  Sqlite 3.31.1  Adds UPSERT, window functions
# Ubuntu 22.04  Python 3.10.x  Sqlite 3.37.2  Adds STRICT tables, JSON ops
# Ubuntu 24.04  Python 3.12.x  Sqlite 3.45.2  Adds math functions

logger = logging.getLogger(__name__)

P = ParamSpec('P')
R = TypeVar('R')


def retry(f: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        exc: sqlite3.Error | None = None

        for _ in range(3):
            try:
                return f(*args, **kwargs)
            except sqlite3.Error as e:  # noqa: PERF203
                exc = e
                continue
        else:
            assert exc
            raise exc

    return wrapper


class Buffer:
    """Buffer for tracing data.

    Access buffer attributes is effectively protected by an sqlite transaction.
    """

    ids: set[int]
    """tracing data ids buffered during this dispatch invocation."""
    observed = False
    """Marks that data from this dispatch invocation has been marked observed."""

    def __init__(self, path: str):
        self.path = path
        self.ids = set()
        self._set_db_schema()

    @retry
    def _set_db_schema(self):
        # NOTE: measure the cost of this vs two-level approach:
        # - check table and index in read-only mode
        # - if needed, update the DSL
        # NOTE: ops storage sets u+rw, go-rw permissions
        # should we follow suit?
        with self.tx(timeout=LONG_DB_TIMEOUT) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tracing (
                    -- effectively auto-incremented
                    id INTEGER PRIMARY KEY,
                    -- observed events are more important
                    priority INTEGER NOT NULL,
                    -- Protobuf-formatted tracing data
                    data BLOB NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS tracing_priority_id
                ON tracing
                (priority, id)
                """
            )

    @contextlib.contextmanager
    def tx(self, *, timeout: float = DB_TIMEOUT, readonly: bool = False):
        """Thread-safe transaction context manager."""
        with sqlite3.connect(self.path, isolation_level=None, timeout=timeout) as conn:
            mode = 'DEFERRED' if readonly else 'IMMEDIATE'
            conn.execute(f'BEGIN {mode}')
            try:
                yield conn
            except:
                conn.execute('ROLLBACK')
                raise
            else:
                conn.execute('COMMIT')

    # TODO:
    # add some retry mechanism...
    # when database is written to from two threads,
    # either we'd get an exaception on BEGIN (immediate mode)
    # or we'd get an exception on COMMIT (deferred mode)
    # either way, a retry will be needed sooner or later.

    @retry
    def mark_observed(self):
        if self.observed:
            return

        with self.tx(timeout=LONG_DB_TIMEOUT) as conn:
            conn.execute(
                f"""
                UPDATE tracing
                SET priority = ?
                WHERE id IN ({','.join(('?',) * len(self.ids))})
                """,  # noqa: S608
                (OBSERVED_PRIORITY, *tuple(self.ids)),
            )
        self.observed = True
        self.ids.clear()

    @retry
    def pump(self, chunk: bytes | None = None) -> tuple[int, bytes] | None:
        """Pump the buffer queue.

        Accepts an optional new data chunk.
        Removes old, boring data if needed.
        Returns the oldest important record.
        """
        # NOTE: discussion about transaction type:
        # - this may be a read-only transaction (no data to save, read out one record)
        # - or a read transaction later upgraded to write (check space, then delete some)
        # currently I've made `self.tx()` return a write transaction always
        # which is safer, but may incur a filesystem modification cost.
        with self.tx(readonly=not chunk) as conn:
            if chunk:
                # Ensure that there's enough space in the buffer
                chunklen = (len(chunk) + 4095) // 4096 * 4096
                stored: int | None = conn.execute(
                    """
                    SELECT sum((length(data)+4095)/4096*4096)
                    FROM tracing
                    """
                ).fetchone()[0]
                # TODO: expose `stored` in metrics, one day
                excess = (stored or 0) + chunklen - BUFFER_SIZE
                logging.debug(f'{excess=}')

                if excess > 0:
                    # Drop lower-priority, older data
                    cursor = conn.execute(
                        """
                        SELECT id, (length(data)+4095)/4096*4096
                        FROM tracing
                        ORDER BY priority ASC, id ASC
                        """
                    )

                    collected_ids: set[int] = set()
                    collected_size: int = 0
                    for id_, size in cursor:
                        collected_ids.add(id_)
                        collected_size += size
                        if collected_size > excess:
                            break

                    assert collected_ids
                    logging.debug(f'{len(collected_ids)=}')
                    conn.execute(
                        f"""
                        DELETE FROM tracing
                        WHERE id IN ({','.join(('?',) * len(collected_ids))})
                        """,  # noqa: S608
                        tuple(collected_ids),
                    )

                # Store the new tracing data
                priority = OBSERVED_PRIORITY if self.observed else DEFAULT_PRIORITY
                cursor = conn.execute(
                    """
                    INSERT INTO tracing (priority, data)
                    VALUES (?, ?)
                    """,
                    (priority, chunk),
                )

                assert cursor.lastrowid is not None
                if not self.observed:
                    self.ids.add(cursor.lastrowid)

            # Return oldest important data
            return conn.execute(
                """
                SELECT id, data
                FROM tracing
                ORDER BY priority DESC, id ASC
                LIMIT 1
                """
            ).fetchone()

    @retry
    def remove(self, id_: int):
        with self.tx() as conn:
            conn.execute(
                """
                DELETE FROM tracing
                WHERE id = ?
                """,
                (id_,),
            )
        self.ids -= {id_}
