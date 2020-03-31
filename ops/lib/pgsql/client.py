# Copyright 2020 Canonical Ltd.
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

"""Implements the requires side handling of a Juju 'pgsql' interface.

- `PostgreSQLClient`_ is the primary interaction point for clients. Most commonly used as
something like::

     from ops.lib.pgsql.client import PostgreSQLClient
     class MyCharm(ops.charm.CharmBase):
         def __init__(self, framework):
            super().__init__(framework, None)

            self.pgsql = PostgreSQLClient(self, "endpoint")
            self.framework.observe(self.pgsql.on.master_changed, self.on_pgsql_changed)
        ...
        def on_pgsql_changed(self):
            try:
              master = self.pgsql.master()
            except PostgreSQLError as e:
              # We are waiting or blocked because the PostgreSQL master is not ready
              self.unit.status = e.status
              return
            connection_info = {
                'database': master.database,
                'host': master.host,
                'port': master.port,
            }

- `PostgresqlError`_ is the exception type for errors from this module. It has an attribute
    '.status' which is the recommended status for the unit, to indicate to the user why their
    application isn't ready yet.
"""

import re

from ...framework import Object, EventBase, EventSetBase, EventSource, StoredState
from ...model import ModelError, BlockedStatus, WaitingStatus

key_value_re = re.compile(r"""(?x)
                               (\w+) \s* = \s*
                               (?:
                                 (\S*)
                               )
                               (?=(?:\s|\Z))
                           """)


class PostgreSQLError(ModelError):
    """All errors raised by interface-pgsql will be subclasses of this error.

    It provides the attribute self.status to indicate what status and message the Unit should use
    based on this relation. (Eg, if there is no relation to PGSQl, it will raise a
    BlockedStatus('Missing relation <relation-name>')
    """

    def __init__(self, kind, message, relation_name):
        super().__init__()
        self.status = kind('{}: {}'.format(message, relation_name))


class PostgreSQLDatabase:
    """"Represents the connection information to PostgreSQL.

    :var master: A libpq connection string that allows you to connect to PostgreSQL
    :type master: str
    """

    def __init__(self, master):
        # This is a pgsql 'key=value key2=value' connection string
        self.master = master
        self.properties = {}
        for key, val in key_value_re.findall(master):
            if key not in self.properties:
                self.properties[key] = val

    @property
    def host(self):
        return self.properties['host']

    @property
    def database(self):
        return self.properties['dbname']

    @property
    def port(self):
        return self.properties['port']

    @property
    def user(self):
        return self.properties['user']

    @property
    def password(self):
        return self.properties['password']


class PostgreSQLMasterChanged(EventBase):
    """Event emitted by PostgreSQLClient.on.master_changed.

    :var master: The master connection string for the changed master.
    """

    def __init__(self, handle, master):
        super().__init__(handle)
        self.master = master
        # TODO: jam 2020-03-31 should .master actually be a PostgreSQLDatabase like you would
        #  get from calling PostgreSQLClient.master() ?

    def snapshot(self):
        return {'master': self.master}

    def restore(self, snapshot):
        self.master = snapshot['master']


class PostgreSQLEvents(EventSetBase):
    """"The events that can be generated by PostgreSQLClient."""
    master_changed = EventSource(PostgreSQLMasterChanged)


class PostgreSQLClient(Object):
    """This provides a Client that understands how to communicate with the PostgreSQL Charm.

    The two primary methods is .master() and on.master_changed.
    master() returns a PostgreSQLDatabase once all configuration has been successfully handled,
    and will raise PostgreSQLError the relation or master is not properly established yet.

    My default the PostgreSQL charm will create a database based on the name of the application
    that is connecting to it, and set some default roles. If these need to be overridden by you
    charm, use set_database_name, set_roles, or set_extensions as appropriate.
    The PostgreSQL charm will acknowledge requests to change the default database, and whether
    your unit is allowed to connect. This client encapsulates that logic, and master() will
    raise an exception as long as postgresql has not finished setting up all support for your
    application.
    """
    on = PostgreSQLEvents()
    _stored = StoredState()

    def __init__(self, charm, relation_name):
        if charm is None:
            raise RuntimeError('must pass a valid CharmBase')
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._charm = charm
        self.framework.observe(
            charm.on[self._relation_name].relation_changed, self._on_relation_changed)
        self.framework.observe(
            charm.on[self._relation_name].relation_broken, self._on_relation_broken)
        self._stored.set_default(master=None)

    def master(self):
        """Retrieve the libpq connection string for the Master postgresql database.

        This method will raise PostgreSQLError with a status of either Blocked or Waiting if the
        error does/doesn't need user intervention.
        """
        relations = self.framework.model.relations[self._relation_name]
        if len(relations) == 1:
            if self._stored.master is None:
                raise PostgreSQLError(WaitingStatus, 'master not ready yet', self._relation_name)
            return PostgreSQLDatabase(self._stored.master)
        if len(relations) == 0:
            raise PostgreSQLError(BlockedStatus, 'missing relation', self._relation_name)
        if len(relations) > 1:
            raise PostgreSQLError(
                BlockedStatus,
                'too many related applications',
                self._relation_name)

    def standbys(self):
        """Retrieve the connection strings for all PostgreSQL standby machines."""
        # TODO: support the HA model used by PostgreSQL charm with active standby's, etc.
        raise NotImplementedError

    def set_database_name(self, value):
        """Indicate the database that this charm wants to use."""
        # request the database name from postgresql
        for relation in self._charm.model.relations[self._relation_name]:
            relation.data[self._charm.model.unit]['database'] = value

    def set_roles(self, value):
        """Indicate what roles you want available from PostgreSQL."""
        for relation in self._charm.model.relations[self._relation_name]:
            relation.data[self._charm.model.unit]['roles'] = value

    def set_extensions(self, value):
        """Indicate what extensions you want available from PostgreSQL."""
        for relation in self._charm.model.relations[self._relation_name]:
            relation.data[self._charm.model.unit]['extensions'] = value

    def _is_relation_ready(self, my_data, remote_data):
        # TODO: the pgsql charm likes to report that you can't actually connect as long as
        #   local[egress-subnets] is not a subset of remote[allowed-subnets] and
        #   the requested database, roles and extensions all match the values provided by remote
        # TODO: old versions of the charm only used allowed_units and not allowed_subnets,
        #  should we be compatible with older versions?
        allowed_subnets = remote_data.get('allowed-subnets')
        if allowed_subnets is not None:
            allowed_set = set(comma_separated_list(allowed_subnets))
            egress_subnets = my_data.get('egress-subnets', '')
            egress_set = set(comma_separated_list(egress_subnets))
            if not egress_set.issubset(allowed_set):
                return False
        requested_database = my_data.get('database')
        if requested_database is not None:
            if remote_data.get('database', '') != requested_database:
                return False
        requested_roles = my_data.get('roles')
        if requested_roles is not None:
            if remote_data.get('roles', '') != requested_roles:
                return False
        requested_extensions = my_data.get('extensions')
        if requested_extensions is not None:
            if remote_data.get('extensions', '') != requested_extensions:
                return False
        return True

    def _on_relation_changed(self, event):
        # Check to see if the master is now at a different location
        relation = event.relation
        data = relation.data[event.unit]
        # TODO: do we check if any related units have a 'master' set?
        #  Also, we need to check if we actually have the database, roles, and access that we want
        master = data.get('master')
        if not self._is_relation_ready(relation.data[self._charm.model.unit], data):
            # Not ready to set master
            return
        should_emit = self._stored.master != master
        if should_emit:
            self._stored.master = master
            self.on.master_changed.emit(master)

    def _on_relation_broken(self, event):
        pass


def comma_separated_list(s):
    """Convert a string holding comma separated values into a python list."""
    return [part.strip() for part in s.split(',') if part.strip()]
