#!/usr/bin/python3

import unittest

import juju.model
import juju.charm


class TestModelBackend:
    def relation_ids(self, relation_name):
        return {
            'db0': [],
            'db1': ['db1:1'],
            'db2': ['db2:1', 'db2:2'],
        }[relation_name]

    def relation_list(self, relation_id):
        return {
            'db1:1': ['remoteapp1/0'],
            'db2:1': ['remoteapp1/0'],
            'db2:2': ['remoteapp2/0'],
        }[relation_id]

    def relation_get(self, relation_id, member_name):
        return {
            'db1:1': {
                'myapp/0': {'host': 'myapp-0'},
                'remoteapp1/0': {'host': 'remoteapp1-0'},
            },
            'db2:1': {
                'myapp/0': {'host': 'myapp-0'},
                'remoteapp1/0': {'host': 'remoteapp1-0'},
            },
            'db2:2': {
                'myapp/0': {'host': 'myapp-0'},
                'remoteapp2/0': {'host': 'remoteapp2-0'},
            },
        }[relation_id][member_name]

    def config_get(self):
        return {
            'foo': 'foo',
            'bar': 1,
            'qux': True,
        }


class TestModel(unittest.TestCase):
    def setUp(self):
        self.charm_env = juju.charm.CharmEnv(unit_name='myapp/0')
        self.charm_env.metadata.relations = ['db0', 'db1', 'db2']
        self.model = juju.model.Model(self.charm_env, TestModelBackend())

    def test_model(self):
        self.assertIs(self.model.app, self.model.unit.app)
        for relation in self.model.relations['db2']:
            self.assertIn(self.model.unit, relation.data)
            unit_from_rel = next(filter(lambda u: u.name == 'myapp/0', relation.data.keys()))
            self.assertIs(self.model.unit, unit_from_rel)
        self.assertIsNone(self.model.relation('db0'))
        self.assertIsInstance(self.model.relation('db1'), juju.model.Relation)
        with self.assertRaises(juju.model.TooManyRelatedApps):
            self.model.relation('db2')
        random_unit = self.model._cache.get(juju.model.Unit, 'randomunit/0')
        with self.assertRaises(KeyError):
            self.model.relation('db1').data[random_unit]
        remoteapp1_0 = next(filter(lambda u: u.name == 'remoteapp1/0', self.model.relation('db1').units))
        self.assertEqual(self.model.relation('db1').data[remoteapp1_0], {'host': 'remoteapp1-0'})

    def test_config(self):
        self.assertEqual(self.model.config, {
            'foo': 'foo',
            'bar': 1,
            'qux': True,
        })
        with self.assertRaises(TypeError):
            # Confirm that we cannot modify config values.
            self.model.config['foo'] = 'bar'
