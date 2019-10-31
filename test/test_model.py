#!/usr/bin/python3

import unittest

import juju.model


# TODO: We need some manner of test to validate the actual ModelBackend implementation, round-tripped
# through the actual subprocess calls. Either this class could implement these functions as executables
# that were called via subprocess, or more simple tests that just test through ModelBackend while leaving
# these tests alone, depending on what proves easier.
class TestModelBackend:
    def __init__(self, is_leader=False):
        self.is_leader = is_leader
        self.relation_set_calls = []

    def relation_ids(self, relation_name):
        return {
            'db0': [],
            'db1': [4],
            'db2': [5, 6],
        }[relation_name]

    def relation_list(self, relation_id):
        try:
            return {
                4: ['remoteapp1/0'],
                5: ['remoteapp1/0'],
                6: ['remoteapp2/0'],
            }[relation_id]
        except KeyError:
            raise juju.model.RelationNotFound()

    def relation_get(self, relation_id, member_name, app):
        try:
            return {
                4: {
                    'myapp/0': {'host': 'myapp-0'},
                    'remoteapp1/0': {'host': 'remoteapp1-0'},
                    'myapp': {'password': 'deadbeefcafe'},
                    'remoteapp1': {'secret': 'cafedeadbeef'}
                },
                5: {
                    'myapp/0': {'host': 'myapp-0'},
                    'remoteapp1/0': {'host': 'remoteapp1-0'},
                    'myapp': {'password': 'deadbeefcafe'},
                    'remoteapp1': {'secret': 'cafedeadbeef'}
                },
                6: {
                    'myapp/0': {'host': 'myapp-0'},
                    'remoteapp2/0': {'host': 'remoteapp2-0'},
                    'myapp': {'password': 'deadbeefcafe'},
                    'remoteapp1': {'secret': 'cafedeadbeef'}
                },
            }[relation_id][member_name]
        except KeyError:
            raise juju.model.RelationNotFound()

    def relation_set(self, relation_id, key, value, app):
        if relation_id == 5:
            raise ValueError()
        self.relation_set_calls.append((relation_id, key, value, app))

    def config_get(self):
        return {
            'foo': 'foo',
            'bar': 1,
            'qux': True,
        }

    def is_leader(self):
        return self.is_leader


class TestModel(unittest.TestCase):
    def setUp(self):
        self.backend = TestModelBackend()
        self.model = juju.model.Model('myapp/0', ['db0', 'db1', 'db2'], self.backend)

    def test_model(self):
        self.assertIs(self.model.app, self.model.unit.app)

    def test_relations_keys(self):
        for relation in self.model.relations['db2']:
            self.assertIn(self.model.unit, relation.data)
            unit_from_rel = next(filter(lambda u: u.name == 'myapp/0', relation.data.keys()))
            self.assertIs(self.model.unit, unit_from_rel)

    def test_get_relation(self):
        with self.assertRaises(juju.model.ModelError):
            self.model.get_relation('db1', 'db1:4')
        db1_4 = self.model.get_relation('db1', 4)
        self.assertIsInstance(db1_4, juju.model.Relation)
        dead_rel = self.model.get_relation('db1', 7)
        self.assertIsInstance(dead_rel, juju.model.Relation)
        self.assertEqual(list(dead_rel.data.keys()), [self.model.unit, self.model.unit.app])
        self.assertEqual(dead_rel.data[self.model.unit], {})
        self.assertIsNone(self.model.get_relation('db0'))
        self.assertIs(self.model.get_relation('db1'), db1_4)
        with self.assertRaises(juju.model.TooManyRelatedApps):
            self.model.get_relation('db2')

    def test_unit_relation_data(self):
        random_unit = self.model._cache.get(juju.model.CacheKey(juju.model.Unit, 'randomunit/0'), 'randomunit/0', False, self.model._backend, self.model._cache)
        with self.assertRaises(KeyError):
            self.model.get_relation('db1').data[random_unit]
        remoteapp1_0 = next(filter(lambda u: u.name == 'remoteapp1/0', self.model.get_relation('db1').units))
        self.assertEqual(self.model.get_relation('db1').data[remoteapp1_0], {'host': 'remoteapp1-0'})

    def test_remote_app_relation_data(self):
        random_app = self.model._cache.get(juju.model.CacheKey(juju.model.Application, 'randomapp'), 'randomapp', False, self.model._backend)
        with self.assertRaises(KeyError):
            self.model.get_relation('db1').data[random_app]
        remoteapp1 = self.model.get_relation('db1').app
        self.assertEqual(self.model.get_relation('db1').data[remoteapp1], {'secret': 'cafedeadbeef'})

    def test_relation_data_modify_remote(self):
        rel_db1 = self.model.get_relation('db1')
        remoteapp1_0 = next(filter(lambda u: u.name == 'remoteapp1/0', self.model.get_relation('db1').units))
        # Force memory cache to be loaded.
        self.assertIn('host', rel_db1.data[remoteapp1_0])
        with self.assertRaises(juju.model.RelationDataError):
            rel_db1.data[remoteapp1_0]['foo'] = 'bar'
        self.assertEqual(self.backend.relation_set_calls, [])
        self.assertNotIn('foo', rel_db1.data[remoteapp1_0])

    def test_relation_data_modify_local(self):
        rel_db1 = self.model.get_relation('db1')
        # Force memory cache to be loaded.
        self.assertIn('host', rel_db1.data[self.model.unit])
        rel_db1.data[self.model.unit]['host'] = 'bar'
        self.assertEqual(self.backend.relation_set_calls, [(4, 'host', 'bar', False)])
        self.assertEqual(rel_db1.data[self.model.unit]['host'], 'bar')

    def test_app_relation_data_modify_local_as_leader(self):
        # Used for tweaking backend's is-leader behavior.
        self.backend = TestModelBackend(is_leader=True)
        self.model = juju.model.Model('myapp/0', ['db0', 'db1', 'db2'], self.backend)

        local_app = self.model.unit.app

        rel_db1 = self.model.get_relation('db1')
        self.assertEqual(rel_db1.data[local_app], {'password': 'deadbeefcafe'})

        rel_db1.data[local_app]['password'] = 'foo'
        self.assertEqual(self.backend.relation_set_calls, [(4, 'password', 'foo', True)])
        self.assertEqual(rel_db1.data[local_app]['password'], 'foo')

    def test_app_relation_data_modify_local_as_minion(self):
        local_app = self.model.unit.app

        rel_db1 = self.model.get_relation('db1')
        self.assertEqual(rel_db1.data[local_app], {'password': 'deadbeefcafe'})

        self.backend = TestModelBackend(is_leader=False)
        with self.assertRaises(juju.model.RelationDataError):
            rel_db1.data[local_app]['password'] = 'foobar'

    def test_relation_data_del_key(self):
        rel_db1 = self.model.get_relation('db1')
        # Force memory cache to be loaded.
        self.assertIn('host', rel_db1.data[self.model.unit])
        del rel_db1.data[self.model.unit]['host']
        self.assertEqual(self.backend.relation_set_calls, [(4, 'host', '', False)])
        self.assertNotIn('host', rel_db1.data[self.model.unit])

    def test_relation_set_fail(self):
        rel_db2 = self.model.relations['db2'][0]
        # Force memory cache to be loaded.
        self.assertIn('host', rel_db2.data[self.model.unit])
        with self.assertRaises(ValueError):
            rel_db2.data[self.model.unit]['host'] = 'bar'
        self.assertEqual(rel_db2.data[self.model.unit]['host'], 'myapp-0')
        with self.assertRaises(ValueError):
            del rel_db2.data[self.model.unit]['host']
        self.assertIn('host', rel_db2.data[self.model.unit])

    def test_relation_data_type_check(self):
        rel_db1 = self.model.get_relation('db1')
        with self.assertRaises(juju.model.RelationDataError):
            rel_db1.data[self.model.unit]['foo'] = 1
        with self.assertRaises(juju.model.RelationDataError):
            rel_db1.data[self.model.unit]['foo'] = {'foo': 'bar'}
        with self.assertRaises(juju.model.RelationDataError):
            rel_db1.data[self.model.unit]['foo'] = None
        self.assertEqual(self.backend.relation_set_calls, [])

    def test_config(self):
        self.assertEqual(self.model.config, {
            'foo': 'foo',
            'bar': 1,
            'qux': True,
        })
        with self.assertRaises(TypeError):
            # Confirm that we cannot modify config values.
            self.model.config['foo'] = 'bar'
