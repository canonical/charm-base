# Copyright 2019-2021 Canonical Ltd.
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

import functools
import os
import pathlib
import shutil
import subprocess
import tempfile
import typing
import unittest

import pytest

import ops
from ops.model import _ModelBackend
from ops.storage import SQLiteStorage


def fake_script(test_case: unittest.TestCase, name: str, content: str):
    if not hasattr(test_case, 'fake_script_path'):
        fake_script_path = tempfile.mkdtemp('-fake_script')
        old_path = os.environ["PATH"]
        os.environ['PATH'] = os.pathsep.join([fake_script_path, os.environ["PATH"]])

        def cleanup():
            shutil.rmtree(fake_script_path)
            os.environ['PATH'] = old_path

        test_case.addCleanup(cleanup)
        test_case.fake_script_path = pathlib.Path(fake_script_path)  # type: ignore

    template_args: typing.Dict[str, str] = {
        'name': name,
        'path': test_case.fake_script_path.as_posix(),  # type: ignore
        'content': content,
    }

    path: pathlib.Path = test_case.fake_script_path / name  # type: ignore
    with path.open('wt') as f:  # type: ignore
        # Before executing the provided script, dump the provided arguments in calls.txt.
        # ASCII 1E is RS 'record separator', and 1C is FS 'file separator', which seem appropriate.
        f.write(  # type: ignore
            '''#!/bin/sh
{{ printf {name}; printf "\\036%s" "$@"; printf "\\034"; }} >> {path}/calls.txt
{content}'''.format_map(template_args))
    os.chmod(str(path), 0o755)  # type: ignore  # noqa: S103
    # TODO: this hardcodes the path to bash.exe, which works for now but might
    #       need to be set via environ or something like that.
    path.with_suffix(".bat").write_text(  # type: ignore
        f'@"C:\\Program Files\\git\\bin\\bash.exe" {path} %*\n')


def fake_script_calls(test_case: unittest.TestCase,
                      clear: bool = False) -> typing.List[typing.List[str]]:
    calls_file: pathlib.Path = test_case.fake_script_path / 'calls.txt'  # type: ignore
    if not calls_file.exists():  # type: ignore
        return []

    # newline and encoding forced to linuxy defaults because on
    # windows they're written from git-bash
    with calls_file.open('r+t', newline='\n', encoding='utf8') as f:  # type: ignore
        calls = [line.split('\x1e') for line in f.read().split('\x1c')[:-1]]  # type: ignore
        if clear:
            f.truncate(0)  # type: ignore
    return calls  # type: ignore


def create_framework(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    *,
    meta: typing.Union[ops.CharmMeta, None] = None):
    monkeypatch.setenv("PATH", str(pathlib.Path(__file__).parent / 'bin'), prepend=os.pathsep)
    monkeypatch.setenv("JUJU_UNIT_NAME", "local/0")

    tmpdir = pathlib.Path(tempfile.mkdtemp())
    request.addfinalizer(functools.partial(shutil.rmtree, str(tmpdir)))

    class CustomEvent(ops.EventBase):
        pass

    class TestCharmEvents(ops.CharmEvents):
        custom = ops.EventSource(CustomEvent)

    # Relations events are defined dynamically and modify the class attributes.
    # We use a subclass temporarily to prevent these side effects from leaking.
    ops.CharmBase.on = TestCharmEvents()  # type: ignore

    def cleanup():
        ops.CharmBase.on = ops.CharmEvents()  # type: ignore

    request.addfinalizer(cleanup)

    if not meta:
        meta = ops.CharmMeta()
    model = ops.Model(meta, _ModelBackend('local/0'))  # type: ignore
    # we can pass foo_event as event_name because we're not actually testing dispatch
    framework = ops.Framework(SQLiteStorage(':memory:'), tmpdir, meta, model)  # type: ignore
    request.addfinalizer(framework.close)

    return framework


class FakeScript:
    def __init__(self,
                 monkeypatch: pytest.MonkeyPatch,
                 tmp_path: pathlib.Path,
                 fake_script_path: typing.Union[pathlib.Path, None] = None):
        self.monkeypatch = monkeypatch
        if fake_script_path is None:
            self.fake_script_path = tmp_path
            self.monkeypatch.setenv(
                "PATH",
                str(self.fake_script_path),
                prepend=os.pathsep)  # type: ignore
        else:
            self.fake_script_path = fake_script_path

    def write(self,
              name: str,
              content: str):
        template_args: typing.Dict[str, str] = {
            'name': name,
            'path': self.fake_script_path.as_posix(),  # type: ignore
            'content': content,
        }

        path: pathlib.Path = self.fake_script_path / name  # type: ignore
        with path.open('wt') as f:  # type: ignore
            # Before executing the provided script, dump the provided arguments in calls.txt.
            # ASCII 1E is RS 'record separator', and 1C is FS 'file separator', which
            # seem appropriate.
            f.write(  # type: ignore
                '''#!/bin/sh
    {{ printf {name}; printf "\\036%s" "$@"; printf "\\034"; }} >> {path}/calls.txt
    {content}'''.format_map(template_args))
        os.chmod(str(path), 0o755)  # type: ignore  # noqa: S103
        # TODO: this hardcodes the path to bash.exe, which works for now but might
        #       need to be set via environ or something like that.
        path.with_suffix(".bat").write_text(  # type: ignore
            f'@"C:\\Program Files\\git\\bin\\bash.exe" {path} %*\n')

    def calls(self, clear: bool = False) -> typing.List[typing.List[str]]:
        calls_file: pathlib.Path = self.fake_script_path / 'calls.txt'  # type: ignore
        if not calls_file.exists():  # type: ignore
            return []

        # newline and encoding forced to linuxy defaults because on
        # windows they're written from git-bash
        with calls_file.open('r+t', newline='\n', encoding='utf8') as f:  # type: ignore
            calls = [line.split('\x1e') for line in f.read().split('\x1c')[:-1]]  # type: ignore
            if clear:
                f.truncate(0)  # type: ignore
        return calls  # type: ignore


class FakeScriptTest(unittest.TestCase):

    def test_fake_script_works(self):
        fake_script(self, 'foo', 'echo foo runs')
        fake_script(self, 'bar', 'echo bar runs')
        # subprocess.getoutput goes via the shell, so it needs to be
        # something both sh and CMD understand
        output = subprocess.getoutput('foo a "b c " && bar "d e" f')
        assert output == 'foo runs\nbar runs'
        assert fake_script_calls(self) == [
            ['foo', 'a', 'b c '],
            ['bar', 'd e', 'f'],
        ]

    def test_fake_script_clear(self):
        fake_script(self, 'foo', 'echo foo runs')

        output = subprocess.getoutput('foo a "b c"')
        assert output == 'foo runs'

        assert fake_script_calls(self, clear=True) == [['foo', 'a', 'b c']]

        fake_script(self, 'bar', 'echo bar runs')

        output = subprocess.getoutput('bar "d e" f')
        assert output == 'bar runs'

        assert fake_script_calls(self, clear=True) == [['bar', 'd e', 'f']]

        assert fake_script_calls(self, clear=True) == []


class BaseTestCase(unittest.TestCase):

    def create_framework(self,
                         *,
                         model: typing.Optional[ops.Model] = None,
                         tmpdir: typing.Optional[pathlib.Path] = None):
        """Create a Framework object.

        By default operate in-memory; pass a temporary directory via the 'tmpdir'
        parameter if you wish to instantiate several frameworks sharing the
        same dir (e.g. for storing state).
        """
        if tmpdir is None:
            data_fpath = ":memory:"
            charm_dir = 'non-existant'
        else:
            data_fpath = tmpdir / "framework.data"
            charm_dir = tmpdir

        framework = ops.Framework(
            SQLiteStorage(data_fpath),
            charm_dir,
            meta=model._cache._meta if model else ops.CharmMeta(),
            model=model)  # type: ignore
        self.addCleanup(framework.close)
        return framework

    def create_model(self):
        """Create a Model object."""
        backend = _ModelBackend(unit_name='myapp/0')
        meta = ops.CharmMeta()
        model = ops.Model(meta, backend)
        return model
