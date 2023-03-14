import marshal
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import yaml
from ops.framework import _event_regex
from ops.storage import SQLiteStorage

from scenario.logger import logger as scenario_logger
from scenario.ops_main_mock import NoObserverError

if TYPE_CHECKING:
    from ops.charm import CharmBase
    from ops.testing import CharmType

    from scenario.state import Event, State, _CharmSpec

    _CT = TypeVar("_CT", bound=Type[CharmType])

    PathLike = Union[str, Path]

logger = scenario_logger.getChild("runtime")
# _stored_state_regex = "(.*)\/(\D+)\[(.*)\]"
_stored_state_regex = "((?P<owner_path>.*)\/)?(?P<data_type_name>\D+)\[(?P<name>.*)\]"

RUNTIME_MODULE = Path(__file__).parent


class ScenarioRuntimeError(RuntimeError):
    """Base class for exceptions raised by scenario.runtime."""


class UncaughtCharmError(ScenarioRuntimeError):
    """Error raised if the charm raises while handling the event being dispatched."""


class DirtyVirtualCharmRootError(ScenarioRuntimeError):
    """Error raised when the runtime can't initialize the vroot without overwriting existing metadata files."""


class InconsistentScenarioError(ScenarioRuntimeError):
    """Error raised when the combination of state and event is inconsistent."""


class ConsistencyChecker:
    def __init__(
        self,
        state: "State",
        event: "Event",
        charm_spec: "_CharmSpec",
        juju_version: str,
    ):
        self.state = state
        self.event = event
        self.charm_spec = charm_spec
        self.juju_version: Tuple[int, ...] = tuple(map(int, juju_version.split(".")))

    def run(self):
        if os.getenv("SCENARIO_SKIP_CONSISTENCY_CHECKS"):
            logger.info("skipping consistency checks.")
            return

        errors = []

        for check in (
            self._check_containers,
            self._check_config,
            self._check_event,
            self._check_secrets,
        ):
            try:
                results = check()
            except Exception as e:
                logger.error(
                    f"error encountered processing check {check}", exc_info=True
                )
                errors.append(
                    f"an unexpected error occurred processing check {check} ({e}); see the logs"
                )
                continue

            errors.extend(results)

        if errors:
            err_fmt = "\n".join(errors)
            logger.error(
                f"Inconsistent scenario. The following errors were found: {err_fmt}"
            )
            raise InconsistentScenarioError(errors)

    def _check_event(self) -> Iterable[str]:
        from scenario.state import (  # avoid cycles
            is_relation_event,
            is_workload_event,
            normalize_name,
        )

        event = self.event
        errors = []
        if not event.relation and is_relation_event(event.name):
            errors.append(
                "cannot construct a relation event without the relation instance. "
                "Please pass one."
            )
        if is_relation_event(event.name) and not event.name.startswith(
            normalize_name(event.relation.endpoint)
        ):
            errors.append(
                f"relation event should start with relation endpoint name. {event.name} does "
                f"not start with {event.relation.endpoint}."
            )

        if not event.container and is_workload_event(event.name):
            errors.append(
                "cannot construct a workload event without the container instance. "
                "Please pass one."
            )
        if is_workload_event(event.name) and not event.name.startswith(
            normalize_name(event.container.name)
        ):
            errors.append(
                f"workload event should start with container name. {event.name} does "
                f"not start with {event.container.name}."
            )
        return errors

    def _check_config(self) -> Iterable[str]:
        state_config = self.state.config
        meta_config = (self.charm_spec.config or {}).get("options", {})
        errors = []

        for key, value in state_config.items():
            if key not in meta_config:
                errors.append(
                    f"config option {key!r} in state.config but not specified in config.yaml."
                )
                continue

            # todo unify with snapshot's when merged.
            converters = {
                "string": str,
                "int": int,
                "integer": int,  # fixme: which one is it?
                "number": float,
                "boolean": bool,
                "attrs": NotImplemented,  # fixme: wot?
            }

            expected_type_name = meta_config[key].get("type", None)
            if not expected_type_name:
                errors.append(f"config.yaml invalid; option {key!r} has no 'type'.")
                continue

            expected_type = converters.get(expected_type_name)
            if not isinstance(value, expected_type):
                errors.append(
                    f"config invalid; option {key!r} should be of type {expected_type} "
                    f"but is of type {type(value)}."
                )

        return errors

    def _check_secrets(self) -> Iterable[str]:
        from scenario.state import is_secret_event  # avoid cycles

        errors = []
        if is_secret_event(self.event.name) and not self.state.secrets:
            errors.append(
                "the event being processed is a secret event; but the state has no secrets."
            )

        if (
            is_secret_event(self.event.name) or self.state.secrets
        ) and self.juju_version < (3,):
            errors.append(
                f"secrets are not supported in the specified juju version {self.juju_version}. "
                f"Should be at least 3.0."
            )

        return errors

    def _check_containers(self) -> Iterable[str]:
        from scenario.state import is_workload_event  # avoid cycles

        meta_containers = list(self.charm_spec.meta.get("containers", {}))
        state_containers = [c.name for c in self.state.containers]
        errors = []

        # it's fine if you have containers in meta that are not in state.containers (yet), but it's not fine if:
        # - you're processing a pebble-ready event and that container is not in state.containers or meta.containers
        if is_workload_event(self.event.name):
            evt_container_name = self.event.name[: -len("-pebble-ready")]
            if evt_container_name not in meta_containers:
                errors.append(
                    f"the event being processed concerns container {evt_container_name!r}, but a container "
                    f"with that name is not declared in the charm metadata"
                )
            if evt_container_name not in state_containers:
                errors.append(
                    f"the event being processed concerns container {evt_container_name!r}, but a container "
                    f"with that name is not present in the state. It's odd, but consistent, if it cannot "
                    f"connect; but it should at least be there."
                )

        # - a container in state.containers is not in meta.containers
        if diff := (set(state_containers).difference(set(meta_containers))):
            errors.append(
                f"some containers declared in the state are not specified in metadata. That's not possible. "
                f"Missing from metadata: {diff}."
            )
        return errors


class Runtime:
    """Charm runtime wrapper.

    This object bridges a local environment and a charm artifact.
    """

    def __init__(
        self,
        charm_spec: "_CharmSpec",
        charm_root: Optional["PathLike"] = None,
        juju_version: str = "3.0.0",
    ):
        self._charm_spec = charm_spec
        self._juju_version = juju_version
        self._charm_root = charm_root
        # TODO consider cleaning up venv on __delete__, but ideally you should be
        #  running this in a clean venv or a container anyway.

    @staticmethod
    def from_local_file(
        local_charm_src: Path,
        charm_cls_name: str,
    ) -> "Runtime":
        sys.path.extend((str(local_charm_src / "src"), str(local_charm_src / "lib")))

        ldict = {}

        try:
            exec(
                f"from charm import {charm_cls_name} as my_charm_type", globals(), ldict
            )
        except ModuleNotFoundError as e:
            raise RuntimeError(
                f"Failed to load charm {charm_cls_name}. "
                f"Probably some dependency is missing. "
                f"Try `pip install -r {local_charm_src / 'requirements.txt'}`"
            ) from e

        my_charm_type: Type["CharmBase"] = ldict["my_charm_type"]
        return Runtime(_CharmSpec(my_charm_type))  # TODO add meta, options,...

    @staticmethod
    def _cleanup_env(env):
        # cleanup env, in case we'll be firing multiple events, we don't want to accumulate.
        for key in env:
            os.unsetenv(key)

    @property
    def unit_name(self):
        meta = self._charm_spec.meta
        if not meta:
            return "local/0"
        return meta["name"] + "/0"  # todo allow override

    def _get_event_env(self, state: "State", event: "Event", charm_root: Path):
        if event.name.endswith("_action"):
            # todo: do we need some special metadata, or can we assume action names are always dashes?
            action_name = event.name[: -len("_action")].replace("_", "-")
        else:
            action_name = ""

        env = {
            "JUJU_VERSION": self._juju_version,
            "JUJU_UNIT_NAME": self.unit_name,
            "_": "./dispatch",
            "JUJU_DISPATCH_PATH": f"hooks/{event.name}",
            "JUJU_MODEL_NAME": state.model.name,
            "JUJU_ACTION_NAME": action_name,
            "JUJU_MODEL_UUID": state.model.uuid,
            "JUJU_CHARM_DIR": str(charm_root.absolute())
            # todo consider setting pwd, (python)path
        }

        if relation := event.relation:
            env.update(
                {
                    "JUJU_RELATION": relation.endpoint,
                    "JUJU_RELATION_ID": str(relation.relation_id),
                }
            )

        if container := event.container:
            env.update({"JUJU_WORKLOAD_NAME": container.name})

        if secret := event.secret:
            env.update(
                {
                    "JUJU_SECRET_ID": secret.id,
                    "JUJU_SECRET_LABEL": secret.label or "",
                }
            )

        return env

    @staticmethod
    def _wrap(charm_type: "_CT") -> "_CT":
        # dark sorcery to work around framework using class attrs to hold on to event sources
        # todo this should only be needed if we call play multiple times on the same runtime.
        #  can we avoid it?
        class WrappedEvents(charm_type.on.__class__):
            pass

        WrappedEvents.__name__ = charm_type.on.__class__.__name__

        class WrappedCharm(charm_type):  # type: ignore
            on = WrappedEvents()

        WrappedCharm.__name__ = charm_type.__name__
        return WrappedCharm

    @contextmanager
    def virtual_charm_root(self):
        # If we are using runtime on a real charm, we can make some assumptions about the directory structure
        #  we are going to find.
        #  If we're, say, dynamically defining charm types and doing tests on them, we'll have to generate
        #  the metadata files ourselves. To be sure, we ALWAYS use a tempdir. Ground truth is what the user
        #  passed via the CharmSpec
        spec = self._charm_spec

        if vroot := self._charm_root:
            vroot_is_custom = True
            virtual_charm_root = Path(vroot)
        else:
            vroot = tempfile.TemporaryDirectory()
            virtual_charm_root = Path(vroot.name)
            vroot_is_custom = False

        metadata_yaml = virtual_charm_root / "metadata.yaml"
        config_yaml = virtual_charm_root / "config.yaml"
        actions_yaml = virtual_charm_root / "actions.yaml"

        metadata_files_present = any(
            (file.exists() for file in (metadata_yaml, config_yaml, actions_yaml))
        )

        if spec.is_autoloaded and vroot_is_custom:
            # since the spec is autoloaded, in theory the metadata contents won't differ, so we can
            # overwrite away even if the custom vroot is the real charm root (the local repo).
            # Still, log it for clarity.
            if metadata_files_present:
                logger.info(
                    f"metadata files found in custom vroot {vroot}. "
                    f"The spec was autoloaded so the contents should be identical. "
                    f"Proceeding..."
                )

        elif not spec.is_autoloaded and metadata_files_present:
            logger.error(
                f"Some metadata files found in custom user-provided vroot {vroot} "
                f"while you have passed meta, config or actions to trigger(). "
                "We don't want to risk overwriting them mindlessly, so we abort. "
                "You should not include any metadata files in the charm_root. "
                "Single source of truth are the arguments passed to trigger(). "
            )
            raise DirtyVirtualCharmRootError(vroot)

        metadata_yaml.write_text(yaml.safe_dump(spec.meta))
        config_yaml.write_text(yaml.safe_dump(spec.config or {}))
        actions_yaml.write_text(yaml.safe_dump(spec.actions or {}))

        yield virtual_charm_root

        if not vroot_is_custom:
            vroot.cleanup()

    @staticmethod
    def _get_store(temporary_charm_root: Path):
        charm_state_path = temporary_charm_root / ".unit-state.db"
        store = SQLiteStorage(charm_state_path)
        return store

    def _initialize_storage(self, state: "State", temporary_charm_root: Path):
        """Before we start processing this event, expose the relevant parts of State through the storage."""
        store = self._get_store(temporary_charm_root)

        for event in state.deferred:
            store.save_notice(event.handle_path, event.owner, event.observer)
            try:
                marshal.dumps(event.snapshot_data)
            except ValueError as e:
                raise ValueError(
                    f"unable to save the data for {event}, it must contain only simple types."
                ) from e
            store.save_snapshot(event.handle_path, event.snapshot_data)

        for stored_state in state.stored_state:
            store.save_snapshot(stored_state.handle_path, stored_state.content)

        store.close()

    def _close_storage(self, state: "State", temporary_charm_root: Path):
        """Now that we're done processing this event, read the charm state and expose it via State."""
        from scenario.state import DeferredEvent, StoredState  # avoid cyclic import

        store = self._get_store(temporary_charm_root)

        deferred = []
        stored_state = []
        event_regex = re.compile(_event_regex)
        sst_regex = re.compile(_stored_state_regex)
        for handle_path in store.list_snapshots():
            if event_regex.match(handle_path):
                notices = store.notices(handle_path)
                for handle, owner, observer in notices:
                    event = DeferredEvent(
                        handle_path=handle, owner=owner, observer=observer
                    )
                    deferred.append(event)

            else:
                # it's a StoredState. TODO: No other option, right?
                stored_state_snapshot = store.load_snapshot(handle_path)
                match = sst_regex.match(handle_path)
                if not match:
                    logger.warning(
                        f"could not parse handle path {handle_path!r} as stored state"
                    )
                    continue

                kwargs = match.groupdict()
                sst = StoredState(content=stored_state_snapshot, **kwargs)
                stored_state.append(sst)

        store.close()
        return state.replace(deferred=deferred, stored_state=stored_state)

    def exec(
        self,
        state: "State",
        event: "Event",
        pre_event: Optional[Callable[["CharmType"], None]] = None,
        post_event: Optional[Callable[["CharmType"], None]] = None,
    ) -> "State":
        """Runs an event with this state as initial state on a charm.

        Returns the 'output state', that is, the state as mutated by the charm during the event handling.

        This will set the environment up and call ops.main.main().
        After that it's up to ops.
        """
        ConsistencyChecker(state, event, self._charm_spec, self._juju_version).run()

        charm_type = self._charm_spec.charm_type
        logger.info(f"Preparing to fire {event.name} on {charm_type.__name__}")

        # we make a copy to avoid mutating the input state
        output_state = state.copy()

        logger.info(" - generating virtual charm root")
        with self.virtual_charm_root() as temporary_charm_root:
            # todo consider forking out a real subprocess and do the mocking by
            #  generating hook tool executables

            logger.info(" - initializing storage")
            self._initialize_storage(state, temporary_charm_root)

            logger.info(" - preparing env")
            env = self._get_event_env(
                state=state, event=event, charm_root=temporary_charm_root
            )
            os.environ.update(env)

            logger.info(" - Entering ops.main (mocked).")
            # we don't import from ops.main because we need some extras, such as the pre/post_event hooks
            from scenario.ops_main_mock import main as mocked_main

            try:
                mocked_main(
                    pre_event=pre_event,
                    post_event=post_event,
                    state=output_state,
                    event=event,
                    charm_spec=self._charm_spec.replace(
                        charm_type=self._wrap(charm_type)
                    ),
                )
            except NoObserverError:
                raise  # propagate along
            except Exception as e:
                raise UncaughtCharmError(
                    f"Uncaught error in operator/charm code: {e}."
                ) from e
            finally:
                logger.info(" - Exited ops.main.")

            logger.info(" - clearing env")
            self._cleanup_env(env)

            logger.info(" - closing storage")
            output_state = self._close_storage(output_state, temporary_charm_root)

        logger.info("event dispatched. done.")
        return output_state


def trigger(
    state: "State",
    event: Union["Event", str],
    charm_type: Type["CharmType"],
    pre_event: Optional[Callable[["CharmType"], None]] = None,
    post_event: Optional[Callable[["CharmType"], None]] = None,
    # if not provided, will be autoloaded from charm_type.
    meta: Optional[Dict[str, Any]] = None,
    actions: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    charm_root: Optional[Dict["PathLike", "PathLike"]] = None,
    juju_version: str = "3.0",
) -> "State":
    """Trigger a charm execution with an Event and a State.

    Calling this function will call ops' main() and set up the context according to the specified
    State, then emit the event on the charm.

    :arg event: the Event that the charm will respond to. Can be a string or an Event instance.
    :arg state: the State instance to use as data source for the hook tool calls that the charm will
        invoke when handling the Event.
    :arg charm_type: the CharmBase subclass to call ``ops.main()`` on.
    :arg pre_event: callback to be invoked right before emitting the event on the newly
        instantiated charm. Will receive the charm instance as only positional argument.
    :arg post_event: callback to be invoked right after emitting the event on the charm instance.
        Will receive the charm instance as only positional argument.
    :arg meta: charm metadata to use. Needs to be a valid metadata.yaml format (as a python dict).
        If none is provided, we will search for a ``metadata.yaml`` file in the charm root.
    :arg actions: charm actions to use. Needs to be a valid actions.yaml format (as a python dict).
        If none is provided, we will search for a ``actions.yaml`` file in the charm root.
    :arg config: charm config to use. Needs to be a valid config.yaml format (as a python dict).
        If none is provided, we will search for a ``config.yaml`` file in the charm root.
    :arg juju_version: Juju agent version to simulate.
    :arg charm_root: virtual charm root the charm will be executed with.
     If the charm, say, expects a `./src/foo/bar.yaml` file present relative to the
        execution cwd, you need to use this.
        >>> virtual_root = tempfile.TemporaryDirectory()
        >>> local_path = Path(local_path.name)
        >>> (local_path / 'foo').mkdir()
        >>> (local_path / 'foo' / 'bar.yaml').write_text('foo: bar')
        >>> scenario.State().trigger(..., charm_root = virtual_root)
    """
    from scenario.state import Event, _CharmSpec

    if isinstance(event, str):
        event = Event(event)

    if not any((meta, actions, config)):
        logger.debug("Autoloading charmspec...")
        spec = _CharmSpec.autoload(charm_type)
    else:
        if not meta:
            meta = {"name": str(charm_type.__name__)}
        spec = _CharmSpec(
            charm_type=charm_type, meta=meta, actions=actions, config=config
        )

    runtime = Runtime(
        charm_spec=spec,
        juju_version=juju_version,
        charm_root=charm_root,
    )

    return runtime.exec(
        state=state,
        event=event,
        pre_event=pre_event,
        post_event=post_event,
    )
