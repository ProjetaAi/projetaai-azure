"""Script config reading utils."""
from __future__ import annotations
from abc import abstractproperty
from dataclasses import dataclass, field
from functools import wraps
from itertools import chain
from pathlib import Path
import re
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Protocol,
    Tuple,
    Type,
    TypedDict,
    Union,
    get_args,
    get_origin,
    runtime_checkable,
)
from azure.cli.core._profile import Profile
from typing_extensions import Literal, NotRequired

import click
from projetaai_azure.converters.step import ConverterStep
from azureml.core import Workspace

from kedro_projetaai.utils.script import Step
from projetaai_azure.utils.constants import CLICK_TYPEMAP
from projetaai_azure.utils.other import (
    identity,
    truthify,
)
from projetaai_azure.utils.io import (
    readcfg,
    readtoml,
    readyml,
)
from projetaai_azure.utils.string import (
    get_filepath_extension,
)


class _BaseSpecification(TypedDict):
    target: str
    """Dict key to receive the extracted value"""
    path: str
    """Dict keys path to get the value"""
    preparator: Callable[[Any, Dict[str, Any]], Any]
    """Function to postprocess the value"""
    validator: Callable[[[Any, Dict[str, Any]]], Union[bool, Tuple[bool, str]]]
    """Function to validate the value"""
    required: bool
    """Throws an error if True or not present, otherwise, sets it to None"""
    type: NotRequired[Union[Type[bool], Type[int], Type[str], Type[List[Any]]]]
    """Type of the value. Used for argv overwrite. If not specified, str"""


class _DefaultSpecification(TypedDict):
    default: Callable[[Dict[str, Any]], Any]
    """Default value if the path key is not found"""
    help: str
    """Help message for click"""


class _MetaSpecification(_BaseSpecification):
    file: str
    """Path to the file to extract the value from"""


class _FileSpecificSpecification(_MetaSpecification, _DefaultSpecification):
    pass


class _ArgvSpecification(_MetaSpecification, _DefaultSpecification):
    type: Union[Type[bool], Type[int], Type[str], Type[List[Any]]]
    """Type of the value. If bool, the flag is True if present, otherwise
    False"""


# Ignore it, class used for linting only
@runtime_checkable
class _GenericAlias(Protocol):
    """Type for typing aliases, because linter cannot import it."""

    __args__: Tuple[Union[type, _GenericAlias]]
    _name: str


@dataclass
class BaseSettingsReader(Step):
    """
    Gets data from multiple inputs.

    Execution order:
    MetadataReader -> FileSpecificReader -> ArgvReader

    Note:
        Remember to overwrite METADATA, or FILE_SPECIFIC, or ARGV in the
        execution order and to declare its items in dependency order if
        default, validator, or preparator contains intra-dependencies.
    """

    argv: dict = field(default_factory=dict)
    settings: dict = field(init=False, default_factory=dict)

    @abstractproperty
    def metadata_requirements(self) -> List[_MetaSpecification]:
        """Returns the list of requirements that are read from meta files."""
        pass

    @abstractproperty
    def file_specific_requirements(self) -> List[_FileSpecificSpecification]:
        """Returns the list of requirements that can be read from a file."""
        pass

    @abstractproperty
    def argv_requirements(self) -> List[_ArgvSpecification]:
        """Returns the list of requirements that can be read from argv."""
        pass

    @property
    def full_requirements(self) -> List[_ArgvSpecification]:
        """Returns the list of requirements that can be read from argv.

        Returns:
            List[_ArgvSpecification]: List of requirements
        """
        metadata = [
            {
                "target": spec["target"],
                "type": spec.get("type", str),
                "required": False,
            }
            for spec in self.metadata_requirements
        ]
        other = [
            {
                "target": spec["target"],
                "type": spec.get("type", str),
                "required": False,
            }
            for spec in self.file_specific_requirements
        ]
        return metadata + other + self.argv_requirements

    def _read_file(self, filepath: str) -> dict:
        extension = get_filepath_extension(filepath)
        if extension in [".yml", ".yaml"]:
            return readyml(filepath)
        elif extension == ".toml":
            return readtoml(filepath)
        elif extension == ".cfg":
            return readcfg(filepath)

    def _find_in_dict(self, path: str, dictionary: dict) -> Union[Any, None]:
        dictionary = dictionary or {}
        for part in path.split("."):
            dictionary = dictionary.get(part, {})
        return dictionary or None

    def _apply_default(self, spec: _DefaultSpecification, value: Any) -> Any:
        if value is None:
            default = spec.get("default")
            if default:
                return default(self.settings)
            else:
                return None
        else:
            return value

    def _prepare(self, spec: _BaseSpecification, value: Any) -> Any:
        try:
            return spec.get("preparator", identity)(value, self.settings)
        except Exception:
            return value

    def _validate(self, spec: _BaseSpecification, value: Any) -> bool:
        output = spec.get("validator", truthify)(value, self.settings)
        if isinstance(output, tuple):
            status = output[0]
            message = output[1]
        else:
            status = output
            message = f'Invalid value "{value}" in "{spec["target"]}"'

        if not status:
            raise ValueError(message)
        return status

    def _set_target(self, spec: _BaseSpecification, value: Any):
        if value is not None:
            self.settings[spec["target"]] = value
        elif not spec.get("required", True):
            if spec["target"] not in self.settings:
                self.settings[spec["target"]] = value

    def _read_meta_spec(self, meta: _MetaSpecification) -> Any:
        """Reads a metadata specification and the value."""
        dictionary = self._read_file(meta["file"])
        value = self._find_in_dict(meta["path"], dictionary)
        value = self._prepare(meta, value)
        self._validate(meta, value)
        if value is None:
            raise KeyError(f'please specify "{meta["path"]}" in ' f'"{meta["file"]}"')
        return value

    def read_meta(self):
        """Reads metadata from meta files and updates 'settings'.

        Reads metadata files like 'setup.cfg' and 'pyproject.toml'.
        """
        for meta in self.metadata_requirements:
            self._set_target(meta, self._read_meta_spec(meta))

    def _read_file_specific_spec(self, spec: _FileSpecificSpecification) -> Any:
        dictionary = self._read_file(spec["file"])
        value = self._find_in_dict(spec["path"], dictionary)
        value = self._prepare(spec, self._apply_default(spec, value))
        self._validate(spec, value)
        return value

    def read_file_specific(self):
        """Reads settings read from files and updates 'settings'.

        Reads credentials files like 'credentials.py'.
        """
        for spec in self.file_specific_requirements:
            self._set_target(spec, self._read_file_specific_spec(spec))

    def _read_argv_spec(self, argv: _ArgvSpecification) -> Any:
        value = self.argv.get(argv["target"])
        value = self._prepare(argv, self._apply_default(argv, value))
        self._validate(argv, value)
        return value

    def read_argv(self):
        """Reads settings from 'argv' and updates 'settings'."""
        for spec in self.full_requirements:
            self._set_target(spec, self._read_argv_spec(spec))

    def _validate_not_filled(self):
        """Checks if all settings were filled."""
        for spec in chain(
            self.metadata_requirements,
            self.file_specific_requirements,
            self.argv_requirements,
        ):
            if spec["target"] not in self.settings or (
                self.settings[spec["target"]] is None and spec.get("required", True)
            ):
                raise KeyError(
                    f'"{spec["target"]}" must be defined in '
                    + (
                        f'"{spec["file"]}" under "{spec["path"]}" or in '
                        if spec.get("file")
                        else ""
                    )
                    + "argv"
                )

    def run(self) -> dict:
        """Runs the configuration reader.

        Returns:
            dict: Dictionary of settings
        """
        self.read_meta()
        self.read_file_specific()
        self.read_argv()
        self._validate_not_filled()
        return self.settings

    def click_command(
        self, fn: Callable[[dict], None], **click_kwargs: Any
    ) -> click.Command:
        """Returns a click command that runs the configuration reader.

        Args:
            fn (Callable[[dict], None]): Function to run with the settings
            **click_kwargs: Keyword arguments for the click command

        Returns:
            click.Command: Click command
        """

        @wraps(fn)
        def cmd(**kwargs: Any) -> None:
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            self.argv = kwargs
            kwargs = self()
            return fn(**kwargs)

        def spec_parser(spec: dict) -> dict:
            if spec.get("type") in CLICK_TYPEMAP:
                return {
                    "type": CLICK_TYPEMAP[spec["type"]],
                    "help": spec.get("help", None),
                }
            elif spec.get("type") and get_origin(spec.get("type")) is list:
                return {
                    "type": CLICK_TYPEMAP[get_args(spec.get("type"))[0]],
                    "multiple": True,
                    "help": spec.get("help", None),
                }
            return {}

        for spec in self.file_specific_requirements:
            cmd = click.option(
                f'--{spec["target"]}', required=False, **spec_parser(spec)
            )(cmd)
        for spec in self.argv_requirements:
            cmd = click.option(
                f'--{spec["target"]}',
                required=("default" not in spec),
                **spec_parser(spec),
            )(cmd)

        return click.command(cmd, **click_kwargs)


@dataclass
class BasicAzureMLSettingsReader(BaseSettingsReader):
    """Contains the basic settings for AzureML operations.

    Outputs:
        compute (str): Name of the compute target
        workspace (str): Workspace id
        resource_group (str): Resource group id
        project (str): Name of the project
        python (str): 'major.minor.micro' version of python
        description (str): Description of the project
    """

    CREDENTIALS_FILENAME: ClassVar[str] = str(
        Path("conf") / "local" / "credentials.yml"
    )
    CREDENTIALS_SECTION: ClassVar[str] = "azure.deploy"
    PYPROJECT_FILENAME: ClassVar[str] = "pyproject.toml"
    SETUP_FILENAME: ClassVar[str] = "setup.cfg"

    @property
    def metadata_requirements(self) -> List[_MetaSpecification]:
        """Returns the basic metadata requirements.

        Returns:
            List[_MetaSpecification]: List of metadata requirements
        """
        return [
            {
                "target": "project",
                "file": self.PYPROJECT_FILENAME,
                "path": "tool.kedro.package_name",
            },
            {
                "target": "python",
                "file": self.SETUP_FILENAME,
                "path": "options.python_requires",
                "preparator": lambda x, _: re.search(r"3\.\d+(\.\d+)?", x)[0],
            },
            {
                "target": "description",
                "file": self.SETUP_FILENAME,
                "path": "metadata.description",
            },
        ]

    @property
    def file_specific_requirements(self) -> List[_FileSpecificSpecification]:
        """Returns the basic file specific requirements.

        Returns:
            List[_FileSpecificSpecification]:
                List of file specific requirements
        """
        return [
            {
                "target": target,
                "file": self.CREDENTIALS_FILENAME,
                "path": f"{self.CREDENTIALS_SECTION}.{target}",
            }
            for target in ["compute", "resource_group", "workspace"]
        ]


class _AuthManagedTenant(TypedDict):
    tenantId: str
    """Tenant id of an manager"""


class _AuthUser(TypedDict):
    name: str
    """Name of the user"""
    type: Union[Literal["user"], str]
    """Account type"""


class _AuthSubscription(TypedDict):
    cloudName: Union[Literal["AzureCloud"], str]
    """Service provider name"""
    homeTenantId: str
    """User group tenant"""
    id: str
    """User id"""
    isDefault: bool
    """Whether the subscription is the default subscription"""
    managedByTenants: List[_AuthManagedTenant]
    """List of tenant ids that manage the subscription"""
    name: str
    """Username"""
    state: Literal["Enabled", "Disabled"]
    """Whether the subscription is active"""
    tenantId: str
    """User tenant id"""
    user: _AuthUser
    """User account details"""


@dataclass
class Authenticator(ConverterStep):
    """Authenticates the user and returns its credentials.

    Requires:
        resource_group (str): Resource group id
        workspace (str): Workspace id

    Outputs:
        subscription_id (str): User subscription id
        workspace_instance (Workspace): Logged workspace instance
    """

    resource_group: str
    workspace: str

    def read_cached_subscriptions(self) -> List[_AuthSubscription]:
        """Obtains subscriptions cached from azure CLI.

        Returns:
            List[_AuthSubscription]: List of subscriptions
        """
        return Profile().load_cached_subscriptions()

    def is_valid_subscription(self, subscription: _AuthSubscription) -> bool:
        """Checks if the subscription is valid.

        Args:
            subscription (_AuthSubscription): Subscription to check

        Returns:
            bool: Whether the subscription is valid
        """
        try:
            Workspace(subscription["id"], self.resource_group, self.workspace)
            return True
        except Exception:
            return False

    def _read_azlogin_subscriptions(self) -> List[_AuthSubscription]:
        """Obtains subscriptions from `az login` command.

        Returns:
            List[_AuthSubscription]: List of subscriptions
        """
        self.log(
            "error",
            "None of the logged subscriptions are valid, run "
            "`az login` or `az login --use-device-code` with a valid "
            "account and try again",
        )
        raise RuntimeError("No valid subscriptions")

    def _get_valid_subscription(
        self, subscriptions: List[_AuthSubscription]
    ) -> _AuthSubscription:
        for subscription in subscriptions:
            if self.is_valid_subscription(subscription):
                return subscription

    def get_subscription(self) -> _AuthSubscription:
        """Obtains a valid subscription from caches or `az login` command.

        Raises:
            ValueError: If no valid subscription is found

        Returns:
            _AuthSubscription: Valid subscription
        """
        subscription = self._get_valid_subscription(
            self.read_cached_subscriptions()
        ) or self._get_valid_subscription(self._read_azlogin_subscriptions())
        if subscription:
            return subscription
        else:
            raise ValueError("No valid subscription found")

    def _instance_workspace(self, subscription: _AuthSubscription) -> Workspace:
        return Workspace(
            subscription_id=subscription["id"],
            resource_group=self.resource_group,
            workspace_name=self.workspace,
        )

    def run(self) -> dict:
        """Runs the authenticator.

        Returns:
            dict: Subscription id and workspace instance
        """
        subscription = self.get_subscription()
        return {
            "subscription_id": subscription["id"],
            "workspace_instance": self._instance_workspace(subscription),
        }
