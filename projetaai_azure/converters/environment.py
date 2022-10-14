"""AzureML environment conversion steps."""
from dataclasses import dataclass
import os
from pathlib import Path
import shutil
from typing_extensions import Literal, NotRequired
from requirements.requirement import Requirement
from typing import ClassVar, Dict, List, TypedDict, Union
from azureml.core.environment import Environment
from azureml.core import Workspace

import requirements

from projetaai_azure.converters.step import ConverterStep
from projetaai_azure.runners.databricks import is_databricks_project
from projetaai_azure.utils.io import writejson, writelines, writeyml


class _EnvCondaDependencies(TypedDict):
    """Dict representing a conda file yml structure."""

    name: str
    """Environment name"""
    channels: Union[List[str], Literal["conda-forge"]]
    """Channels to get dependencies from"""
    dependencies: List[Union[str, Dict[str, List[str]]]]
    """List of dependencies to install. Can be a requirement representation
    or a dict with the package manager as key and a list of requirements as
    value"""


class _EnvAzureMLEnvironment(TypedDict):
    """Dict representing the Azure ML Environment Json structure."""

    name: str
    """Environment name"""
    version: NotRequired[str]
    """Environment version"""
    python: dict
    """Python version"""


@dataclass
class EnvironmentCreator(ConverterStep):
    """Converts the requirements.txt in an AzureML environment.

    Requires:
        project (str): Project name
        python (str): Python version

    Outputs:
        environment (str): AzureML environment name
    """

    ENVIRONMENT_FOLDER: ClassVar[str] = "environment"
    REQUIREMENTS_FILENAME: ClassVar[str] = "requirements.txt"
    CONDAFILE_FILENAME: ClassVar[str] = "conda_dependencies.yml"
    DOCKERFILE_FILENAME: ClassVar[str] = "BaseDockerfile"
    AZUREML_ENVIRONMENT_FILENAME: ClassVar[str] = "azureml_environment.json"

    project: str
    python: str
    experiment: str
    workspace_instance: Workspace

    def __post_init__(self):
        """Initializes the environment creator."""
        os.mkdir(self.ENVIRONMENT_FOLDER)

    @property
    def _requirements_filepath(self) -> str:
        """Project requirements filepath.

        Returns:
            str
        """
        return str(Path(".") / self.SOURCE_FOLDER / "src" / self.REQUIREMENTS_FILENAME)

    @property
    def _requirements_dict(self) -> Dict[str, Requirement]:
        """Returns a dictionary from project `requirements.txt`.

        The dictionary is indexed by requirement name and its
        (without version) and the full requirement object as value.

        Returns:
            Dict[str, Requirement]
        """
        with open(self._requirements_filepath, "r") as f:
            reqs = list(requirements.parse(f))

        return {req.name: req for req in reqs}

    @property
    def requirements_lines(self) -> List[str]:
        """Parses requirements.txt and returns a list of requirement line.

        Returns:
            List[str]:

        Example:
            >>> requirements_txt = '''
            ... # mycomment
            ... lib1
            ... lib2>=1.0
            ... lib3==2.0
            ... lib4<=0.4.2
            ... '''
            >>> output = ['lib1', 'lib2>=1.0', 'lib3==2.0', 'lib4<=0.4.2']
        """
        return [req.line for req in self._requirements_dict.values()]

    @property
    def conda_channel(self) -> str:
        """Returns a condafile channel.

        Returns:
            str
        """
        return "conda-forge"

    @property
    def base_environment_name(self) -> str:
        """Returns the non-experiment environment name.

        Returns:
            str: Environment name
        """
        return self.project

    @property
    def environment_name(self) -> str:
        """Returns the environment name.

        Returns:
            str: Environment name
        """
        return self.experiment

    def _build_condafile(self) -> _EnvCondaDependencies:
        return {
            "name": self.environment_name,
            "channels": [self.conda_channel],
            "dependencies": [
                f"python={self.python}",
                "pip",
                {"pip": self.requirements_lines},
            ],
        }

    @property
    def docker_image(self) -> str:
        """Returns the docker image url.

        Returns:
            str: Docker image url
        """
        return "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:20220516.v1"

    @property
    def docker_jdk(self) -> str:
        """Returns JAVA jdk version to be installed.

        Returns:
            str: java jdk version to be installed using apt-get
        """
        return "openjdk-8-jre"

    @property
    def docker_databricks_connect(self) -> str:
        """Returns databricks-connect version to be installed.

        Returns:
            str: databricks-connect version compatible with your databricks
                cluster.
        """
        return "databricks-connect==9.1.21"

    @property
    def docker_azure_cli_extension(self) -> str:
        """Return azure cli and extension needed.

        Returns:
            str: azure-cli and azure extension needed
        """
        return "azure-cli-ml"

    @property
    def docker_java_home(self) -> str:
        """Return JAVA_HOME path.

        Returns:
            str: JAVA_HOME path
        """
        return "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"

    def _has_spark_conf_file(self) -> bool:
        return is_databricks_project(Path(self.SOURCE_FOLDER))

    def _build_dockerfile(self) -> List[str]:
        docker_file = [
            f"FROM {self.docker_image}",
            "RUN pip install azure-cli",
            f"RUN az extension add --name {self.docker_azure_cli_extension}",
        ]

        if self._has_spark_conf_file():
            docker_file = docker_file + [
                "USER root:root",
                "RUN mkdir -p /usr/share/man/man1",
                f"RUN apt-get update && apt-get install -y {self.docker_jdk}",
                f"RUN pip install {self.docker_databricks_connect}",
                f"RUN export JAVA_HOME={self.docker_java_home}",
            ]

        return docker_file

    def _build_azureml_environment(self) -> _EnvAzureMLEnvironment:
        return {"name": self.environment_name, "python": {}}

    def _save_condafile(self):
        writeyml(
            str(Path(self.ENVIRONMENT_FOLDER) / self.CONDAFILE_FILENAME),
            self._build_condafile(),
        )

    def _save_dockerfile(self):
        writelines(
            str(Path(self.ENVIRONMENT_FOLDER) / self.DOCKERFILE_FILENAME),
            self._build_dockerfile(),
        )

    def _save_azureml_environment(self):
        writejson(
            str(Path(self.ENVIRONMENT_FOLDER) / self.AZUREML_ENVIRONMENT_FILENAME),
            self._build_azureml_environment(),
        )

    def save(self):
        """Saves all environment files."""
        self._save_condafile()
        self._save_dockerfile()
        self._save_azureml_environment()

    def submit(self):
        """Pushes the environment to Azure ML."""
        self.azml("environment", "register", "--directory", self.ENVIRONMENT_FOLDER)

    def clean(self):
        """Removes environment files."""
        shutil.rmtree(self.ENVIRONMENT_FOLDER)

    def _fetch_environment(self, name: str) -> Union[Environment, None]:
        try:
            return Environment.get(self.workspace_instance, name)
        except Exception:
            return None

    def _is_requirements_equal(self, environment: Environment) -> bool:
        current_requirements = {req for req in self.requirements_lines}
        environment_requirements = {
            req for req in environment.python.conda_dependencies.pip_packages
        }
        return current_requirements == environment_requirements

    def _find_environment(self, name: str) -> Union[Environment, None]:
        environment = self._fetch_environment(name)
        if environment is not None:
            if self._is_requirements_equal(environment):
                return environment.name

    def find_environment(self) -> Union[str, None]:
        """Finds an existing environment with the same requirements.

        Returns:
            Union[str, None]: Environment name or None if not found
        """
        return self._find_environment(
            self.base_environment_name
        ) or self._find_environment(self.environment_name)

    def run(self) -> dict:
        """Runs the environment creator.

        Returns:
            dict: Environment name
        """
        existing_environment = self.find_environment()
        if existing_environment:
            self.log(
                "info",
                f'environment "{existing_environment}" with the '
                "same requirements already exists, using it",
            )
            return {"environment": existing_environment}
        else:
            self.log(
                "info",
                "no environments with the same requirements "
                f'found, creating/updating "{self.environment_name}"',
            )
            self.save()
            self.submit()
            self.clean()
            return {"environment": self.environment_name}
