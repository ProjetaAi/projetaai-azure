"""
Prepare and submits a Kedro project to AzureML.

Note:
    This script was built on top of Azure ML CLI v1.
    Check the reference for it out in this link:
    https://docs.microsoft.com/en-us/cli/azure/ml(v1)?view=azure-cli-latest
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass, field
from importlib import import_module
from os import getcwd
import os
from pathlib import Path, PurePosixPath
import re
from typing import (
    ClassVar,
    Dict,
    List,
    Literal,
    Set,
    TypedDict,
    Union,
)
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
import sys
import shutil

from projetaai_azure.converters.step import ConverterStep
from projetaai_azure.utils.io import (
    writejson,
    writestr,
    writeyml,
)
from azureml.pipeline.core import PipelineDraft
from azureml.core import Workspace

sys.path.append(str(Path(getcwd()) / "src"))

WeekDays = Literal[
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
]


@dataclass
class FolderPreparator(ConverterStep):
    """Prepare the project folder for the next steps.

    Creates the following structure:

    ```
    <CONVERTER_FOLDER>/
    └── <SOURCE_FOLDER>/
        ├── conf/
        │   └── base
        ├── src/
        ├── data/
        ├── logs/
        └── pyproject.tom
    ```

    And changes cwd to <CONVERTER_FOLDER>
    """

    def run(self):
        """Run the step."""
        if os.path.exists(self.CONVERTER_FOLDER):
            shutil.rmtree(self.CONVERTER_FOLDER)

        os.mkdir(self.CONVERTER_FOLDER)
        os.mkdir(str(Path(self.CONVERTER_FOLDER) / self.SOURCE_FOLDER))

        folder = Path(self.CONVERTER_FOLDER) / self.SOURCE_FOLDER
        for dir in ["src", "conf"]:
            shutil.copytree(dir, str(folder / dir))
        for file in ["pyproject.toml"]:
            shutil.copy(file, str(folder / file))

        os.chdir(str(Path(os.getcwd()) / self.CONVERTER_FOLDER))
        os.mkdir(str(Path(self.SOURCE_FOLDER) / "logs"))

        for dir in [str(PurePosixPath(self.SOURCE_FOLDER) / "conf" / "local")]:
            if os.path.exists(dir):
                shutil.rmtree(dir)
                os.mkdir(dir)


class _PipeInput(TypedDict):
    source: str
    """Origin of the data"""


class _PipeOutput(TypedDict):
    destination: str
    """Output of the step"""


class _PipeStep(TypedDict):
    name: str
    """Name of the step"""
    runconfig: str
    """Run configuration file"""
    script_name: str
    """Name of the script to run"""
    type: Union[str, Literal["PythonScriptStep"]]
    """AzureML script type"""
    source_directory: str
    """Directory containing the pipeline source code"""
    allow_reuse: bool
    """Whether to allow not reprocessing the step"""
    arguments: List[str]
    """Arguments to pass to the script"""
    inputs: Dict[str, _PipeInput]
    """Inputs to the step"""
    outputs: Dict[str, _PipeOutput]
    """Outputs of the step"""


class _PipeSpecification(TypedDict):
    name: str
    """Name of the pipeline"""
    description: str
    """Description of the pipeline"""
    default_compute: str
    """Compute target used when step doesn't specify one"""
    steps: Dict[str, _PipeStep]
    """Steps of the pipeline"""


class _PipePipeline(TypedDict):
    pipeline: _PipeSpecification
    """Pipeline specification"""


class _PipeEnv(TypedDict):
    name: str
    """Name of the AzureML environment"""


class _PipeRunConfig(TypedDict):
    environment: str
    """Name of the AzureML environment"""


_PipeCLIDraft = TypedDict(
    "PipeCLIDraft",
    {
        "Id": str,  # Unique identifier of the pipeline
        "Last Submitted Pipeline Run Id": Union[str, Literal["null"]],
        # Unique identifier of the last submitted pipeline run
        "Name": str,  # Name of the pipeline
        "Properties": dict,  # Properties of the pipeline
        "Tags": Dict[str, bool],  # Tags of the pipeline
    },
)


@dataclass
class PipelineConverter(ConverterStep):
    """Converts a Kedro pipeline to an Azure ML pipeline.

    Requires:
        project (str): Name of the project
        azure_pipeline (str): AzureML pipeline name
        python (str): Python version to use
        compute (str): AzureML compute target
        pipeline (str): Kedro pipeline name
        experiment (str): AzureML experiment name
        environment (str): AzureML environment name

    Outputs:
        pipeline_id (str): Unique identifier of the AzureML draft pipeline
    """

    PIPELINE_FOLDER: ClassVar[str] = "pipeline"
    PIPELINE_FILENAME: ClassVar[str] = "pipeline.yml"
    ENV_FILENAME: ClassVar[str] = "env.json"
    RUNCONFIG_FILENAME: ClassVar[str] = "runconfig.yml"
    RUN_FILENAME: ClassVar[str] = "run.py"
    RUN_FILEPATH: ClassVar[str] = str(Path(PIPELINE_FOLDER) / RUN_FILENAME)
    COMPRESSED_PROJECT_FILENAME: ClassVar[str] = "code.zip"
    RUN_TEMPLATED_CREDENTIALS_FILEPATH: ClassVar[str] = str(
        PurePosixPath(FolderPreparator.SOURCE_FOLDER)
        / "conf"
        / "base"
        / "credentials.yml"
    )
    AZURE_SECTION: ClassVar[str] = "azure"
    STORAGE_SECTION: ClassVar[str] = "storage"

    project: str
    azure_pipeline: str
    python: str
    compute: str
    pipeline: str
    description: str
    experiment: str
    environment: str

    workspace_instance: Workspace

    steps: Dict[str, _PipeStep] = field(init=False, default_factory=dict)
    pipeline_id: str = field(init=False, default=None)

    @property
    def pipeline_object(self) -> Pipeline:
        """Get the pipeline object from 'pipeline_registry'.

        Returns:
            Pipeline: pipeline object.
        """
        path = ".".join([self.project, "pipeline_registry"])
        module = import_module(path)
        return module.register_pipelines()[self.pipeline]

    @classmethod
    def _normalize_connector(cls, conn: str) -> str:
        """Convert an IO string to normalized name.

        Args:
            conn (str)

        Returns:
            str
        """
        return re.sub(r"[^a-zA-Z0-9]", "_", conn)

    @classmethod
    def get_normalized_outputs(cls, node: Node) -> List[str]:
        """Return all output references normalized.

        Args:
            node (Node)

        Returns:
            List[str]
        """
        return [cls._normalize_connector(out) for out in node.outputs]

    @classmethod
    def get_normalized_inputs(cls, node: Node) -> List[str]:
        """Return all input references normalized.

        Args:
            node (Node)

        Returns:
            List[str]
        """
        return [cls._normalize_connector(inp) for inp in node.inputs]

    @classmethod
    def get_step_name(cls, node: Node) -> str:
        """Return the node name.

        Args:
            node (Node)

        Raises:
            KeyError: thrown whenever a node doesn't have its name set

        Returns:
            str
        """
        name = node.name
        if name is None:
            raise ValueError(f'node "{node.name}" not named')
        return name

    def _add_step(self, node: Node, all_outputs: Set[str]):
        """Adds a step dict given a node.

        Args:
            node (Node)
            inputs (Dict[str, str])
        """
        name = self.get_step_name(node)
        self.steps[name] = {
            "name": name,
            "script_name": self.RUN_FILENAME,
            "allow_reuse": False,
            "runconfig": "runconfig.yml",
            "source_directory": self.PIPELINE_FOLDER,
            "type": "PythonScriptStep",
            "arguments": ["--pipeline", self.pipeline, "--node", name],
        }

        # get connections only
        inputs = [i for i in self.get_normalized_inputs(node) if i in all_outputs]
        if inputs:
            self.steps[name]["inputs"] = {i: {"source": i} for i in inputs}

        outputs = self.get_normalized_outputs(node)
        if outputs:
            self.steps[name]["outputs"] = {o: {"destination": o} for o in outputs}

    def _add_steps(self):
        """Adds the nodes from the pipeline as step dicts."""
        pipeline = self.pipeline_object
        outputs = [self._normalize_connector(o) for o in pipeline.all_outputs()]
        for node in pipeline.nodes:
            self._add_step(node, outputs)

    def _build_run(self) -> str:
        """Creates the kedro caller script."""
        return (Path(__file__).parent / self.RUN_FILENAME).read_text()

    def _build_pipeline(self) -> _PipePipeline:
        self._add_steps()
        return {
            "pipeline": {
                "name": self.project,
                "description": self.description,
                "default_compute": self.compute,
                "steps": self.steps,
            }
        }

    def _build_env(self) -> _PipeEnv:
        return {"name": self.environment}

    def _build_runconfig(self) -> _PipeRunConfig:
        return {"environment": self.ENV_FILENAME}

    def _save_run(self):
        writestr(self.RUN_FILEPATH, self._build_run())

    def _save_env(self):
        writejson(self.ENV_FILENAME, self._build_env())

    def _save_runconfig(self):
        writeyml(self.RUNCONFIG_FILENAME, self._build_runconfig())

    def _save_pipeline(self):
        writeyml(self.PIPELINE_FILENAME, self._build_pipeline())

    def save(self):
        """Saves the pipeline, run, runconfig and env files."""
        self._save_run()
        self._save_env()
        self._save_runconfig()
        self._save_pipeline()

    def _fetch_pipeline_id(self):
        """Checks if a pipeline draft already exists and sets its id."""
        pipes = PipelineDraft.list(self.workspace_instance)
        for pipe in pipes:
            if pipe.name == self.azure_pipeline:
                self.pipeline_id = pipe.id
                break
        else:
            self.pipeline_id = None

    def _prepare_folder(self):
        os.mkdir(self.PIPELINE_FOLDER)
        shutil.make_archive(
            *self.COMPRESSED_PROJECT_FILENAME.split("."), self.SOURCE_FOLDER
        )
        shutil.move(self.COMPRESSED_PROJECT_FILENAME, self.PIPELINE_FOLDER)

    def _submit_update(self):
        self.azml(
            "pipeline",
            "update-draft",
            "--name",
            self.azure_pipeline,
            "--experiment_name",
            self.experiment,
            "--pipeline-yaml",
            self.PIPELINE_FILENAME,
            "--continue",
            "False",
            "--pipeline-draft-id",
            self.pipeline_id,
            json=True,
        )

    def _submit_create(self):
        call_json: _PipeCLIDraft = self.azml(
            "pipeline",
            "create-draft",
            "--name",
            self.azure_pipeline,
            "--experiment_name",
            self.experiment,
            "--pipeline-yaml",
            self.PIPELINE_FILENAME,
            "--continue",
            "False",
            json=True,
        )
        self.pipeline_id = call_json["Id"]

    def submit(self) -> dict:
        """Submits the pipeline to AzureML.

        Returns:
            dict: Pipeline draft id
        """
        self._fetch_pipeline_id()
        if self.pipeline_id:
            self.log("info", "pipeline draft already exists, updating it")
            self._submit_update()
        else:
            self.log("info", "pipeline draft doesn't exist, creating it")
            self._submit_create()

    def run(self) -> dict:
        """Converts a Kedro pipeline to an azureml pipeline.

        Returns:
            dict: Pipeline run id
        """
        self._prepare_folder()
        self.save()
        self.submit()
        return {"pipeline_id": self.pipeline_id}


@dataclass
class Cleaner(ConverterStep):
    """Removes the files created by the script."""

    def clean(self):
        """Removes the files created by the script."""
        os.chdir(os.path.dirname(os.getcwd()))
        shutil.rmtree(self.CONVERTER_FOLDER)

    def run(self):
        """Removes the files created by the script."""
        self.clean()
