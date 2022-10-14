"""
Creates a Batch Pipeline based on a given specification.

Note:
    This script was built on top of Azure ML SDK v1.
    Check the reference for it out in this link:
    https://docs.microsoft.com/pt-br/azure/machine-learning/v1/introduction
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass
from os import getcwd
import datetime
from pathlib import Path
from typing import (
    List,
)
import sys
from projetaai_azure._deploy.azureml.config import (
    _ArgvSpecification,
    BasicAzureMLSettingsReader,
)
from projetaai_azure._deploy.azureml.step import ConverterStep
from projetaai_azure._framework.cli.step import pipe

from azureml.core import Workspace, Experiment, Environment
from azureml.core.compute import AmlCompute
from azureml.core.runconfig import RunConfiguration
from azureml.pipeline.core import Pipeline
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import ParallelRunConfig, ParallelRunStep

sys.path.append(str(Path(getcwd()) / "src"))


@dataclass
class _SettingsReader(BasicAzureMLSettingsReader):
    """Settings reader for pipeline deployment.

    Outputs:
        aml_compute_target (str): the compute target name at AzureML
        environment_name (str): the name of the environment to be used
        dataset_name (str): the name of the dataset to be processed
        output_dir_name (str): the output path
        source_directory (str): the path to the scoring script
        entry_script (str): the scoring script filename, including extension
        mini_batch_size (int): the size of the mini-batches
        error_threshold (int): the error threshold accepted
        node_count (int): the amount of nodes to be used on the scoring
        job_name (str): the desired name for the job
    """

    @property
    def argv_requirements(self) -> List[_ArgvSpecification]:
        return [
            {
                "target": "aml_compute_target",
                "type": str,
            },
            {
                "target": "environment_name",
                "type": str,
            },
            {
                "target": "dataset_name",
                "type": str,
            },
            {
                "target": "output_dir_name",
                "type": str,
                "default": lambda _: "output",
            },
            {
                "target": "source_directory",
                "type": str,
                "default": lambda _: ".",
            },
            {
                "target": "entry_script",
                "type": str,
            },
            {
                "target": "mini_batch_size",
                "type": str,
                "default": lambda _: 10,
            },
            {
                "target": "error_threshold",
                "type": str,
                "default": lambda _: 10,
            },
            {
                "target": "node_count",
                "type": str,
                "default": lambda _: 1,
            },
            {
                "target": "job_name",
                "type": str,
                "default": lambda _: "my-batch-pipeline-"
                + datetime.datetime.now().strftime("%Y%m%d%H%M"),
            },
        ]


@dataclass
class CreateBatchPipeline(ConverterStep):
    """Creates an endpoint for Batch inference.

    Requires:
        workspace_instance (Workspace): the workspace instance
        aml_compute_target (str): the compute target name at AzureML
        environment_name (str): the name of the environment to be used
        dataset_name (str): the name of the dataset to be processed
        output_dir_name (str): the output path
        source_directory (str): the path to the scoring script
        entry_script (str): the scoring script filename, including extension
        mini_batch_size (int): the size of the mini-batches
        error_threshold (int): the error threshold accepted
        node_count (int): the amount of nodes to be used on the scoring
        job_name (str): the desired name for the job
    """

    workspace_instance: Workspace
    aml_compute_target: str
    environment_name: str
    dataset_name: str
    output_dir_name: str
    source_directory: str
    entry_script: str
    mini_batch_size: int
    error_threshold: int
    node_count: int
    job_name: str

    def _get_basic_deploy_info(self):
        # Compute
        self.aml_compute = AmlCompute(self.workspace_instance, self.aml_compute_target)

        # Environment
        self.aml_run_config = RunConfiguration()
        self.aml_run_config.target = self.aml_compute
        self.curated_environment = Environment.get(
            workspace=self.workspace_instance, name=self.environment_name
        )
        self.aml_run_config.environment = self.curated_environment

    def _get_basic_data_info(self):
        self.def_blob_store = self.workspace_instance.get_default_datastore()
        self.batch_data_set = self.workspace_instance.datasets[self.dataset_name]
        self.output_dir = OutputFileDatasetConfig(name=self.output_dir_name)

    def _define_parallelrun_config(self):
        self.parallel_run_config = ParallelRunConfig(
            source_directory=self.source_directory,
            entry_script=self.entry_script,
            mini_batch_size=self.mini_batch_size,
            error_threshold=self.error_threshold,
            output_action="append_row",
            environment=self.curated_environment,
            compute_target=self.aml_compute_target,
            node_count=self.node_count,
        )

    def _create_batch_pipeline_step(self):
        self.parallelrun_step = ParallelRunStep(
            name="batch-scoring-step",
            parallel_run_config=self.parallel_run_config,
            inputs=[self.batch_data_set.as_named_input("batch_data")],
            output=self.output_dir,
            arguments=[],
            allow_reuse=False,
        )

    def _run_batch_pipeline(self):
        self.pipeline = Pipeline(
            workspace=self.workspace_instance, steps=[self.parallelrun_step]
        )
        self.pipeline_run = Experiment(self.workspace_instance, self.job_name).submit(
            self.pipeline
        )

    def run(self) -> None:
        """Run all the pipeline deployment scripts."""
        self._get_basic_deploy_info()
        self._get_basic_data_info()
        self._define_parallelrun_config()
        self._create_batch_pipeline_step()
        return self._run_batch_pipeline()


def main():
    """Run a pipeline inference using batch."""
    pipe(
        _SettingsReader,
        CreateBatchPipeline,
    )


if __name__ == "__main__":
    main()
