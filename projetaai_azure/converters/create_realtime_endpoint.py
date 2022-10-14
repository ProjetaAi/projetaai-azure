"""Creates a Real Time Endpoint based on a given specification.

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

from azureml.core.compute import AksCompute
from azureml.core import Workspace, Environment
from azureml.core.webservice import AksWebservice
from azureml.core.model import Model, InferenceConfig

sys.path.append(str(Path(getcwd()) / "src"))


@dataclass
class _SettingsReader(BasicAzureMLSettingsReader):
    """Settings reader for pipeline deployment.

    Outputs:
        aks_target_name (str): the name of the AKS target we want to run the
            endpoint on
        environment_name (str): the name of the environment on AzureML
        source_directory (str): the path to the entry script code
        entry_script (str): the filename, including file extension, of the
            entry script
        cpu_cores (float): the number of cpu cores desired for the deploy
        memory_gb (float): the amount of run desired for the deploy
        model (str): the name of the registered model at AzureML
        endpoint_name (str): the name of the endpoint
    """

    @property
    def argv_requirements(self) -> List[_ArgvSpecification]:
        return [
            {
                "target": "aks_target_name",
                "type": str,
            },
            {
                "target": "environment_name",
                "type": str,
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
                "target": "cpu_cores",
                "type": str,
                "default": lambda _: 1,
            },
            {
                "target": "memory_gb",
                "type": str,
                "default": lambda _: 1,
            },
            {
                "target": "model",
                "type": str,
            },
            {
                "target": "endpoint_name",
                "type": str,
                "default": lambda _: "my-realtime-endpoint-"
                + datetime.datetime.now().strftime("%Y%m%d%H%M"),
            },
        ]


@dataclass
class CreateRealTimeEndpoint(ConverterStep):
    """Creates a RealTime endpoint using Azure Kubernetes.

    Requires:
        workspace_instance (Workspace): the workspace instance
        aks_target_name (str): the name of the AKS target we want to run the
            endpoint on
        environment_name (str): the name of the environment on AzureML
        source_directory (str): the path to the entry script code
        entry_script (str): the filename, including file extension, of the
            entry script
        cpu_cores (float): the number of cpu cores desired for the deploy
        memory_gb (float): the amount of run desired for the deploy
        model (str): the name of the registered model at AzureML
        endpoint_name (str): the name of the endpoint
    """

    workspace_instance: Workspace
    aks_target_name: str
    environment_name: str
    source_directory: str
    entry_script: str
    cpu_cores: float
    memory_gb: float
    model_name: str
    endpoint_name: str

    def _get_basic_deploy_info(self):
        self.aks_target = AksCompute(self.workspace_instance, self.aks_target_name)
        self.service_env = Environment(name=self.environment_name)
        self.model = self.workspace_instance.models[self.model_name]

    def _define_general_config(self):
        self.classifier_inference_config = InferenceConfig(
            source_directory=self.source_directory,
            entry_script=self.entry_script,
            environment=self.service_env,
        )

        self.classifier_deploy_config = AksWebservice.deploy_configuration(
            cpu_cores=self.cpu_cores, memory_gb=self.memory_gb
        )

    def _deploy_realtime_endpoint(self):
        self.service = Model.deploy(
            workspace=self.workspace_instance,
            name=self.endpoint_name,
            models=[self.model],
            inference_config=self.classifier_inference_config,
            deployment_config=self.classifier_deploy_config,
            deployment_target=self.aks_target,
        )
        print("The service is being deployed. Please wait...")
        self.service.wait_for_deployment(show_output=True)

    def run(self):
        """Runs CreateRealTimeEndpoint."""
        self._get_basic_deploy_info()
        self._define_general_config()
        self._deploy_realtime_endpoint()


def main():
    """Creates a realtime endpoint."""
    pipe(
        _SettingsReader,
        CreateRealTimeEndpoint,
    )


if __name__ == "__main__":
    main()
