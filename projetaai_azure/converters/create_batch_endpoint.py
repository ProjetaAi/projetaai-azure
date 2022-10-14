"""Creates a Batch Endpoint based on a given specification.

Note:
    This script was built on top of Azure ML SDK v2.
    Check the reference for it out in this link:
    https://docs.microsoft.com/en-us/azure/machine-learning/concept-v2
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass
from os import getcwd
import datetime
from pathlib import Path
from typing import (
    Any,
    List,
)
import sys
from projetaai_azure._deploy.azureml.config import (
    _ArgvSpecification,
    BasicAzureMLSettingsReader,
)
from projetaai_azure._deploy.azureml.step import ConverterStep
from projetaai_azure._framework.cli.step import pipe

from azure.ai.ml import MLClient
from azure.ai.ml.entities import (
    BatchEndpoint,
    BatchDeployment,
    Model,
    BatchRetrySettings,
)
from azure.identity import DefaultAzureCredential
from azure.ai.ml.constants import BatchDeploymentOutputAction

sys.path.append(str(Path(getcwd()) / "src"))


@dataclass
class _SettingsReader(BasicAzureMLSettingsReader):
    """Settings reader for pipeline deployment.

    Outputs:
        subscription_id (str): the id of the Azure subscription
        resource_group (str): the name of the resource group which contains
            the AzureML workspace
        workspace_name (str):the name of the AzureML workspace
        batch_endpoint_name (str): the desired name for the batch endpoint
        description (str): the desired description for the batch endpoint
        compute (str): the name of the compute to be used
        model_path (str): the local path of the model to be used
        environment_name (str): the name of the environment
        environment_label (str): the label of the environment
        deployment_name (str): the desired name for the deployment
        deployment_description (str): the desired description for the
            deployment
        code_path (str): the path to the scoring script
        scoring_script (str): the filename, including file extension, of the
            scoring script
        instance_count (int): the amount of nodes to be used on the scoring
        max_concurrency_per_instance (int): the amount of cores per node to be
            used on scoring
        mini_batch_size (int): the size of the mini-batches
    """

    @property
    def argv_requirements(self) -> List[_ArgvSpecification]:
        return [
            {
                "target": "subscription_id",
                "type": str,
            },
            {
                "target": "resource_group",
                "type": str,
            },
            {
                "target": "workspace_name",
                "type": str,
            },
            {
                "target": "batch_endpoint_name",
                "type": str,
                "default": lambda _: "my-batch-endpoint-"
                + datetime.datetime.now().strftime("%Y%m%d%H%M"),
            },
            {
                "target": "description",
                "type": str,
                "default": lambda _: "A batch endpoint created by projetaai",
            },
            {
                "target": "compute",
                "type": str,
            },
            {
                "target": "model_path",
                "type": str,
            },
            {
                "target": "environment_name",
                "type": str,
            },
            {
                "target": "environment_label",
                "type": str,
                "default": lambda _: "latest",
            },
            {
                "target": "deployment_name",
                "type": str,
                "default": lambda _: "my-batch-deploy-"
                + datetime.datetime.now().strftime("%Y%m%d%H%M"),
            },
            {
                "target": "deployment_description",
                "type": str,
                "default": lambda _: "A batch deployment created by projetaai",
            },
            {
                "target": "scoring_script",
                "type": str,
            },
            {
                "target": "instance_count",
                "type": int,
                "default": lambda _: 1,
            },
            {
                "target": "max_concurrency_per_instance",
                "type": int,
                "default": lambda _: 2,
            },
            {
                "target": "mini_batch_size",
                "type": int,
                "default": lambda _: 10,
            },
        ]


@dataclass
class CreateBatchEndpoint(ConverterStep):
    """Creates an endpoint for Batch inference.

    Requires:
        subscription_id (str): the id of the Azure subscription
        resource_group (str): the name of the resource group which contains
            the AzureML workspace
        workspace_name (str):the name of the AzureML workspace
        batch_endpoint_name (str): the desired name for the batch endpoint
        description (str): the desired description for the batch endpoint
        compute (str): the name of the compute to be used
        model_path (str): the local path of the model to be used
        environment_name (str): the name of the environment
        environment_label (str): the label of the environment
        deployment_name (str): the desired name for the deployment
        deployment_description (str): the desired description for the
            deployment
        code_path (str): the path to the scoring script
        scoring_script (str): the filename, including file extension, of the
            scoring script
        instance_count (int): the amount of nodes to be used on the scoring
        max_concurrency_per_instance (int): the amount of cores per node to be
            used on scoring
        mini_batch_size (int): the size of the mini-batches
    """

    subscription_id: str
    resource_group: str
    workspace_name: str
    batch_endpoint_name: str
    description: str
    compute: str
    model_path: str
    environment_name: str
    environment_label: str
    deployment_name: str
    deployment_description: str
    code_path: str
    scoring_script: str
    instance_count: int
    max_concurrency_per_instance: int
    mini_batch_size: int

    def _set_ml_client(self):
        self.ml_client = MLClient(
            DefaultAzureCredential(),
            self.subscription_id,
            self.resource_group,
            self.workspace_name,
        )

    def _create_endpoint(self):
        endpoint = BatchEndpoint(
            name=self.batch_endpoint_name, description=self.description
        )
        self.ml_client.begin_create_or_update(endpoint)

    def _get_basic_deploy_info(self):
        self.model = Model(path=self.model_path)
        self.env = self.ml_client.environments.get(
            name=self.environment_name, label=self.environment_label
        )

    def _deploy_batch_endpoint(self):
        deployment = BatchDeployment(
            name=self.deployment_name,
            description=self.deployment_description,
            endpoint_name=self.batch_endpoint_name,
            model=self.model,
            code_path=self.code_path,
            scoring_script=self.scoring_script,
            environment=self.env,
            compute=self.compute,
            instance_count=self.instance_count,
            max_concurrency_per_instance=self.max_concurrency_per_instance,
            mini_batch_size=self.mini_batch_size,
            output_action=BatchDeploymentOutputAction.APPEND_ROW,
            output_file_name="predictions.csv",
            retry_settings=BatchRetrySettings(max_retries=5, timeout=60),
            logging_level="info",
        )
        print("The service is being deployed. Please wait...")
        self.ml_client.begin_create_or_update(deployment)

    def run(self):
        """Runs CreateBatchEndpoint."""
        self._set_ml_client()
        self._create_endpoint()
        self._get_basic_deploy_info()
        self._deploy_batch_endpoint()


def main(**kwargs: Any):
    """Creates a batch endpoint for AzureML."""
    pipe(_SettingsReader, CreateBatchEndpoint, initial_dict=kwargs)


if __name__ == "__main__":
    main()
