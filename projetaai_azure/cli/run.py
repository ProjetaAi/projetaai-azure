"""Azure job execution scripts."""
from dataclasses import dataclass
from typing import Any
from projetaai_azure.converters.config import Authenticator
from projetaai_azure.converters.job_creator import JobCreator
from kedro_projetaai.utils.script import pipe
from projetaai_azure.cli.pipeline import CreateDraftInputs


@dataclass
class CreateJobInputs(CreateDraftInputs):
    """Settings reader for job creation.

    Outputs:
        compute (str): Name of the compute target
        workspace (str): Workspace id
        resource_group (str): Resource group id
        project (str): Name of the project
        description (str): Description of the project
        pipeline (str): Kedro pipeline name
        azure_pipeline (str): AzureML pipeline name
        experiment (str): AzureML experiment name
    """

    pass


@CreateJobInputs().click_command
def create_job(**kwargs: Any):
    """Creates an AzureML job."""
    pipe(Authenticator, JobCreator, initial_dict=kwargs)
