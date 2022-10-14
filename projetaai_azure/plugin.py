"""Azure plugin for ProjetaAi."""
from typing import List
import warnings
from projetaai_azure.converters.setup import install_azml_cli
from projetaai_azure.cli.credential import credential_create

warnings.filterwarnings("ignore", category=DeprecationWarning)  # fixes azureml warnings

from projetaai_azure.cli.run import create_job  # noqa: E402
from click import Command  # noqa: E402
from kedro_projetaai.cli import ProjetaAiCLIPlugin, CIStarterSpec  # noqa: E402
from projetaai_azure.cli.pipeline import create_draft, publish, schedule  # noqa: E402


install_azml_cli()


class AzureCLI(ProjetaAiCLIPlugin):
    """Azure commands for ProjetaAi."""

    @property
    def credential_create(self) -> Command:
        """Create a credential for Azure Blob Storage.

        Returns:
            Command: Command for creating a credential.
        """
        return credential_create

    @property
    def pipeline(self) -> List[Command]:
        """List of pipeline commands.

        Returns:
            List[Command]: List of pipeline commands
        """
        return [publish, schedule]

    @property
    def pipeline_create(self) -> Command:
        """Create pipeline command.

        Returns:
            Command: Create pipeline command
        """
        return create_draft

    @property
    def run(self) -> Command:
        """Job creation command.

        Returns:
            Command: Job creation command
        """
        return create_job


AZURE_STARTERS_REPO = "git+https://github.com/ProjetaAi/" "projetaai-azure-starters.git"

ci_starters = [
    CIStarterSpec(
        alias="azure-pipelines",
        template_path=AZURE_STARTERS_REPO,
        directory="ci/azure-pipelines",
    )
]
