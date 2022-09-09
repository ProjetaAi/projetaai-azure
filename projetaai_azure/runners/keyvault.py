"""Keyvault access classes."""
from logging import getLogger
from typing import Mapping, Any, Iterable
from azureml.core import Workspace, Keyvault as _Keyvault


logger = getLogger(__name__)


class Keyvault(Mapping):
    """Keyvault as dict wrapper.

    Attributes:
        workspace (Workspace): Azure workspace.
    """

    def __init__(self, workspace: Workspace):
        """Initializes the Keyvault wrapper.

        Args:
            workspace (Workspace): Azure workspace.
        """
        self.workspace = workspace

    @property
    def keyvault(self) -> _Keyvault:
        """Gets the default Keyvault.

        Returns:
            azureml.core.Keyvault: Keyvault.
        """
        return self.workspace.get_default_keyvault()

    def __getitem__(self, secret: str) -> Any:
        """Gets a secret from the Keyvault.

        Args:
            secret (str): Secret name.

        Returns:
            Any: Secret value.
        """
        logger.info(f'Getting secret "{secret}"')
        return self.keyvault.get_secret(secret)

    def __len__(self) -> int:
        """Gets the number of secrets in the Keyvault.

        Returns:
            int: Number of secrets.
        """
        return len(self.keys())

    def __iter__(self) -> Iterable[str]:
        """Gets the Keyvault secrets.

        Returns:
            Iterable[str]: Secret names.
        """
        return iter(tuple(self.keyvault.list_secrets()))
