"""DataStore management classes."""
from logging import getLogger
from azureml.core import (
    Workspace,
    Datastore as _Datastore,
)
from typing import TypedDict


logger = getLogger(__name__)


class ServicePrincipal(TypedDict):
    """Service principal."""

    client_id: str
    client_secret: str
    tenant_id: str


class InvalidDataStoreError(Exception):
    """Raised when a datastore is not a Gen2 or have private credentials."""

    def __init__(self, datastore: str):
        """Initializes the InvalidDataStoreError exception.

        Args:
            datastore (str): Datastore name.
        """
        super().__init__(
            f"Datastore {datastore} is not a Gen2 or have private" " credentials."
        )


class DataStore:
    """Returns the service principal given a datastore name.

    Attributes:
        workspace (Workspace): Azure workspace.
    """

    def __init__(self, workspace: Workspace):
        """Initializes the DataStore wrapper.

        Args:
            workspace (Workspace): Azure workspace.
        """
        self.workspace = workspace

    def _validate(self, ds: _Datastore) -> bool:
        return (
            getattr(ds, "client_id")
            and getattr(ds, "client_secret")
            and getattr(ds, "tenant_id")
        )

    def __getitem__(self, datastore: str) -> ServicePrincipal:
        """Gets the service principal from a datastore.

        Args:
            datastore (str): Datastore name.

        Raises:
            InvalidDataStoreError: Raised when the datastore is not a Gen2 or
                have private credentials.

        Returns:
            ServicePrincipal: Service principal credentials dict.
        """
        logger.info(f'Getting service principal from "{datastore}"')
        ds = _Datastore.get(self.workspace, datastore)

        if not self._validate(ds):
            raise InvalidDataStoreError(datastore)

        return {
            "client_id": ds.client_id,
            "client_secret": ds.client_secret,
            "tenant_id": ds.tenant_id,
        }
