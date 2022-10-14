"""Databricks configuration module."""
import json
from azureml.core import Workspace
from pathlib import Path
from projetaai_azure.runners.keyvault import Keyvault


def is_databricks_project(folder: Path = None) -> bool:
    """Checks if the project is a Databricks project.

    Args:
        folder (Path, optional): Project root. Defaults to Path.cwd().

    Returns:
        bool: True if the project is a Databricks project.
    """
    folder = folder or Path.cwd()
    return (folder / "conf" / "base" / "spark.yml").exists()


def configure_databricks_connect(
    workspace: Workspace, folder: Path = None, dot_db_connect_folder: Path = None
):
    """Sets up Databricks Connect.

    This function writes the Databricks connect configuration json, called
    `.databricks-connect`. This file contains the information to connect to
    Databricks, which are host, token, org_id, cluster_id and port. To do this
    it uses the Keyvault to get these values.

    Warning:
        In order to use this function, the Keyvault must be set up. You have
        to set the following secrets in the Keyvault:

        - databricks_host
        - databricks_token
        - databricks_org_id
        - databricks_cluster_id
        - databricks_port

    Args:
        workspace (Workspace): Azure workspace.
        folder (Path, optional): Project root folder. Defaults to Path.cwd().
        dot_db_connect_folder (Path, optional): Folder to save
            `.databricks-connect`. Defaults to '/root'.
    """
    if is_databricks_project(folder):
        dot_db_connect_folder = dot_db_connect_folder or Path("/root")
        kv = Keyvault(workspace)
        connect_config = {
            "host": kv["databricks_host"],
            "token": kv["databricks_token"],
            "org_id": kv["databricks_org_id"],
            "cluster_id": kv["databricks_cluster_id"],
            "port": kv["databricks_port"],
        }
        (dot_db_connect_folder / ".databricks-connect").write_text(
            json.dumps(connect_config)
        )
