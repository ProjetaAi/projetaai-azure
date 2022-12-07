"""Databricks configuration module."""
import json
from azureml.core import Workspace
from pathlib import Path
from projetaai_azure.runners.keyvault import Keyvault
from kedro.config import ConfigLoader


def is_databricks_project(folder: Path = None) -> bool:
    """Checks if the project is a Databricks project.

    Args:
        folder (Path, optional): Project root. Defaults to Path.cwd().

    Returns:
        bool: True if the project is a Databricks project.
    """
    folder = folder or Path.cwd()
    return (folder / 'conf' / 'base' / 'spark.yml').exists()


def configure_databricks_connect(
    workspace: Workspace,
    config: ConfigLoader,
    folder: Path = None,
    spark_config_path: Path = None,
    dot_db_connect_folder: Path = None
):
    """Sets up Databricks Connect.

    This function writes the Databricks connect configuration json, called
    `.databricks-connect`. This file contains the information to connect to
    Databricks, which are host, token, org_id, cluster_id and port.
    The token must be stored in the keyvault. Its string, valueto look for
    in the KV, must be set on the spark.yaml in conf/base as well as all other
    informations.

    Warning:
        In order to use this function, the Keyvault must be set up. You have
        to set the databricks token from the desired cluster as a secret
        in the Keyvault

        conf/base/spark.yaml structure
        **replace all values between {} for the correct value**

        spark.databricks.service.address: https://{cluster_address}
        spark.databricks.service.clusterId: {cluster_id}
        spark.databricks.service.orgId: {org_id}
        spark.databricks.service.port: {cluster_port}
        spark.databricks.service.token: "kv::{value to look for in the keyvault}"

    Args:
        workspace (Workspace): Azure workspace.
        folder (Path, optional): Project root folder. Defaults to Path.cwd().
        spark_config_path (Path, optional): spark.yaml path. Defaults to
              conf/base/spark.yaml in the project root
        dot_db_connect_folder (Path, optional): Folder to save
            `.databricks-connect`. Defaults to '/root'.
    """
    if is_databricks_project(folder):
        dot_db_connect_folder = dot_db_connect_folder or Path('/root')
        kv = Keyvault(workspace)

        spark_config = config.get("spark*", "spark*/**")

        _host = spark_config["spark.databricks.service.address"]
        _cluster_id = spark_config["spark.databricks.service.clusterId"]
        _org_id = spark_config["spark.databricks.service.orgId"]
        _port = spark_config["spark.databricks.service.port"]
        _token_string = spark_config["spark.databricks.service.token"]

        connect_config = {
            'host': _host,
            'token': kv[str(_token_string)],
            'org_id': _org_id,
            'cluster_id': _cluster_id,
            'port': _port,
        }
        (dot_db_connect_folder / '.databricks-connect').write_text(
            json.dumps(connect_config))
