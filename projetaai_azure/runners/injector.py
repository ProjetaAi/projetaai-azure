"""Injects Azure runners into the Kedro context."""
from pathlib import Path
from typing import Tuple
from azureml.core import Run, Experiment, Workspace
import importlib
import os
import sys
from projetaai_azure.runners.config_loader import AzureConfigLoader
import kedro.framework.project as kedro_project
from kedro.framework.startup import bootstrap_project
from projetaai_azure.runners.databricks import configure_databricks_connect


def is_azureml_environment() -> bool:
    """Checks if the current environment is AzureML."""
    return "IS_AZML_ENVIRONMENT" in os.environ


def get_azure_objects() -> Tuple[Workspace, Experiment, Run]:
    """Obtains Azure objects from the current run."""
    run: Run = Run.get_context()
    experiment: Experiment = run.experiment
    workspace: Workspace = experiment.workspace
    return workspace, experiment, run


def configure_settings(workspace: Workspace):
    """Configures the kedro settings.py file.

    Args:
        workspace (Workspace): The AzureML workspace.
    """
    path = Path.cwd()

    # Remove false caches
    src = str(path / "src")
    if sys.path_importer_cache.get(src, True) is None:
        del sys.path_importer_cache[src]

    bootstrap_project(path)

    settings = importlib.import_module(f"{kedro_project.PACKAGE_NAME}.settings")
    settings.CONFIG_LOADER_CLASS = AzureConfigLoader
    settings.CONFIG_LOADER_ARGS = {
        **getattr(settings, "CONFIG_LOADER_ARGS", {}),
        "workspace": workspace,
    }

    bootstrap_project(path)


def inject():
    """Injects the AzureML objects into the Kedro context."""
    if is_azureml_environment():
        workspace, _, _ = get_azure_objects()
        configure_settings(workspace)
        configure_databricks_connect(workspace)
