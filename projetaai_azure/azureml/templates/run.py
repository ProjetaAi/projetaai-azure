"""AzureML/Kedro runner."""
from __future__ import annotations
import os
import subprocess
import sys
import shutil
from typing import Dict, Tuple
from azureml.core import Workspace, Experiment, Run
from azureml.core.datastore import Datastore


def unzip_code():
    """Unpack the code."""
    shutil.unpack_archive('{{code_archive}}')


def get_azure_objects() -> Tuple[Workspace, Experiment, Run]:
    """Obtain Azure objects from the current run."""
    run: Run = Run.get_context()
    experiment: Experiment = run.experiment
    workspace: Workspace = experiment.workspace
    return workspace, experiment, run


def set_credentials(
    workspace: Workspace, templates: Dict[str, Dict[str, str]]
):
    """Set the Azure credentials in the environment.

    Args:
        workspace (Workspace): Azure workspace.
        templates (Dict[str, Dict[str, str]]): Datastore by template.
    """
    for datastore, template in templates.items():
        datastore = Datastore.get(workspace, datastore)
        for attr, variable in template.items():
            os.environ[variable] = getattr(datastore, attr)


def run_code():
    """Run ProjetaAI project.

    Raises:
        Exception: If the project fails to run.
    """
    process = subprocess.Popen(['kedro', 'run', 'local', *sys.argv[1:]],
                               env=os.environ)
    exit_code = process.wait()

    if exit_code != 0:
        raise Exception("Kedro run failed.")


def main():
    """Run the project."""
    unzip_code()
    workspace, _, _ = get_azure_objects()
    set_credentials(workspace, {{credentials}})  # noqa: F821
    run_code()


if __name__ == '__main__':
    main()
