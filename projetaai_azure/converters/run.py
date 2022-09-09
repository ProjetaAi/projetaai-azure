"""AzureML/Kedro runner."""
from __future__ import annotations
import os
from pathlib import Path
import sys
import shutil
from projetaai_azure.converters.pipeline_converter import PipelineConverter
from projetaai_azure.runners.injector import inject
from click.testing import CliRunner
from kedro.framework.cli.project import run
from kedro.framework.startup import bootstrap_project


def unzip_code():
    """Unpacks the code."""
    shutil.unpack_archive(PipelineConverter.COMPRESSED_PROJECT_FILENAME)


def set_azureml_environment():
    """Sets the AzureML flag."""
    os.environ['IS_AZML_ENVIRONMENT'] = 'true'


def run_code():
    """Runs ProjetaAI project."""
    bootstrap_project(Path.cwd())
    runner = CliRunner()
    runner.invoke(run, sys.argv[1:], catch_exceptions=False)


def main():
    """Run the project."""
    unzip_code()
    set_azureml_environment()
    inject()
    run_code()


if __name__ == '__main__':
    main()
