"""DevOps execution scripts."""
import click
import os
from projetaai_azure.utils.io import readyml
from kedro_projetaai.utils.io import upwriteyml
from projetaai_azure.utils.constants import CWD
from kedro_projetaai import __version__ as version

LOGO = rf"""
 __   __ __    _ _ __ _ _ _ _ __ _ 
|  \_/  | |   |  __  |     |  _ __|
| |\ /| | |   | |  | |  _ _|_|__ _    
| | V | | |_,_| |__| | |    __ _| | 
|_|   |_|_,_,_|_,_,_,|_|   |_,_,_,|

v{version}
"""


@click.command
@click.version_option(version, "--version", "-V", help="Show version and exit")
def parameters_ci_create():
    """Creates conf/base/parameters_ci.yml, used in commands to automate Azure Dev Ops Management."""

    filepath = CWD / 'conf' / 'base' / 'parameters_ci.yml'

    click.secho(LOGO, fg="green")

    repo_name = click.prompt('Repository name (can be anything you want)')
    organization = click.prompt('Organization name (in Azure DevOps)')
    project = click.prompt('Project name (in Azure DevOps)')
    description_pipeline = click.prompt('Description of CI/CD Pipeline (can be anything you want without punctuation)')
    variables = click.prompt('Dict with environment variables names and values')

    parameters_file = {
        "repo_name": repo_name,
        "organization": organization,
        "project": project,
        "description_pipeline": description_pipeline,
        "variables": variables
    }

    upwriteyml(str(filepath), parameters_file)
    click.echo(f'Updated {filepath}')


@click.command
def devops_create_repo() -> None:
    """Automates repository creation in Azure DevOps. You need to be log in azure with "az login" in terminal

    Args:
        None
    """
    if os.path.exists(CWD / 'conf' / 'base' / 'parameters_ci.yml'):

        parameters = readyml(CWD / 'conf' / 'base' / 'parameters_ci.yml')

        project = parameters['project']
        organization = parameters['organization']
        repo_name = parameters['repo_name']

        os.system(f"az repos create --name {repo_name} --org {organization} --project {project}")

        click.echo(f"Repositório com nome {repo_name} criado dentro do project {project} na organization {organization}")

        pass

    else:

        click.echo("Não foi encontrado o arquivo conf/base/parameters_ci.yml")

        pass


@click.command
def devops_create_pipeline_cicd() -> None:
    """Automates pipeline creation in Azure DevOps
       You need to be log in azure with "az login" in terminal

    Args:
        None
    """

    if os.path.exists(CWD / 'conf' / 'base' / 'parameters_ci.yml'):

        parameters = readyml(CWD / 'conf' / 'base' / 'parameters_ci.yml')

        project = parameters['project']
        organization = parameters['organization']
        repo_name = parameters['repo_name']
        description_pipeline = parameters['description_pipeline']

        os.system(f"az pipelines create --name {repo_name} --description {description_pipeline} --repository {organization}/{project}/_git/{repo_name} --branch main --yml-path azure-pipelines/azure-ci-pipeline.yml --org {organization} --project {project}")

        var_aux = parameters['variables']

        for key, value in var_aux.items():

            os.system(f"az pipelines variable create --name {key} --value '{value}' --org {organization} --pipeline-name {repo_name} --project {project}")

        pass

    else:

        click.echo("Não foi encontrado o arquivo conf/base/parameters_ci.yml")

        pass
