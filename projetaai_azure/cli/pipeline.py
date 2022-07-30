"""Pipeline management scripts."""
from dataclasses import dataclass
from typing import Any, List, get_args
from projetaai_azure.azureml.config import (
    _ArgvSpecification,
    Authenticator,
    BasicAzureMLSettingsReader,
)
from projetaai_azure.azureml.environment import EnvironmentCreator
from projetaai_azure.azureml.pipeline_converter import (
    Cleaner,
    FolderPreparator,
    PipelineConverter,
)
from kedro_projetaai.utils.script import pipe

from projetaai_azure.azureml.publisher import Publisher
from projetaai_azure.azureml.scheduler import Scheduler, WeekDays
from projetaai_azure.utils.iterable import unique


@dataclass
class CreateDraftInputs(BasicAzureMLSettingsReader):
    """Settings reader for pipeline deployment.

    Outputs:
        compute (str): Name of the compute target
        workspace (str): Workspace id
        resource_group (str): Resource group id
        project (str): Name of the project
        python (str): 'major.minor.micro' version of python
        description (str): Description of the project
        pipeline (str): Kedro pipeline name
        azure_pipeline (str): AzureML pipeline name
        experiment (str): AzureML experiment name
    """

    @staticmethod
    def azure_pipeline_default(filled: dict) -> str:
        """Default value for azure_pipeline.

        Args:
            filled (dict): Filled settings

        Returns:
            str: Default value for azure_pipeline
        """
        name = filled['project']

        if filled['pipeline'] == '__default__':
            name += '_default'
        else:
            name += '_' + filled['pipeline']

        if filled['experiment'] != filled['project']:
            name += filled['experiment'].split(filled['project'])[-1]

        return name

    @property
    def argv_requirements(self) -> List[_ArgvSpecification]:
        """Argv requirements for pipeline deployment.

        Returns:
            List[_ArgvSpecification]: _description_
        """
        return ([
            {
                'target': 'pipeline',
                'type': str,
                'default': lambda _: '__default__',
                'help': 'Kedro pipeline name. Defaults to "__default__"',
            },
            {
                'target': 'experiment',
                'type': str,
                'default': lambda _: '',
                'preparator': lambda x, filled:
                f'{filled["project"]}{f"_{x}" if x else ""}',
                'help': 'Experiment name. Defaults to project name.'
            },
            {
                'target': 'azure_pipeline',
                'type': str,
                'default': self.azure_pipeline_default,
            },
        ])


@CreateDraftInputs().click_command
def create_draft(**kwargs: Any):
    """Creates an Azure pipeline."""
    pipe(
        Authenticator,
        FolderPreparator,
        EnvironmentCreator,
        PipelineConverter,
        Cleaner,
        initial_dict=kwargs
    )


@dataclass
class PublishDraftInputs(CreateDraftInputs):
    """Settings reader for pipeline deployment.

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


@PublishDraftInputs().click_command
def publish(**kwargs: Any):
    """Publishes an Azure pipeline."""
    pipe(
        Authenticator,
        Publisher,
        Scheduler,
        initial_dict=kwargs
    )


@dataclass
class SchedulePublishedInputs(CreateDraftInputs):
    """Settings reader for scheduling a pipeline.

    Outputs:
        description (str): Description of the project
        pipeline (str): Kedro pipeline name
        azure_pipeline (str): AzureML pipeline name
        experiment (str): AzureML experiment name
        workspace_instance (Workspace): the workspace instance
        hour (int): the hour of the day to run the pipeline
        minute (int): the minute of the hour to run the pipeline
        day (list): the days of the week to run the pipeline
    """

    @property
    def argv_requirements(self) -> List[_ArgvSpecification]:
        """Argv requirements for scheduling a pipeline.

        Returns:
            List[_ArgvSpecification]
        """
        return super().argv_requirements + [{
            'target': 'hour',
            'type': int,
            'validator': lambda x, filled: (
                x in range(0, 24),
                '"hour" must be between 0 and 23'
            ),
            'required': False,
            'help': 'The hour of the day to run the pipeline.',
        }, {
            'target': 'minute',
            'type': int,
            'default': lambda _: 0,
            'validator': lambda x, _:
            (x in range(0, 60), '"minute" must be in range 0-59'),
            'help': 'The minute of the hour to run the pipeline. '
                    'Defaults to 0.',
        }, {
            'target': 'day',
            'type': List[str],
            'default': lambda _: ['Sunday'],
            'preparator': lambda x, _: unique(x),
            'validator': lambda x, _: (
                set(x).issubset(get_args(WeekDays)),
                '"day" must be a subset of '
                f'"{str(get_args(WeekDays))}"'
            ),
            'help': ('Days of the week to run the pipeline '
                     f'{get_args(WeekDays)}, pass it multiple times for '
                     'multiple days e.g. "--day Monday --day Tuesday". '
                     'Defaults to "Sunday".'),
        }]


@SchedulePublishedInputs().click_command
def schedule(**kwargs: Any):
    """Schedules an Azure pipeline."""
    pipe(
        Authenticator,
        Scheduler,
        initial_dict=kwargs
    )
