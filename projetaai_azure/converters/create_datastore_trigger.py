"""Creates a Real Time Endpoint based on a given specification.

Note:
    This script was built on top of Azure ML SDK v1.
    Check the reference for it out in this link:
    https://docs.microsoft.com/pt-br/azure/machine-learning/v1/introduction
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass, field
import datetime
from os import getcwd
from pathlib import Path
from typing import (
    ClassVar,
    List,
    Literal,
    Union,
    cast,
)
import sys
from projetaai_azure._deploy.azureml.config import (
    _ArgvSpecification,
    BasicAzureMLSettingsReader,
)
from projetaai_azure._deploy.azureml.step import ConverterStep
from projetaai_azure._framework.cli.step import pipe

from azureml.core import Workspace, Datastore
from azureml.pipeline.core.schedule import Schedule

sys.path.append(str(Path(getcwd()) / "src"))

WeekDays = Literal[
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
]


@dataclass
class _SettingsReader(BasicAzureMLSettingsReader):
    """Settings reader for pipeline deployment.

    Outputs:
        published_id (str): the published pipeline id
        experiment (str): the name of the experiment on AzureML
        datastore_name (str): name of the datastore to be watched
        datastore_path (str): the path to the desired folder inside the
            datastore
        schedule_name (str): the name of the schedule
        description (str): the description of the schedule
        polling_interval (int): time, in minutes, between each datastore change
            check
    """

    @property
    def argv_requirements(self) -> List[_ArgvSpecification]:
        return [
            {
                "target": "published_id",
                "type": str,
            },
            {
                "target": "experiment",
                "type": str,
            },
            {
                "target": "datastore_name",
                "type": str,
            },
            {
                "target": "datastore_path",
                "type": str,
                "default": lambda _: ".",
            },
            {
                "target": "schedule_name",
                "type": str,
                "default": lambda _: "my-trigger-"
                + datetime.datetime.now().strftime("%Y%m%d%H%M"),
            },
            {
                "target": "description",
                "type": str,
                "default": lambda _: "A trigger created by projetaai",
            },
            {
                "target": "polling_interval",
                "type": str,
                "default": lambda _: 30,
            },
        ]


@dataclass
class Trigger(ConverterStep):
    """Schedules a published pipeline to run when a datastore changes.

    Schedules a published pipeline to run on each change on an specified
    datastore location.

    Requires:
        workspace_instance (Workspace): the workspace instance
        published_id (str): the published pipeline id
        experiment (str): the name of the experiment on AzureML
        datastore_name (str): name of the datastore to be watched
        datastore_path (str): the path to the desired folder inside the
            datastore
        schedule_name (str): the name of the schedule
        description (str): the description of the schedule
        polling_interval (int): time, in minutes, between each datastore change
            check
    """

    SCHEDULE_FILENAME: ClassVar[str] = "schedule.yml"
    TIMEOUT: ClassVar[int] = 3600
    AZ_MIN_DATE: ClassVar[str] = datetime.datetime(2000, 1, 1, 0, 0, 0).isoformat()

    workspace_instance: Workspace

    published_id: str
    experiment: str
    datastore_name: str
    datastore_path: str
    schedule_name: str
    description: str
    polling_interval: int

    old_published_id: str = None
    old_schedule_instance: Union[Schedule, None] = field(init=False, default=None)

    def _find_schedule(self):
        schedules = Schedule.list(
            self.workspace_instance, pipeline_id=self.old_published_id
        )
        self.old_schedule_instance = schedules[0] if schedules else None

    def _disable_old_schedule(self):
        schedule = cast(Schedule, self.old_schedule_instance)
        schedule.disable()

    def create_new_schedule(self):
        """Creates a new schedule."""
        self.datastore = Datastore(
            workspace=self.workspace_instance, name=self.datastore_name
        )
        self.reactive_schedule = Schedule.create(
            self.workspace_instance,
            name=self.schedule_name,
            description=self.description,
            pipeline_id=self.published_id,
            experiment_name=self.experiment,
            datastore=self.datastore,
            path_on_datastore=self.datastore_path,
            polling_interval=self.polling_interval,
        )

    def run(self):
        """Runs Trigger step."""
        self._find_schedule()
        if self.old_schedule_instance:
            self._disable_old_schedule()
        self.create_new_schedule()


def main():
    """Creates a datastore trigger based on the given arguments."""
    pipe(
        _SettingsReader,
        Trigger,
    )


if __name__ == "__main__":
    main()
