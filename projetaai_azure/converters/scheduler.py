"""
Schedules a published pipeline to run on a schedule.

Note:
    This script was built on top of Azure ML CLI v1.
    Check the reference for it out in this link:
    https://docs.microsoft.com/en-us/cli/azure/ml(v1)?view=azure-cli-latest
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass, field
import datetime
from typing import (
    ClassVar,
    List,
    Literal,
    Union,
    cast,
)

from projetaai_azure.converters.step import ConverterStep

from azureml.core import Workspace
from azureml.pipeline.core import Schedule, ScheduleRecurrence
from azureml.pipeline.core import TimeZone
from azureml.pipeline.core import (
    PipelineEndpoint,
)


WeekDays = Literal[
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
]


@dataclass
class Scheduler(ConverterStep):
    """Schedules a published pipeline to run on a schedule.

    Requires:
        workspace_instance (Workspace): the workspace instance
        description (str): the description of the schedule
        published_id (str): the published pipeline id
        azure_pipeline (str): the name of the pipeline on AzureML
        experiment (str): the name of the experiment on AzureML
        hour (int): the hour of the day to run the pipeline
        minute (int): the minute of the hour to run the pipeline
        day (list): the days of the week to run the pipeline
        old_bulished_id (str, optional): the id of the previous published
            pipeline
    """

    SCHEDULE_FILENAME: ClassVar[str] = "schedule.yml"
    TIMEOUT: ClassVar[int] = 3600
    AZ_MIN_DATE: ClassVar[str] = datetime.datetime(2000, 1, 1, 0, 0, 0).isoformat()

    workspace_instance: Workspace

    description: str
    azure_pipeline: str
    experiment: str

    hour: int
    minute: int
    day: List[WeekDays]

    old_published_id: str = None
    published_id: str = field(init=False)
    old_schedule_instance: Union[Schedule, None] = field(init=False, default=None)

    def _fetch_published(self):
        """Fetches the published pipeline id."""
        try:
            endpoint = PipelineEndpoint.get(
                self.workspace_instance, name=self.azure_pipeline
            )
            self.published_id = endpoint.get_pipeline().id
        except Exception as e:
            raise RuntimeError("published pipeline not found") from e

    def _find_schedule(self):
        if self.old_published_id:
            schedules: List[Schedule] = Schedule.list(
                self.workspace_instance, pipeline_id=self.old_published_id
            )
            if schedules:
                if schedules[0]._pipeline_id == self.old_published_id:
                    self.old_schedule_instance = schedules[0]

    def create_new_schedule(self):
        """Creates a new schedule."""
        recurrence = ScheduleRecurrence(
            frequency="Week",
            hours=[self.hour],
            minutes=[self.minute],
            week_days=self.day,
            start_time=self.AZ_MIN_DATE,
            # starts in the next scheduled datetime
            interval=1,
            time_zone=TimeZone.UTC,
        )
        Schedule.create(
            workspace=self.workspace_instance,
            pipeline_id=self.published_id,
            name=self.azure_pipeline,
            experiment_name=self.experiment,
            description=self.description,
            recurrence=recurrence,
            continue_on_step_failure=False,
            wait_for_provisioning=True,
            wait_timeout=3600,
        )

    def _disable_old_schedule(self):
        schedule = cast(Schedule, self.old_schedule_instance)
        schedule.disable()

    def _forward_schedule(self):
        schedule = cast(Schedule, self.old_schedule_instance)
        Schedule.create(
            workspace=self.workspace_instance,
            description=self.description,
            experiment_name=self.experiment,
            name=self.azure_pipeline,
            pipeline_id=self.published_id,
            recurrence=schedule.recurrence,
            wait_for_provisioning=schedule._wait_for_provisioning,
            continue_on_step_failure=schedule.continue_on_step_failure,
        )

    def run(self):
        """Schedules a published pipeline to run on a schedule."""
        self._fetch_published()
        self._find_schedule()
        if self.old_schedule_instance:
            if self.hour:
                self.log("info", "an old schedule exists, disabling it")
            else:
                self.log("info", "schedule already exists, forwarding it")
                self._forward_schedule()
            self._disable_old_schedule()

        if self.hour:
            self.log("info", "creating a new schedule")
            self.create_new_schedule()
