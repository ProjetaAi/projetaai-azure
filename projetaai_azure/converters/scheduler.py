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
from os.path import exists
from pathlib import Path
from typing import (
    ClassVar,
    List,
    Literal,
    Union,
    cast,
)

from projetaai_azure.utils.io import readyml
from projetaai_azure.converters.step import ConverterStep

from azureml.core import Workspace, Datastore
from azureml.pipeline.core import Schedule, ScheduleRecurrence
from azureml.pipeline.core import TimeZone
from azureml.pipeline.core import (
    PipelineEndpoint,
    Pipeline,
)


WeekDays = Literal['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday',
                   'Friday', 'Saturday']


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

    TIMEBASED_SCHEDULE_FILENAME: ClassVar[str] = str(
        Path('conf') / 'base' / 'timebased_schedule.yml'
    )
    CHANGEBASED_SCHEDULE_FILENAME: ClassVar[str] = str(
        Path('conf') / 'base' / 'changebased_schedule.yml'
    )

    TIMEOUT: ClassVar[int] = 3600
    AZ_MIN_DATE: ClassVar[str] = datetime.datetime(2000, 1, 1, 0, 0,
                                                   0).isoformat()

    workspace_instance: Workspace

    description: str
    azure_pipeline: str
    experiment: str

    hour: int
    minute: int
    day: List[WeekDays]

    pipeline_published: Pipeline = field(init=False)

    old_published_id: str = None
    published_id: str = field(init=False)
    old_schedule_instance: Union[Schedule,
                                 None] = field(init=False, default=None)

    TEMPLATE_FILE_DICT = {
        'Minute': '''
                scheduler:
                    frequency: "Minute" # "Minute", "Hour", "Day", "Week", or "Month"
                    interval: "1"
                 ''',
        'Hour': '''
                scheduler:
                    frequency: "Minute" # "Minute", "Hour", "Day", "Week", or "Month"
                    interval: "1"
                 ''',
        'Day': '''
                scheduler:
                    frequency: "Minute" # "Minute", "Hour", "Day", "Week", or "Month"
                    hours: "10,12,14,16,18,20,22" # For Day and Week you can specify a list of hours separated by comma
                    minutes: "0" # For Day and Week you can specify a list of minutes separated by comma
                    interval: "1"
                 ''',
        'Week': '''
                scheduler:
                    frequency: "Minute" # "Minute", "Hour", "Day", "Week", or "Month"
                    hours: "10,12,14,16,18,20,22" # For Day and Week you can specify a list of hours separated by comma
                    minutes: "0" # For Day and Week you can specify a list of minutes separated by comma
                    week_days:  "Monday, Tuesday, Wednesday, Thursday, Friday, Saturday,Sunday" # For Day and Week you can specify a weekdays separated by comma
                    interval: "1"
                ''',
        'Month': '''
                scheduler:
                    frequency: "Minute" # "Minute", "Hour", "Day", "Week", or "Month"
                    interval: "1"
                 ''',
    }

    LIST_OF_FIELDS = {
        'Minute': ['frequency', 'interval'],
        'Hour': ['frequency', 'interval'],
        'Day': ['frequency', 'hours', 'minutes', 'interval'],
        'Week': ['frequency', 'hours', 'minutes', 'week_days', 'interval'],
        'Month': ['frequency', 'interval'],
    }

    def _fetch_published(self):
        """Fetches the published pipeline id."""
        try:
            print(self.azure_pipeline)
            endpoint = PipelineEndpoint.get(self.workspace_instance,
                                            name=self.azure_pipeline)
            self.pipeline_published = endpoint.get_pipeline()
            self.published_id = self.pipeline_published.id

        except Exception as e:
            raise RuntimeError('published pipeline not found') from e

    def _find_schedule(self):
        if self.old_published_id:
            schedules: List[Schedule] = Schedule.list(
                self.workspace_instance, pipeline_id=self.old_published_id
            )
            if schedules:
                if schedules[0]._pipeline_id == self.old_published_id:
                    self.old_schedule_instance = schedules[0]

    def _disable_old_schedule(self):
        schedule = cast(Schedule, self.old_schedule_instance)
        schedule.disable()

    def _disable_schedulers(self):
        for scheduler in Schedule.get_schedules_for_pipeline_id(
            self.workspace_instance,
            self.published_id
        ):
            scheduler.disable()

    def create_new_timebased_schedule(self):
        """Creates a new schedule."""
        yaml_file = readyml(self.TIMEBASED_SCHEDULE_FILENAME)

        frequency = yaml_file['scheduler']['frequency']
        hours = None
        minutes = None
        week_days = None
        interval = None

        if frequency == 'Minute':
            keys = list(yaml_file['scheduler'].keys())
            if keys == self.LIST_OF_FIELDS['Minute']:
                interval = int(yaml_file['scheduler']['interval'])
            else:
                raise Exception(
                    f"""
                    File is not in the correct format.
                    We are expecting: { self.TEMPLATE_FILE_DICT["Minute"]}
                    """
                )

        if frequency == 'Hour':
            keys = list(yaml_file['scheduler'].keys())
            if keys == self.LIST_OF_FIELDS['Hour']:
                interval = int(yaml_file['scheduler']['interval'])
            else:
                raise Exception(
                    f"""
                    File is not in the correct format.
                    We are expecting: { self.TEMPLATE_FILE_DICT["Hour"]}
                    """
                )

        if frequency == 'Day':
            keys = list(yaml_file['scheduler'].keys())
            if keys == self.LIST_OF_FIELDS['Day']:
                hours = [
                    int(_) for _ in
                    yaml_file['scheduler']['hours'].split(',')
                ]

                minutes = [
                    int(_) for _ in
                    yaml_file['scheduler']['minutes'].split(',')
                ]

                interval = int(
                    yaml_file['scheduler']['interval']
                )
            else:
                raise Exception(
                    f"""
                    File is not in the correct format.
                    We are expecting: { self.TEMPLATE_FILE_DICT["Day"]}
                    """
                )

        if frequency == 'Week':
            keys = list(yaml_file['scheduler'].keys())
            if keys == self.LIST_OF_FIELDS['Week']:

                hours = [
                    int(_) for _ in
                    yaml_file['scheduler']['hours'].split(',')
                ]

                minutes = [
                    int(_) for _ in
                    yaml_file['scheduler']['minutes'].split(',')
                ]

                week_days = list(
                    yaml_file['scheduler']['week_days'].split(',')
                )

                interval = int(yaml_file['scheduler']['interval'])
            else:
                raise Exception(
                    f"""
                    File is not in the correct format.
                    We are expecting: { self.TEMPLATE_FILE_DICT["Week"]}
                    """
                )

        if frequency == 'Month':
            keys = list(yaml_file['scheduler'].keys())
            if keys == self.LIST_OF_FIELDS['Month']:
                interval = int(yaml_file['scheduler']['interval'])
            else:
                raise Exception(
                    f"""
                    File is not in the correct format.
                    We are expecting: { self.TEMPLATE_FILE_DICT["Hour"]}
                    """
                )

        recurrence = ScheduleRecurrence(
            frequency=frequency,
            hours=hours,
            minutes=minutes,
            week_days=week_days,
            start_time=self.AZ_MIN_DATE,
            # starts in the next scheduled datetime
            interval=interval,
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

    def create_new_changebased_schedule(self):
        yaml_file = readyml(self.CHANGEBASED_SCHEDULE_FILENAME)

        datastore_name = yaml_file['scheduler']['datastore_name']
        datastore_path = yaml_file['scheduler']['datastore_path']

        datastore_object = Datastore(workspace=self.workspace_instance, name=datastore_name)

        Schedule.create(
            workspace=self.workspace_instance,
            datastore=datastore_object,
            path_on_datastore=datastore_path,
            pipeline_id=self.published_id,
            name=self.azure_pipeline,
            experiment_name=self.experiment,
            description=self.description,
            continue_on_step_failure=False,
            wait_for_provisioning=True,
            wait_timeout=3600,
        )

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
        # self._find_schedule()

        # print(self.published_id, self.old_schedule_instance)

        # if self.old_schedule_instance:
        #     if self.hour:
        #         self.log('info', 'an old schedule exists, disabling it')
        #     else:
        #         self.log('info', 'schedule already exists, forwarding it')
        #         self._forward_schedule()
        #     self._disable_old_schedule()
        # else:
        self._disable_schedulers()
        self.log('info', 'creating a new schedule')

        if exists(self.TIMEBASED_SCHEDULE_FILENAME) and exists(self.CHANGEBASED_SCHEDULE_FILENAME):
            self.log('error', 'You have files for both time-based and change-based schedule. You can only choose one type of scheduling')
        elif exists(self.TIMEBASED_SCHEDULE_FILENAME):
            self.create_new_timebased_schedule()
        elif exists(self.CHANGEBASED_SCHEDULE_FILENAME):
            self.create_new_changebased_schedule()
        else:
            self.log('error', 'You do not have a schedule file. Cannot operate')
