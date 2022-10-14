"""
Pushes a pipeline draft to an endpoint or creates it if not found.

Note:
    This script was built on top of Azure ML CLI v1.
    Check the reference for it out in this link:
    https://docs.microsoft.com/en-us/cli/azure/ml(v1)?view=azure-cli-latest
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass, field
from typing import Union
from projetaai_azure.converters.step import ConverterStep

from azureml.core import Workspace
from azureml.pipeline.core import PipelineEndpoint, PublishedPipeline, PipelineDraft


@dataclass
class Publisher(ConverterStep):
    """Pushes a pipeline draft to an endpoint or creates it if not found.

    Requires:
        workspace_instance (Workspace): the workspace instance
        pipeline_id (str): the pipeline draft id
        publish (bool): whether to publish the pipeline draft or not
        azure_pipeline (str): the name of the pipeline draft on AzureML
        description (str): the description of the pipeline

    Outputs:
        published_id (str): the published pipeline id
        old_published_id (str): the id of the previous published pipeline on
            a matching endpoint
    """

    workspace_instance: Workspace

    azure_pipeline: str
    description: str

    published_instance: PublishedPipeline = field(init=False)
    old_published_instance: PublishedPipeline = field(init=False)
    endpoint: PipelineEndpoint = field(init=False)
    published_id: str = field(init=False)
    just_created_endpoint: bool = field(init=False, default=False)
    pipeline_id: str = field(init=False)

    def _fetch_draft(self):
        drafts = PipelineDraft.list(self.workspace_instance)
        for draft in drafts:
            if draft.name == self.azure_pipeline:
                self.pipeline_id = draft.id
                break
        else:
            raise RuntimeError("No pipeline draft found to publish")

    def _publish_draft(self):
        call_json = self.azml(
            "pipeline",
            "publish-draft",
            "--pipeline-draft-id",
            self.pipeline_id,
            json=True,
        )
        self.published_id = call_json["Id"]

    def _instance_published(self):
        self.published_instance = PublishedPipeline.get(
            self.workspace_instance, self.published_id
        )

    def find_existing_endpoint(self) -> Union[PipelineEndpoint, None]:
        """Finds an existing endpoint with the same name as the pipeline.

        Returns:
            Union[PipelineEndpoint, None]: the endpoint or None if not found
        """
        endpoints = [
            endpoint
            for endpoint in PipelineEndpoint.list(self.workspace_instance)
            if endpoint.name == self.azure_pipeline
        ]
        if endpoints:
            self.just_created_endpoint = False
            return endpoints[0]
        else:
            return None

    def create_new_endpoint(self) -> PipelineEndpoint:
        """Creates a new endpoint with the same name as the pipeline.

        Returns:
            PipelineEndpoint: the endpoint
        """
        endpoint = PipelineEndpoint.publish(
            workspace=self.workspace_instance,
            name=self.azure_pipeline,
            pipeline=self.published_instance,
            description=self.description,
        )
        self.just_created_endpoint = True
        return endpoint

    def find_or_create_endpoint(self):
        """Finds or creates an endpoint with the same name as the pipeline."""
        self.endpoint = self.find_existing_endpoint() or self.create_new_endpoint()

    def _instance_old_published(self):
        self.old_published_instance = self.endpoint.get_pipeline()

    def _replace_endpoint_default_pipeline(self):
        self.endpoint.add_default(self.published_instance)

    def run(self) -> dict:
        """Publishes a pipeline draft to an endpoint.

        Returns:
            dict: the published pipeline id and
                the previous published pipeline id.
        """
        self._fetch_draft()
        self._publish_draft()
        self._instance_published()
        self.find_or_create_endpoint()
        output = {"published_id": self.published_id}
        if self.just_created_endpoint:
            self.log("info", "endpoint doesn't exist, creating it")
        else:
            self.log("info", "endpoint already exists, updating it")
            self._instance_old_published()
            self._replace_endpoint_default_pipeline()
            output["old_published_id"] = self.old_published_instance.id
        return output
