"""
Creates a job from a pipeline draft.

Note:
    This script was built on top of Azure ML CLI v1.
    Check the reference for it out in this link:
    https://docs.microsoft.com/en-us/cli/azure/ml(v1)?view=azure-cli-latest
"""
from __future__ import print_function, unicode_literals, annotations
from dataclasses import dataclass, field
from projetaai_azure.converters.step import ConverterStep
from azureml.pipeline.core import PipelineDraft
from azureml.core import Workspace


@dataclass
class JobCreator(ConverterStep):
    """Creates a job from a pipeline draft.

    Requires:
        azure_pipeline (str): AzureML pipeline name
    """

    azure_pipeline: str
    workspace_instance: Workspace

    pipeline_id: str = field(init=False)

    def _fetch_draft(self):
        drafts = PipelineDraft.list(self.workspace_instance)
        for draft in drafts:
            if draft.name == self.azure_pipeline:
                self.pipeline_id = draft.id
                break
        else:
            raise RuntimeError('No pipeline draft found to publish')

    def submit(self):
        """Submit the job to AzureML."""
        self.azml(
            'pipeline',
            'submit-draft',
            '--pipeline-draft-id',
            self.pipeline_id,
        )

    def run(self):
        """Creates an AzureML job from a pipeline draft."""
        self._fetch_draft()
        self.submit()
