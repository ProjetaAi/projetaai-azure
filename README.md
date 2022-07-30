# ProjetaAi Azure
ProjetaAi plugin to enable Kedro integration with Azure services.

## Wip

- Credential assignment
- Batch endpoint
- Realtime endpoint

## Usable

- Blob Gen2 credential registration: `kedro credential create azure`
    - Datastore credential retrivial when running a pipeline in AzureML
- Pipeline conversion: `kedro pipeline create azure`
    - Environment creation (gets updated automatically)
    - Drafts gets updated too
- Scheduling (weekly): `kedro pipeline azure schedule`
    - Requires pipeline to be published
    - Updates current schedule if exists
- Draft publishing: `kedro pipeline azure publish`
    - Updates endpoint default pipeline after first call
    - Forwards schedules if current published exists

> Use `--help` with any of these commands in order to know what arguments are required
