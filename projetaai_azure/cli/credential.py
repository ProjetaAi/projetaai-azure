"""Manages azure credentials."""
import click

from projetaai_azure.utils.constants import CWD
from projetaai_azure.utils.io import readyml, writeyml


@click.command
@click.option('--name', prompt='Credential name (can be anything you want)',
              help='Name of the credential')
@click.option('--datastore', prompt='Datastore name (to get credentials from)',
              help='Datastore name')
@click.option('--account', prompt='Account name', help='Account name')
def credential_create(name: str, datastore: str, account: str):
    """Creates an Azure Blob Gen2 credential."""
    for level in ['base', 'local']:
        filepath = str(CWD / 'conf' / level / 'credentials.yml')
        try:
            credentials = readyml(filepath)
            credentials['azure'] = credentials.get('azure', {})
            credentials['azure']['storage'] = credentials['azure'].get(
                'storage', {})
        except Exception:
            credentials = {'azure': {'storage': {}}}

        upper_name = name.upper()
        credentials['azure']['storage'][name] = {
            **({'datastore': datastore} if level == 'base' else {}),
            'credential': {
                'account_name': account,
                'anon': False,
                **(
                    {
                        'client_id': '${%s_CLIENT_ID}' % upper_name,
                        'client_secret': '${%s_CLIENT_SECRET}' % upper_name,
                        'tenant_id': '${%s_SUBSCRIPTION_ID}' % upper_name,
                    }
                    if level == 'base' else {}
                )
            }
        }

        writeyml(filepath, credentials)
        click.echo(f'Updated "{filepath}"')
