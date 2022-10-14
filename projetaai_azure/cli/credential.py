"""Manages azure credentials."""
import click
from projetaai_azure.utils.constants import CWD
from kedro_projetaai.utils.io import upwriteyml
from kedro_projetaai.utils.iterable import mergedicts


@click.command
@click.option(
    "--name",
    prompt="Credential name (can be anything you want)",
    help="Name of the credential",
)
@click.option(
    "--datastore",
    prompt="Datastore name (to get credentials from)",
    help="Datastore name",
)
@click.option("--account", prompt="Account name", help="Account name")
def credential_create(name: str, datastore: str, account: str):
    """Creates an Azure Blob Gen2 credential."""
    for level in ["base", "local"]:
        filepath = CWD / "conf" / level / "credentials.yml"

        credentials = {
            "azure": {
                "storage": {
                    name: {
                        "account_name": account,
                        "anon": False,
                    }
                }
            }
        }

        if level == "base":
            credentials = mergedicts(
                credentials,
                {
                    "azure": {
                        "storage": {
                            name: {
                                "datastore": f"ds::{datastore}",
                            }
                        }
                    }
                },
            )

        upwriteyml(str(filepath), credentials)
        click.echo(f'Updated "{filepath}"')
