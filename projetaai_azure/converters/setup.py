"""Setup the AzureML tools."""
import traceback
from azure.cli.core.extension.operations import (
    add_extension,
    get_extension,
    ExtensionNotInstalledException,
)
from azure.cli.core import AzCli
import click


def install_azml_cli():
    """Install the Azure ML CLI extension."""
    try:
        ext = "azure-cli-ml"
        try:
            get_extension(ext)
            return
        except ExtensionNotInstalledException:
            click.secho(
                "Azure ML CLI extension not installed. Installing...", fg="yellow"
            )
            add_extension(
                extension_name="azure-cli-ml",
                cli_ctx=AzCli(),
            )
            click.secho("Azure ML CLI extension installed.", fg="green")
    except Exception:
        click.secho(
            "Error installing Azure ML CLI extension: \n\n" f"{traceback.format_exc()}",
            fg="red",
        )
        exit(1)
