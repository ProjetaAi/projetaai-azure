"""Package for storing constants."""
import os

import click
from projetaai_azure.utils.string import UPath


CWD = UPath(os.getcwd())

CLICK_TYPEMAP = {
    str: click.STRING,
    int: click.INT,
    float: click.FLOAT,
    bool: click.BOOL,
}
