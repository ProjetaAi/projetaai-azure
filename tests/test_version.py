"""Tests if the code version is the same as the pyproject version."""
from projetaai_azure import __version__
import tomli
from pathlib import Path


def test_version():
    """Test that the version is the same in the code and in the pyproject."""
    with (Path(__file__).parent.parent / "pyproject.toml").open("rb") as f:
        pyproject = tomli.load(f)
    assert __version__ == pyproject["tool"]["poetry"]["version"], "Mismatch versions"
