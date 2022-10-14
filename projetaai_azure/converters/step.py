"""A step for AzureML conversion."""
from __future__ import annotations
from pathlib import Path
import re
import subprocess
from typing import Any, ClassVar, Union
import json as JSON
from dataclasses import dataclass
from kedro_projetaai.utils.script import Step


@dataclass
class ConverterStep(Step):
    """A step for converting local code to AzureML components."""

    resource_group: str
    workspace: str

    CONVERTER_FOLDER: ClassVar[str] = ".kedroazml"
    SOURCE_FOLDER: ClassVar[str] = "src"
    TEMPLATES_FOLDER: ClassVar[str] = str(Path(__file__).parent / "templates")

    @staticmethod
    def _parse_std(std: bytes) -> str:
        return std.decode().replace("\r", "")

    def system(self, *commands: str, json: bool = False) -> Union[None, Any]:
        """Calls system commands.

        Args:
            json (bool, optional): If True, returns the json output of the
                command. Defaults to False.

        Raises:
            SystemExit: if the command fails.

        Returns:
            Union[None, Any]: json output of the command.
        """
        cmdstr = " ".join(commands)
        cmd = subprocess.run(
            cmdstr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err, status = (
            self._parse_std(cmd.stdout),
            self._parse_std(cmd.stderr),
            cmd.returncode,
        )

        if status != 0:
            raise SystemExit(
                f"STDOUT:\n{out}\n\n"
                f"STDERR:\n{err}\n\n"
                f'"{cmdstr}" did not complete'
            )

        print(out)
        if json:
            # parses not json pure json outputs
            match = re.search(r"(\[\n)|(\{\n)", out)
            out = out[match.start(0) :]  # noqa: E203
            return JSON.loads(out)
        else:
            return out

    def azml(self, *commands: str, json: bool = False) -> Union[dict, None]:
        """Calls az ml cli commands.

        Args:
            json (bool, optional): If True, returns the json output of the
                command. Defaults to False.

        Returns:
            Union[dict, None]: json output of the command.
        """
        return self.system(
            "az ml",
            *commands,
            "--resource-group",
            self.resource_group,
            "--workspace",
            self.workspace,
            json=json,
        )
