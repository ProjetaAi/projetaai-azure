"""Config loader for AzureML runs."""
from typing import Any, Dict, Optional
from kedro.config.templated_config import TemplatedConfigLoader
from azureml.core import Workspace
from projetaai_azure.runners.datastore import DataStore, ServicePrincipal
from projetaai_azure.runners.keyvault import Keyvault
from projetaai_azure.utils.typing import T


class _UnpackDict(dict):
    """Flag dict to unpack values."""

    pass


class AzureConfigLoader(TemplatedConfigLoader):
    """Config loader that accesses AzureML protected data.

    Attributes:
        workspace (Workspace): Azure workspace.
        keyvault (Keyvault): Keyvault wrapper.
        datastore (DataStore): Datastore wrapper.
    """

    DS_PREFIX = "ds::"
    KV_PREFIX = "kv::"

    def __init__(
        self,
        conf_source: str,
        env: str = None,
        runtime_params: Dict[str, Any] = None,
        *,
        base_env: str = "base",
        default_run_env: str = "local",
        globals_pattern: Optional[str] = None,
        globals_dict: Optional[Dict[str, Any]] = None,
        workspace: Workspace = None,
    ):
        """Initializes the AzureConfigLoader.

        Args:
            workspace (Workspace): Azure workspace.
            conf_source (str): Path to the configuration file.
            env (str, optional): Environment. Defaults to None.
            runtime_params (Dict[str, Any], optional): Params from argv.
                Defaults to None.
            base_env (str, optional): Base environment. Defaults to "base".
            default_run_env (str, optional): Personal environment.
                Defaults to "local".
            globals_pattern (Optional[str], optional): Glob pattern to globals
                files. Defaults to None.
            globals_dict (Optional[Dict[str, Any]], optional): Starting dict.
                Defaults to None.
        """
        assert workspace is not None, "Workspace is required"
        super().__init__(
            conf_source,
            env,
            runtime_params,
            base_env=base_env,
            default_run_env=default_run_env,
            globals_pattern=globals_pattern,
            globals_dict=globals_dict,
        )
        self.workspace = workspace
        self.keyvault = Keyvault(workspace)
        self.datastore = DataStore(workspace)

    def _format_kv(self, string: str) -> Any:
        if string.startswith(self.KV_PREFIX):
            secret = string.split(self.KV_PREFIX)[1]
            return self.keyvault[secret]
        return None

    def _format_ds(self, string: str) -> ServicePrincipal:
        if string.startswith(self.DS_PREFIX):
            name = string.split(self.DS_PREFIX)[1]
            return _UnpackDict(**self.datastore[name])
        else:
            return None

    def _format(self, raw: T) -> T:
        if isinstance(raw, dict):
            new_dict = {}
            for k, v in raw.items():
                item = self._format(v)
                if isinstance(item, _UnpackDict):
                    new_dict.update(item)
                else:
                    new_dict[k] = item
            return new_dict
        elif isinstance(raw, list):
            return [self._format(item) for item in raw]
        elif isinstance(raw, str):
            return self._format_kv(raw) or self._format_ds(raw) or raw
        else:
            return raw

    def get(self, *patterns: str) -> Dict[str, Any]:
        """Get the configuration dictionary for the given patterns.

        Returns:
            Dict[str, Any]: Configuration dictionary.
        """
        raw = super().get(*patterns)
        formatted = self._format(raw)
        print(formatted)
        return formatted
