"""Extended setup.cfg configuration."""
import subprocess
from setuptools import setup
from setuptools.command.install import install


class CLISetup(install):
    """Setups cli."""

    def run(self):
        """Run the hook."""
        super().run()
        print('Installing AzureML CLI')
        subprocess.run(['az', 'extension', 'add', '--name', 'azure-cli-ml'])
        print('Installed AzureML CLI')


setup(
    cmdclass={
        'install': CLISetup
    }
)
