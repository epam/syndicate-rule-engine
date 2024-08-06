__version__ = '5.4.1'

import sys
from distutils.version import LooseVersion

# todo rewrite
def check_version_compatibility(api_version):
    if not api_version:
        print('Custodian API did not return the version number!')
        return
    cli_version = LooseVersion(__version__)
    api_version = LooseVersion(api_version)
    if cli_version > api_version:
        print(f'Consider that you SRE CLI version {cli_version} is '
              f'higher than the API version {api_version}',
              file=sys.stderr)
        return
    if cli_version.version[0] < api_version.version[0]:  # Major
        print(f'CLI Major version {cli_version} is lower than '
              f'the API version {api_version}. Please, update the CLI',
              file=sys.stderr)
        sys.exit(1)
    if cli_version.version[1] < api_version.version[1]:  # Minor
        print(f'CLI Minor version {cli_version} is lower than the '
              f'API version {api_version}. Some features may not '
              f'work. Consider updating the SRE CLI',
              file=sys.stderr)
