import click

from srecli.group.integrations_dojo import dojo
from srecli.group.integrations_re import re
from srecli.group.integrations_chronicle import chronicle


@click.group(name='integrations')
def integrations():
    """Manages Custodian Service Integrations"""


integrations.add_command(dojo)
integrations.add_command(re)
integrations.add_command(chronicle)
