import click

from srecli.group.integrations_dojo import dojo
from srecli.group.integrations_sre import sre
from srecli.group.integrations_chronicle import chronicle


@click.group(name='integrations')
def integrations():
    """Manages Custodian Service Integrations"""


integrations.add_command(dojo)
integrations.add_command(sre)
integrations.add_command(chronicle)
