import click

from c7ncli.group.integrations_dojo import dojo
from c7ncli.group.integrations_sre import sre


@click.group(name='integrations')
def integrations():
    """Manages Custodian Service Integrations"""


integrations.add_command(dojo)
integrations.add_command(sre)
