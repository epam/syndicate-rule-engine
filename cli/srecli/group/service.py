import click

from srecli.group.service_operations import operations

@click.group(name='service')
def service():
    """Manages Services"""

service.add_command(operations)