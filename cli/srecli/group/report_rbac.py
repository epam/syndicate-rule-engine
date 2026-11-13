import click

from srecli.group.report_rbac_compact import compact
from srecli.group.report_rbac_detailed import detailed


@click.group(name='rbac')
def rbac():
    """Generate Kubernetes RBAC reports"""


rbac.add_command(detailed)
rbac.add_command(compact)
