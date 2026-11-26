import click

from srecli.group.report_rbac_detailed import detailed
from srecli.group.report_rbac_summary import summary


@click.group(name='rbac')
def rbac():
    """Describes Kubernetes RBAC reports"""


rbac.add_command(detailed)
rbac.add_command(summary)
