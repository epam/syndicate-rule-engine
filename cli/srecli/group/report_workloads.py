import click

from srecli.group.report_workloads_detailed import detailed
from srecli.group.report_workloads_summary import summary


@click.group(name='workloads')
def workloads():
    """Describes Kubernetes workloads rule-level reports for pod rules"""


workloads.add_command(detailed)
workloads.add_command(summary)
