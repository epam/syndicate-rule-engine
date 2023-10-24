import click

from c7ncli.group.platform_k8s import k8s


@click.group(name='platform')
def platform():
    """
    Manages Custodian Service platforms
    """


platform.add_command(k8s)
