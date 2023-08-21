import click

from c7ncli.group.siem_add import add
from c7ncli.group.siem_delete import delete
from c7ncli.group.siem_describe import describe
from c7ncli.group.siem_update import update


@click.group(name='siem')
def siem():
    """Manages SIEM configuration"""


siem.add_command(add)
siem.add_command(update)
siem.add_command(describe)
siem.add_command(delete)
