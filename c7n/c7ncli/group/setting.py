import click

from c7ncli.group.setting_lm import lm
from c7ncli.group.setting_mail import mail


@click.group(name='setting')
def setting():
    """Manages Custodian Service Settings"""


setting.add_command(mail)
setting.add_command(lm)
