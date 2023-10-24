import click

from c7ncli.group.report_errors_accumulated import accumulated
from c7ncli.group.report_errors_jobs import jobs


@click.group(name='errors')
def errors():
    """Describes error reports"""


errors.add_command(jobs)
errors.add_command(accumulated)
