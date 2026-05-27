import click

from srecli.group.integrations_event_sources import sources


@click.group(name="event")
def event():
    """Manages integrations for events"""


event.add_command(sources)
