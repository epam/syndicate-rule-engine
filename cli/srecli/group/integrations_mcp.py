import click

from srecli.group.integrations_mcp_auth import auth


@click.group(name='mcp')
def mcp():
    """
    Manages MCP integrations
    """


mcp.add_command(auth)
