import click

from srecli.group import ContextObj, ViewCommand, cli_response


@click.group(name='client')
def client():
    """Manages License Manager Client data"""


@client.command(cls=ViewCommand, name='describe')
@cli_response()
def describe(ctx: ContextObj, customer_id):
    """
    Describe current License Manager client-key data
    """
    return ctx['api_client'].lm_client_setting_get(
        customer_id=customer_id
    )


@client.command(cls=ViewCommand, name='add')
@click.option('--key_id', '-kid',
              type=str, required=True,
              help='Key-id granted by the License Manager.')
@click.option('--algorithm', '-alg', type=str, default='ECC:p521_DSS_SHA:256',
              show_default=True,
              help='Algorithm granted by the License Manager.', required=True)
@click.option('--private_key', '-prk', type=str, required=True,
              help='Private-key granted by the License Manager.')
@click.option('--b64encoded', '-b64', is_flag=True, default=False,
              help='Specify whether the private is b64encoded.')
@cli_response()
def add(ctx: ContextObj, key_id, algorithm, private_key, b64encoded,
        customer_id):
    """
    Adds License Manager provided client-key data
    """
    return ctx['api_client'].lm_client_setting_post(
        key_id=key_id,
        algorithm=algorithm,
        private_key=private_key,
        b64_encoded=b64encoded,
        customer_id=customer_id
    )


@client.command(cls=ViewCommand, name='delete')
@click.option('--key_id', '-kid',
              type=str, required=True,
              help='Key-id granted by the License Manager.')
@cli_response()
def delete(ctx: ContextObj, key_id: str, customer_id):
    """
    Removes current License Manager client-key data
    """
    return ctx['api_client'].lm_client_setting_delete(
        key_id=key_id,
        customer_id=customer_id
    )


client.add_command(describe)
client.add_command(add)
client.add_command(delete)
