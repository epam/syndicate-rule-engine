from typing import Optional

import click

from srecli.group import ContextObj, ViewCommand, build_tenant_option, cli_response


@click.group(name='k8s')
def k8s():
    """Manages kubernetes platforms"""


@k8s.command(cls=ViewCommand, name='create')
@build_tenant_option(required=True)
@click.option('-n', '--name', type=str, required=True, help='Cluster name')
@click.option('-t', '--type', required=True,
              type=click.Choice(('SELF_MANAGED', 'EKS', 'AKS', 'GKS')),
              help='Cluster type')
@click.option('-r', '--region', type=str, required=False,
              help='Cluster region in case the cluster is bound to a cloud')
@click.option('-d', '--description', type=str, required=False,
              help='Eks platform description')
@click.option('-e', '--endpoint', type=str, required=False,
              help='K8s endpoint')
@click.option('-ca', '--certificate_authority', type=str, required=False,
              help='Certificate authority base64 encoded string')
@click.option('-T', '--token', type=str, required=False,
              help='Long lived token. Short-lived tokens will '
                   'be generated base on this one')
@cli_response()
def create(ctx: ContextObj, tenant_name: str, name: str, region: Optional[str],
           description: Optional[str], type: str, endpoint: Optional[str],
           certificate_authority: Optional[str], token: Optional[str],
           customer_id):
    """
    Register a new K8S Platform within a tenant
    """
    return ctx['api_client'].platform_k8s_create(
        tenant_name=tenant_name,
        name=name,
        region=region,
        type=type,
        description=description,
        endpoint=endpoint,
        certificate_authority=certificate_authority,
        token=token,
        customer_id=customer_id
    )


@k8s.command(cls=ViewCommand, name='describe')
@build_tenant_option()
@cli_response()
def describe(ctx: ContextObj, tenant_name: Optional[str], customer_id):
    """
    List registered K8S platforms
    """
    return ctx['api_client'].platform_k8s_list(
        tenant_name=tenant_name,
        customer_id=customer_id
    )


@k8s.command(cls=ViewCommand, name='delete')
@click.option('-pid', '--platform_id', type=str, required=True,
              help='Platform id')
@cli_response()
def delete(ctx: ContextObj, platform_id: str, customer_id):
    """
    Deregister a platform
    """
    return ctx['api_client'].platform_k8s_delete(platform_id,
                                                 customer_id=customer_id)
