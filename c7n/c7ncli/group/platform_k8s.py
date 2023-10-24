from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, build_tenant_option, \
    ContextObj


@click.group(name='k8s')
def k8s():
    """Manages Kubernetes Platform configuration """


@k8s.command(cls=ViewCommand, name='create_native')
@build_tenant_option(required=True)
@click.option('-n', '--name', type=str, required=True, help='Cluster name')
@click.option('-e', '--endpoint', type=str, required=True, help='K8s endpoint')
@click.option('-ca', '--certificate_authority', type=str, required=True,
              help='Certificate authority base64 encoded string')
@click.option('-t', '--token', type=str, required=False,
              help='Long lived token. Short-lived tokens will '
                   'be generated base on this one')
@click.option('-d', '--description', type=str, required=False,
              help='Eks platform description')
@cli_response()
def create_native(ctx: ContextObj, tenant_name: str, name: str, endpoint: str,
                  certificate_authority: str, token: Optional[str],
                  description: Optional[str]):
    return ctx['api_client'].platform_k8s_create_native(
        tenant_name=tenant_name,
        name=name,
        endpoint=endpoint,
        certificate_authority=certificate_authority,
        token=token,
        description=description
    )


@k8s.command(cls=ViewCommand, name='create_eks')
@build_tenant_option(required=True)
@click.option('-n', '--name', type=str, required=True, help='Cluster name')
@click.option('-r', '--region', type=str, required=True,
              help='AWS region where eks cluster is situated')
@click.option('-aid', '--application_id', type=str, required=True,
              help='ID of application with AWS credentials that have '
                   'access to the cluster')
@click.option('-d', '--description', type=str, required=False,
              help='Eks platform description')
@cli_response()
def create_eks(ctx: ContextObj, tenant_name: str, name: str, region: str,
               application_id: str, description: Optional[str]):
    return ctx['api_client'].platform_eks_create_native(
        tenant_name=tenant_name,
        name=name,
        region=region,
        application_id=application_id,
        description=description,
    )


@k8s.command(cls=ViewCommand, name='describe')
@build_tenant_option()
@cli_response()
def describe(ctx: ContextObj, tenant_name: Optional[str]):
    return ctx['api_client'].platform_k8s_list(
        tenant_name=tenant_name,
    )


@k8s.command(cls=ViewCommand, name='delete_native')
@click.option('-pid', '--platform_id', type=str, required=True,
              help='Platform id')
@cli_response()
def delete_native(ctx: ContextObj, platform_id: str):
    return ctx['api_client'].platform_k8s_delete_native(platform_id)


@k8s.command(cls=ViewCommand, name='delete_eks')
@click.option('-pid', '--platform_id', type=str, required=True,
              help='Platform id')
@cli_response()
def delete_eks(ctx: ContextObj, platform_id: str):
    return ctx['api_client'].platform_k8s_delete_eks(platform_id)
