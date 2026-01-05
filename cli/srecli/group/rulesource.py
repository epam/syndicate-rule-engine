import click

from srecli.group import (
    cli_response,
    ViewCommand,
    ContextObj,
    build_rule_source_id_option,
    next_option,
    limit_option,
)
from srecli.service.adapter_client import SREResponse

attributes_order = (
    "id",
    "type",
    "description",
    "git_project_id",
    "git_url",
    "git_ref",
    "git_rules_prefix",
)


def git_project_id_option(required: bool = False):
    return click.option(
        "--git_project_id",
        "-gpid",
        type=str,
        required=required,
        help="GitLab project id or GitHub owner/repo",
    )


def type_option(required: bool = False):
    return click.option(
        "--type",
        "-t",
        type=click.Choice(("GITHUB", "GITLAB", "GITHUB_RELEASE")),
        required=required,
        help="Rule source type",
    )


def git_url_option(required: bool = False):
    return click.option(
        "--git_url",
        "-gurl",
        type=str,
        required=required,
        help="API endpoint of a Git-based platform",
    )


def git_ref_option(required: bool = False, default: str | None = None):
    return click.option(
        "--git_ref",
        "-gref",
        type=str,
        required=required,
        default=default,
        show_default=default is not None,
        help="Name of the branch to grab rules from",
    )


def git_rules_prefix_option(required: bool = False, default: str | None = None):
    return click.option(
        "--git_rules_prefix",
        "-gprefix",
        type=str,
        required=required,
        default=default,
        show_default=default is not None,
        help="Rules path prefix",
    )


def git_access_secret_option(required: bool = False):
    return click.option(
        "--git_access_secret",
        "-gsecret",
        type=str,
        required=required,
        help="Secret token to access the repository",
    )


def description_option(required: bool = False):
    return click.option(
        "--description",
        "-d",
        type=str,
        required=required,
        help="Human-readable description of the repo",
    )


@click.group(name="rulesource")
def rulesource():
    """Manages Rule Source entity"""


@rulesource.command(cls=ViewCommand, name="describe")
@build_rule_source_id_option(required=False)
@git_project_id_option()
@type_option()
@click.option(
    "--has_secret",
    "-hs",
    type=bool,
    help="Specify whether returned rule sources should have secrets",
)
@limit_option
@next_option
@cli_response(attributes_order=attributes_order)
def describe(
    ctx: ContextObj,
    rule_source_id,
    git_project_id,
    type,
    limit,
    next_token,
    has_secret,
    customer_id,
):
    """Describes rule source"""
    if rule_source_id:
        return ctx["api_client"].rule_source_get(
            rule_source_id, customer_id=customer_id
        )
    return ctx["api_client"].rule_source_query(
        git_project_id=git_project_id,
        type=type,
        limit=limit,
        next_token=next_token,
        has_secret=has_secret,
        customer_id=customer_id,
    )


@rulesource.command(cls=ViewCommand, name="add")
@git_project_id_option(required=True)
@type_option()
@git_url_option()
@git_ref_option(default="main")
@git_rules_prefix_option(default="/")
@git_access_secret_option()
@description_option(required=True)
@cli_response(attributes_order=attributes_order)
def add(
    ctx: ContextObj,
    git_project_id,
    type,
    git_url,
    git_ref,
    git_rules_prefix,
    git_access_secret,
    description,
    customer_id,
):
    """Creates rule source"""
    return ctx["api_client"].rule_source_post(
        git_project_id=git_project_id,
        type=type,
        git_url=git_url,
        git_ref=git_ref,
        git_rules_prefix=git_rules_prefix,
        git_access_secret=git_access_secret,
        description=description,
        customer_id=customer_id,
    )


@rulesource.command(cls=ViewCommand, name="update")
@build_rule_source_id_option(required=True)
@git_project_id_option()
@type_option()
@git_url_option()
@git_ref_option()
@git_rules_prefix_option()
@git_access_secret_option()
@description_option()
@cli_response(attributes_order=attributes_order)
def update(
    ctx: ContextObj,
    rule_source_id,
    git_project_id,
    type,
    git_url,
    git_ref,
    git_rules_prefix,
    git_access_secret,
    description,
    customer_id,
):
    """Updates rule source"""
    if not any(
        [
            git_project_id,
            type,
            git_url,
            git_ref,
            git_rules_prefix,
            git_access_secret,
            description,
        ]
    ):
        raise click.ClickException("At least one parameter must be given to update")
    return ctx["api_client"].rule_source_patch(
        id=rule_source_id,
        git_project_id=git_project_id,
        type=type,
        git_url=git_url,
        git_ref=git_ref,
        git_rules_prefix=git_rules_prefix,
        git_access_secret=git_access_secret,
        description=description,
        customer_id=customer_id,
    )


@rulesource.command(cls=ViewCommand, name="delete")
@build_rule_source_id_option(required=True)
@click.option(
    "--delete_rules",
    "-dr",
    is_flag=True,
    help="Whether to remove all rules belonging to this rule source",
)
@cli_response()
def delete(ctx: ContextObj, rule_source_id, delete_rules, customer_id):
    """
    Deletes rule source
    """
    return ctx["api_client"].rule_source_delete(
        id=rule_source_id,
        delete_rules=delete_rules,
        customer_id=customer_id,
    )


@rulesource.command(cls=ViewCommand, name="sync")
@build_rule_source_id_option(required=True)
@cli_response(
    hint=lambda rule_source_id, **kwargs: (
        f"Use 'sre rulesource describe -rsid {rule_source_id}' to check status."
    ),
)
def sync(
    ctx: ContextObj,
    rule_source_id: str,
    customer_id: str | None,
) -> SREResponse:
    """
    Updates rules for this rule source
    """
    return ctx["api_client"].rule_source_sync(
        id=rule_source_id,
        customer_id=customer_id,
    )
