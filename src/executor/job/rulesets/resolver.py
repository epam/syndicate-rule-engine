"""Ruleset resolution for job execution (standard and licensed)."""

from typing import Generator, Iterable

from helpers.log_helper import get_logger
from services import SP
from services.reports_bucket import RulesetsBucketKeys
from services.ruleset_service import RulesetName

_LOG = get_logger(__name__)


def resolve_standard_ruleset(
    customer_name: str, ruleset: RulesetName
) -> tuple[RulesetName, list[dict]] | None:
    rs = SP.ruleset_service
    if v := ruleset.version:
        item = rs.get_standard(
            customer=customer_name, name=ruleset.name, version=v.to_str()
        )
    else:
        item = rs.get_latest(customer=customer_name, name=ruleset.name)
    if not item:
        _LOG.warning(f'Somehow ruleset does not exist: {ruleset}')
        return
    content = rs.fetch_content(item)
    if not content:
        _LOG.warning(f'Somehow ruleset does not have content: {ruleset}')
        return
    return RulesetName(
        ruleset.name, ruleset.version.to_str() if ruleset.version else None
    ), content.get('policies') or []


def resolve_licensed_ruleset(
    customer_name: str, ruleset: RulesetName
) -> tuple[RulesetName, list[dict]] | None:
    s3 = SP.s3
    if v := ruleset.version:
        content = s3.gz_get_json(
            bucket=SP.environment_service.get_rulesets_bucket_name(),
            key=RulesetsBucketKeys.licensed_ruleset_key(
                ruleset.name, v.to_str()
            ),
        )
        if not content:
            _LOG.warning(f'Content of {ruleset} does not exist')
            return
        return ruleset, content.get('policies', [])
    item = SP.ruleset_service.get_licensed(name=ruleset.name)
    if not item:
        _LOG.warning(f'Ruleset {ruleset} does not exist')
        return
    content = s3.gz_get_json(
        bucket=SP.environment_service.get_rulesets_bucket_name(),
        key=RulesetsBucketKeys.licensed_ruleset_key(
            ruleset.name, item.latest_version
        ),
    )
    if not content:
        _LOG.warning(f'Content of {ruleset} does not exist')
        return
    return RulesetName(
        ruleset.name, item.latest_version, ruleset.license_key
    ), content.get('policies') or []


def resolve_job_rulesets(
    customer_name: str, rulesets: Iterable[RulesetName]
) -> Generator[tuple[RulesetName, list[dict]], None, None]:
    for rs in rulesets:
        if rs.license_key:
            resolver = resolve_licensed_ruleset
        else:
            resolver = resolve_standard_ruleset
        result = resolver(customer_name, rs)
        if result is None:
            continue
        yield result
