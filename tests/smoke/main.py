import argparse
import sys
from pathlib import Path

from smoke.cases.main_flow import TenantRegionsType, run_main_flow
from smoke.cases.rules_management import run_rules_management
from smoke.core.commons import Case
from smoke.core.settings import get_settings


def _exit_code(cases: list[Case]) -> int:
    return 0 if all(case.succeeded for case in cases) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Syndicate Rule Engine smoke tests',
    )
    subparsers = parser.add_subparsers(
        dest='suite',
        required=True,
    )

    main_flow = subparsers.add_parser(
        'main_flow',
        help='Describe entities and optionally submit scan jobs',
    )
    settings = get_settings()
    main_flow.add_argument(
        '--username',
        default=settings.username,
        type=str,
        help='SRE username (default: SMOKE_SRE_USERNAME)',
    )
    main_flow.add_argument(
        '--password',
        default=settings.password,
        type=str,
        help='SRE password (default: SMOKE_SRE_PASSWORD)',
    )
    main_flow.add_argument(
        '--api_link',
        default=settings.api_link,
        type=str,
        help='SRE API link (default: SMOKE_SRE_API_LINK)',
    )
    main_flow.add_argument(
        '--tenants',
        nargs='+',
        required=True,
        type=TenantRegionsType(),
        help='Tenant to list of regions: '
        '--tenants EOOS:eu-central-1,eu-west-1 CIT2:eu-west-1',
    )
    main_flow.add_argument(
        '--customer',
        default=settings.customer,
        required=True,
        type=str,
        help='Customer name for -cid (default: SMOKE_SRE_CUSTOMER)',
    )

    def markdown(value: str) -> Path:
        if not value.endswith('.md'):
            value = value + '.md'
        return Path(value)

    main_flow.add_argument(
        '--filename',
        required=False,
        type=markdown,
        help='Output markdown report file',
    )

    rules_management = subparsers.add_parser(
        'rules_management',
        help='Rules and rulesets management flow',
    )
    rules_management.add_argument(
        '--filename',
        default='smoke-rules-management.md',
        type=str,
        help='Output markdown report file',
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.suite == 'main_flow':
        if not all(
            (args.username, args.password, args.api_link, args.customer)
        ):
            parser.error(
                'username, password, api_link and customer must be provided '
                'via CLI flags or SMOKE_SRE_* environment variables'
            )
        cases = run_main_flow(
            username=args.username,
            password=args.password,
            api_link=args.api_link,
            tenants=args.tenants,
            customer=args.customer,
            filename=args.filename,
        )
        sys.exit(_exit_code(cases))
        return

    if args.suite == 'rules_management':
        cases = run_rules_management(report_name=args.filename)
        sys.exit(_exit_code(cases))
        return

    parser.error(f'Unknown suite: {args.suite}')


if __name__ == '__main__':
    main()
