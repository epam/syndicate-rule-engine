import argparse
import sys
from pathlib import Path

from smoke.cases.main_flow import TenantRegionsType, run_main_flow
from smoke.cases.rules_management import run_rules_management
from smoke.core.commons import Case, set_debug
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

    settings = get_settings()

    common_args = argparse.ArgumentParser(add_help=False)
    common_args.add_argument(
        '--username',
        default=settings.username,
        type=str,
        help='SRE username (default: SMOKE_SRE_USERNAME)',
    )
    common_args.add_argument(
        '--password',
        default=settings.password,
        type=str,
        help='SRE password (default: SMOKE_SRE_PASSWORD)',
    )
    common_args.add_argument(
        '--api_link',
        default=settings.api_link,
        type=str,
        help='SRE API link (default: SMOKE_SRE_API_LINK)',
    )
    common_args.add_argument(
        '--customer',
        default=settings.customer,
        type=str,
        help='Customer name for -cid (default: SMOKE_SRE_CUSTOMER)',
    )
    common_args.add_argument(
        '--debug',
        action='store_true',
        help='Log raw stdout/stderr and exit code of each CLI command',
    )

    main_flow = subparsers.add_parser(
        'main_flow',
        help='Describe entities and optionally submit scan jobs',
        parents=[common_args],
    )
    main_flow.add_argument(
        '--tenants',
        nargs='+',
        required=True,
        type=TenantRegionsType(),
        help='Tenant to list of regions: '
        '--tenants EOOS:eu-central-1,eu-west-1 CIT2:eu-west-1',
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
        parents=[common_args],
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

    set_debug(args.debug)

    if not all(
        (args.username, args.password, args.api_link, args.customer)
    ):
        parser.error(
            'username, password, api_link and customer must be provided '
            'via CLI flags or SMOKE_SRE_* environment variables'
        )

    if args.suite == 'main_flow':
        if not args.tenants:
            parser.error('Tenants must be provided via --tenants or SMOKE_SRE_TENANTS environment variable')
        tenants = [tenant for group in args.tenants for tenant in group]
        cases = run_main_flow(
            username=args.username,
            password=args.password,
            api_link=args.api_link,
            tenants=tenants,
            customer=args.customer,
            filename=args.filename,
        )
        sys.exit(_exit_code(cases))
        return

    if args.suite == 'rules_management':
        cases = run_rules_management(
            username=args.username,
            password=args.password,
            api_link=args.api_link,
            customer=args.customer,
            report_name=args.filename,
        )
        sys.exit(_exit_code(cases))
        return

    parser.error(f'Unknown suite: {args.suite}')


if __name__ == '__main__':
    main()
