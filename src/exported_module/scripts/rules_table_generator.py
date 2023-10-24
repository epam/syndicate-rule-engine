"""
Example:
    python3 table_generator.py --dir_to_rules=/path/to/custodian-epam-cloud --excel_name my_ruleset
"""

import argparse
import os
from pathlib import Path
from typing import Dict, List

RulesData = Dict[str, List[Dict]]


def get_policy_data(policy: dict) -> dict:
    meta = policy.get('metadata')
    if not meta:
        return {}
    cloud = meta.get('cloud')
    standards = {}
    for st in meta.get('standard', {}).items():
        standards.update({st[0]: '\n'.join(st[1])})
    return {
        'Name': policy.get('name', '').lower(),
        'Description': policy.get('description'),
        'Source': meta.get('source'),
        'Article': meta.get('article'),
        'Service Section': meta.get('service_section'),
        'Remediation': meta.get('remediation'),
        '_cloud': cloud,
        **standards
    }


def read_yaml_files(directory: Path) -> RulesData:
    from ruamel.yaml import YAML
    yaml = YAML(typ='safe', pure=False)
    yaml.default_flow_style = False
    data = {}
    for root, dirs, files in os.walk(directory):
        for file in filter(lambda x: x.endswith('.yml') or x.endswith('.yaml'),
                           files):
            filename = os.path.join(root, file)
            print(f'Processing {filename}')
            with open(filename) as fp:
                policies = yaml.load(fp).get('policies') or []
                for policy in policies:
                    policy_data = get_policy_data(policy)
                    _cloud = policy_data.pop('_cloud')
                    data.setdefault(_cloud, []).append(policy_data)
    for cloud, rules in data.items():
        data[cloud] = sorted(rules, key=lambda x: x['Name'])
    return data


def create_excel(data: RulesData, to: Path):
    import pandas as pd
    print('Creating excel file...')
    writer = pd.ExcelWriter(to, engine='xlsxwriter')
    for cloud, data in data.items():
        df = pd.DataFrame(data)
        df.to_excel(writer, sheet_name=cloud, index=False)
    writer.save()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Automatically generates ruleset '
                    'table with all rules info')
    init_parser(parser)
    return parser


def init_parser(parser: argparse.ArgumentParser):
    def xlsx_file_type(value: str) -> str:
        if not value.endswith('.xlsx'):
            value += '.xlsx'
        return value

    parser.add_argument(
        '--dir_to_rules', type=Path, required=True,
        help='Path to the directory with rules (yaml files)')
    parser.add_argument(
        '--output_dir', type=Path, required=False,
        default=Path.cwd(),
        help='Path to store the result excel. Current directory by default.'
    )
    parser.add_argument(
        '--excel_name', type=xlsx_file_type, required=False,
        default='rules', help='By default: (default: %(default)s)'
    )


def main(dir_to_rules: Path, output_dir: Path, excel_name: str):
    if not dir_to_rules.exists():
        raise FileNotFoundError(f'Directory with rules do not exists: '
                                f'{dir_to_rules}')
    output_dir.mkdir(parents=True, exist_ok=True)
    create_excel(data=read_yaml_files(directory=dir_to_rules),
                 to=output_dir / excel_name)


if __name__ == '__main__':
    main(**vars(build_parser().parse_args()))
