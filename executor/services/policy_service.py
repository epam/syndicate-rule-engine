import json
import os
import tempfile
from pathlib import Path
from typing import Union, Optional, List, Dict, Set
from uuid import uuid4

from ruamel.yaml import YAML
from ruamel.yaml import __with_libyaml__

from helpers.constants import AWS, AZURE, GOOGLE
from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService
from services.ruleset_service import RulesetService
from services.s3_service import S3Service

_LOG = get_logger(__name__)

_LOG.info(f'Initializing YAML object. '
          f'Using CLoader & CDumper: {__with_libyaml__}')
yaml = YAML(typ='safe', pure=False)
yaml.default_flow_style = False
_LOG.info('Yaml object was initialized')


class PolicyService:
    def __init__(self, environment_service: EnvironmentService,
                 s3_service: S3Service, ruleset_service: RulesetService):
        self.environment_service = environment_service
        self.s3_service = s3_service
        self.ruleset_service = ruleset_service

    def assure_event_driven_ruleset(self, cloud: str) -> Path:
        """
        For event-driven scans we use full system event-driven rulesets
        created beforehand. But only rules that are allowed for tenant
        are will be loaded.
        Returns local path to event-driven ruleset loading it in case
        it has not been loaded yet
        """
        assert cloud in {AWS, AZURE, GOOGLE}
        cloud = cloud.upper()
        filename = Path(tempfile.gettempdir(), f'{cloud}.json')
        if filename.exists():
            _LOG.info(f'Event-driven ruleset for cloud {cloud} has already '
                      f'been downloaded. Returning path to it.')
            return filename

        item = self.ruleset_service.get_ed_ruleset(cloud)

        if item:
            data = yaml.load(self._download_ruleset_content(item.get_json()))
        else:
            _LOG.warning(f'Event-driven ruleset item for cloud {cloud} not '
                         f'found in DB. Creating an empty mock')
            data = {'policies': []}
        _LOG.debug(f'Dumping event-driven ruleset for cloud {cloud}')
        with open(filename, 'w') as file:
            json.dump(data, file)
        return filename

    def separate_ruleset(self, from_: Path, work_dir: Path,
                         rules_to_keep: Optional[Set] = None,
                         rules_to_exclude: Optional[Set] = None) -> Path:
        """
        Creates new ruleset file in work_dir filtering the ruleset
        in `from_` variable (keeping and excluding specific rules).
        This is done in order to reduce the size of rule-sets for event-driven
        scans before they are loaded by Custom-Core.
        """
        rules_to_keep = rules_to_keep or set()
        rules_to_exclude = rules_to_exclude or set()
        with open(from_, 'r') as file:
            policies = json.load(file)
        filtered = self.filter_policy_by_rules(
            policies, rules_to_keep, rules_to_exclude
        )
        filename = work_dir / f'{uuid4()}.json'
        with open(filename, 'w') as file:
            json.dump(filtered, file)
        return filename

    def get_policies(self, work_dir, ruleset_list: List[Dict] = None,
                     rules_to_keep: Optional[set] = None,
                     rules_to_exclude: Optional[set] = None) -> list:
        """
        The fewer rules in yaml, the faster Custom Core will cope with it.
        That is why we exclude a lot of rules for event-driven
        :param work_dir: dir to put rule-sets files in
        :param ruleset_list: list of dicts (ruleset DTOs).
         Both licensed rule-sets DTOs (from LM) and standard
        :param rules_to_keep: rules to keep in YAMLs
        :param rules_to_exclude: rules to exclude in YAMLs.
        :return:
        """
        rules_to_keep = rules_to_keep or set()
        rules_to_exclude = rules_to_exclude or set()
        region_dependent_rules_number, region_independent_rules_number = 0, 0

        policy_files = []
        _rules_names = set()
        for ruleset in ruleset_list:
            content = self._download_ruleset_content(ruleset=ruleset)
            if not content:
                continue

            _LOG.info(f'Loading yaml string to dict for '
                      f'ruleset: {ruleset.get("id")}')
            policies = yaml.load(content)
            _LOG.info('Yaml string was loaded.')

            policies = self.filter_policy_by_rules(
                policies, rules_to_keep, rules_to_exclude)
            _to_remove = []
            for rule in policies['policies']:
                _name = rule.get('name')
                if _name in _rules_names:
                    _LOG.warning(f'Duplicated rule \'{_name}\' was found. '
                                 f'Removing from ruleset ')
                    _to_remove.append(rule)
                else:
                    _rules_names.add(_name)
            for rule in _to_remove:
                policies['policies'].remove(rule)

            # dumping to JSON is important here because we use ruamel.yaml
            # (YAML 1.2), Custom-Core uses pyyaml (YAML 1.1). The important
            # difference between them is 1.1 consider "on" and "off" without
            # quotes to be booleans. For 1.2 "on" and "off" are always strings.
            # Some Azure rules contain 'on' and 'off' as strings. When we
            # load them, and dump again using safe C-Dumper, the quotes are
            # removed and then Custom Core thinks that on and off are
            # booleans ... and fails with validation error 'cause it
            # expects strings.
            _filename = os.path.join(work_dir, f'{str(uuid4())}.json')
            with open(_filename, 'w') as file:
                _LOG.info(f'Dumping policies json to JSON file {_filename}')
                json.dump(policies, file)
                _LOG.info('Policies were dumped')
                policy_files.append(_filename)

            for policy in policies['policies']:
                if policy.get('metadata', {}).get('multiregional'):
                    region_independent_rules_number += 1
                else:
                    region_dependent_rules_number += 1

        _LOG.debug(f'Global: {region_independent_rules_number};\n'
                   f'Non-global: {region_dependent_rules_number}\n'
                   f'Policies files: {policy_files}')

        return policy_files

    def _download_ruleset_content(self, ruleset: dict):
        s3_path: Union[str, dict] = ruleset.get('s3_path')
        if not s3_path:
            _LOG.warning(f'There is no path to S3 for ruleset '
                         f'"{ruleset.get("name")}"')
            return
        dispatcher = self._instantiate_ruleset_collector().get(type(s3_path))
        if dispatcher:
            return dispatcher(path=s3_path)

    def _collect_ruleset_content_bucket(self, path: dict):
        """
        Mandates ruleset content collection from a bucket
        and respective key-path.
        :parameter path: dict
        :return: Union[str, Type[None]]
        """
        content = self.s3_service.get_file_content(
            bucket_name=path.get('bucket_name'),
            path=path.get('path')
        )
        return content

    def _collect_ruleset_content_uri(self, path: str):
        """
        Mandates ruleset content collection from a source URI path.
        :parameter path: str
        :return: Union[str, Type[None]]
        """
        return self.ruleset_service.pull_ruleset_content(path, 'utf-8')

    def _instantiate_ruleset_collector(self) -> dict:
        return {
            dict: self._collect_ruleset_content_bucket,
            str: self._collect_ruleset_content_uri
        }

    @staticmethod
    def filter_policy_by_rules(policy: dict, rules_to_keep: set = None,
                               rules_to_exclude: set = None) -> dict:
        """If rules_to_keep is empty, all the rules are kept"""
        rules_to_keep = rules_to_keep or set()
        rules_to_exclude = rules_to_exclude or set()
        result = {'policies': []}
        for rule in policy.get('policies', []):
            name = rule.get('name')
            # version = str(rule.get('metadata', {}).get('version', '1.0'))
            if (rules_to_keep and name not in rules_to_keep) or \
                    (name in rules_to_exclude):
                _LOG.warning(f'Skipping {name}')
                continue
            result['policies'].append(rule)
        return result
