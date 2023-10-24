import json
import os
from pathlib import Path

from c7n.policy import Policy

from helpers import batches
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso

_LOG = get_logger(__name__)


class DumpFindingsPolicy:
    def __init__(self, policy: Policy, output_dir: str):
        self._policy = policy
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def __call__(self, *args, **kwargs):
        resources = self._policy(*args, **kwargs)
        if resources:
            self.create_findings(resources)
        return resources

    def create_findings(self, resources):
        action_registry = self._policy.resource_manager.action_registry
        finding_class = action_registry._factories.get('post-finding')
        if not finding_class:
            # not an aws cloud scan
            return
        finding_instance = finding_class(manager=self._policy.resource_manager)

        finding_instance.data["types"] = [
            "Software and Configuration Checks/Vulnerabilities/CVE"
        ]
        # DEFAULT_BATCH_SIZE = 1  # max is 32
        # # will show only DEFAULT_BATCH_SIZE resource in each finding
        #
        # finding_instance.data['batch_size'] = DEFAULT_BATCH_SIZE

        self.process(finding_instance, resources)

    def process(self, finding_instance, resources):
        try:
            findings = []
            now = utc_iso()
            # default batch size to one to work around security hub console issue
            # which only shows a single resource in a finding.
            batch_size = finding_instance.data.get('batch_size', 32)

            for resource_set in batches(resources, batch_size):
                for key, grouped_resources in \
                        finding_instance.group_resources(resource_set).items():
                    for resource in grouped_resources:
                        finding_id, created_at, updated_at = \
                            self.resolve_id_and_time(finding_instance,
                                                     now,
                                                     key,
                                                     resource)

                        finding = finding_instance.get_finding(
                            [resource], finding_id, created_at, updated_at)

                        findings.append(finding)

            file_path = self.resolve_path_path()

            with open(file_path, 'w') as f:
                json.dump(findings, f, separators=(',', ':'))

        except Exception as e:
            _LOG.warning(f"error with finding '{finding_instance.name}' - {e}")

    @staticmethod
    def resolve_id_and_time(finding_instance, now, key, resource):
        if key == finding_instance.NEW_FINDING:
            finding_id = None
            created_at = now
            updated_at = now
        else:
            try:
                finding_id, created_at = finding_instance.get_finding_tag(
                    resource).split(':', 1)
                updated_at = now
            except Exception as e:
                finding_id, created_at, updated_at = \
                    None, now, now
        return finding_id, created_at, updated_at

    def resolve_path_path(self):
        policy_name = self._policy.data.get('name')
        file_name = f'{policy_name}.json'
        return os.path.join(self._output_dir, file_name)
