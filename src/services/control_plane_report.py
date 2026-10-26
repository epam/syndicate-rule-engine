import re
from typing import Literal

from helpers.log_helper import get_logger
from services.license_service import LicenseService
from services.metadata import Metadata
from services.sharding import ShardsCollection

_LOG = get_logger(__name__)

SECTIONS = ('API Server', 'Controller Manager', 'etcd', 'Scheduler')


RULE_ID_REGEX = re.compile(r'^ecc-(k8s-\d+)')


def extract_rule_id(rule_name: str) -> int | None:
    match = RULE_ID_REGEX.match(rule_name)
    if not match:
        return None
    return int(match.group(1))


class ControlPlaneReportError(ValueError):
    pass


class ControlPlaneReportService:
    """Build structured Kubernetes Control Plane findings reports."""

    def __init__(self, license_service: LicenseService) -> None:
        self._license_service = license_service

    def generate_report(
        self,
        collection: ShardsCollection,
        customer_name: str,
        fmt: Literal['md', 'json'],
    ) -> dict | str:
        """Build a report in the requested format for a scan collection."""
        metadata = self._license_service.get_customer_metadata(customer_name)
        report = self._build_report(collection, metadata)
        if fmt == 'json':
            return report
        return self._render_markdown(report)

    @staticmethod
    def _build_report(
        collection: ShardsCollection, metadata: Metadata
    ) -> dict:
        """Group numbered Kubernetes rules and resources into report sections."""
        rules_by_section = {section: [] for section in SECTIONS}

        for part in collection.iter_all_parts():
            rule_name = part.policy
            match = RULE_ID_REGEX.match(part.policy)
            if not match:
                continue

            rule = part.policy
            rule_scan_metadata = collection.meta.get(rule)
            if not rule_scan_metadata:
                _LOG.warning('Missing scan metadata for rule %s', rule)
                continue

            rule_metadata = metadata.rules.get(rule)
            if not rule_metadata:
                _LOG.warning('Missing metadata for rule %s', rule)
                continue

            section = rule_metadata.service_section
            if section not in SECTIONS:
                continue

            resources = []
            for resource in part.resources:
                meta = resource.get("metadata", {})
                spec = resource.get("spec", {})
                status = resource.get("status", {})
                resources.append(
                    {
                        "name": meta.get("name", ""),
                        "namespace": meta.get("namespace", ""),
                        "node_name": spec.get("node_name", ""),
                        "host_ip": status.get("host_ip", ""),
                        "phase": status.get("phase", ""),
                    }
                )

            check = {
                'rule_id': match.group(1),
                'title': rule_scan_metadata['description'],
                'rule_name': rule_name or rule,
                'severity': rule_metadata.severity.value,
                'resources': resources,
            }

            rules_by_section[section].append(check)

        for _, value in rules_by_section.items():
            value.sort(key=lambda x: x['rule_id'])

        return rules_by_section

    def _render_markdown(self, report: dict) -> str:
        """Render the structured report as Markdown with plain-text tables."""

        rendered_sections = []
        for section, checks in report.items():
            rendered_checks = []
            for idx, check in enumerate(checks, start=1):
                resources = self._resource_table(check['resources'])
                rendered_checks.append(
                    '\n'.join(
                        (
                            f'## Check {idx}',
                            '',
                            f'Title: {check["title"]}',
                            f'Rule ID: {check["rule_id"]}',
                            f'Severity: {check["severity"]}',
                            '',
                            '### Resources',
                            '',
                            resources,
                            '',
                            '---',
                        )
                    )
                )
            rendered_sections.append(
                '\n\n'.join((f'# {section}', *rendered_checks))
            )
        return '\n\n'.join(rendered_sections)

    @staticmethod
    def _resource_table(resources: list[dict]) -> str:
        """Format affected resources as an aligned plain-text table."""
        if not resources:
            return 'No resources found'

        headers = ('Pod name', 'namespace', 'node_name', 'host_ip', 'phase')
        rows = [
            (
                resource['name'] or '-',
                resource['namespace'] or '-',
                resource['node_name'] or '-',
                resource['host_ip'] or '-',
                resource['phase'] or '-',
            )
            for resource in resources
        ]
        widths = [
            max(len(header), *(len(row[index]) for row in rows))
            for index, header in enumerate(headers)
        ]
        lines = (
            '  '.join(
                header.ljust(widths[index])
                for index, header in enumerate(headers)
            ),
            '  '.join('-' * width for width in widths),
            *(
                '  '.join(
                    value.ljust(widths[index])
                    for index, value in enumerate(row)
                )
                for row in rows
            ),
        )
        return '\n'.join(line.rstrip() for line in lines)
