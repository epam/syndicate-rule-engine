"""Tests for JobEventsService save/load."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from services.event_driven.events.service import JobEventsService
from services.metadata import Metadata, RuleMetadata


def _resource_scoped_meta() -> RuleMetadata:
    return RuleMetadata(
        cloud='aws',
        source='ecc',
        category='c',
        service_section='s',
        service='iam',
        article='',
        impact='',
        remediation='',
        events=[
            {'event': 'EnableMFADevice', 'source': 'iam.amazonaws.com', 'ids': 'x'},
        ],
    )


class TestJobEventsService:
    def test_save_writes_manifest_and_rule_payload(self) -> None:
        s3 = MagicMock()
        s3.gz_get_json.return_value = None
        svc = JobEventsService(s3_client=s3)
        tenant = SimpleNamespace(
            name='t1',
            customer_name='cust',
            cloud='aws',
            project='proj',
        )
        job = SimpleNamespace(
            id='job-1',
            tenant_name='t1',
            platform_id=None,
            job_type='reactive',
            rules_to_scan=['rule-a'],
            submitted_at='2024-01-15T12:00:00+00:00',
        )
        license = SimpleNamespace(license_key='L1')
        event_rec = SimpleNamespace(
            cloud='aws',
            region_name='eu-central-1',
            source_name='iam.amazonaws.com',
            event_name='EnableMFADevice',
            platform_id=None,
            account_id=None,
            tenant_name='t1',
            metadata={'user': 'alice'},
        )
        event_rec.as_dict = lambda: {
            'cloud': 'aws',
            'region_name': 'eu-central-1',
            'source_name': 'iam.amazonaws.com',
            'event_name': 'EnableMFADevice',
            'metadata': {'user': 'alice'},
        }

        metadata = Metadata(rules={'rule-a': _resource_scoped_meta()})
        svc.save(
            tenant=tenant,
            job=job,
            events_by_rule={'rule-a': [event_rec]},
            license=license,
            metadata=metadata,
        )
        assert s3.gz_put_json.call_count == 2
        rule_call = s3.gz_put_json.call_args_list[0]
        manifest_call = s3.gz_put_json.call_args_list[1]
        assert rule_call.kwargs['obj'][0]['event_name'] == 'EnableMFADevice'
        assert manifest_call.kwargs['obj'] == {
            'rule-a': rule_call.kwargs['key'],
        }

    def test_save_writes_when_rule_in_rules_to_scan(self) -> None:
        """Service persists payloads; resource-scoped filtering is in assembly."""
        s3 = MagicMock()
        svc = JobEventsService(s3_client=s3)
        tenant = SimpleNamespace(
            name='t1',
            customer_name='cust',
            cloud='aws',
            project='proj',
        )
        job = SimpleNamespace(
            id='job-1',
            tenant_name='t1',
            platform_id=None,
            job_type='reactive',
            rules_to_scan=['rule-b'],
            submitted_at='2024-01-15T12:00:00+00:00',
        )
        license = SimpleNamespace(license_key='L1')
        metadata = Metadata(rules={})
        event_rec = SimpleNamespace(as_dict=lambda: {'event_name': 'x'})
        svc.save(
            tenant=tenant,
            job=job,
            events_by_rule={'rule-b': [event_rec]},
            license=license,
            metadata=metadata,
        )
        assert s3.gz_put_json.call_count == 2
