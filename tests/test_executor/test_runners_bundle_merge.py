"""Bundle merge for multiple scans of the same policy name (shared output path)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import msgspec.json
import pytest

from executor.job.policies.loader import PoliciesLoader
from executor.job.policies.runners import Runner
from helpers.constants import Cloud
from services.job_policy_filters.types import APPEND_TYPE, BundleFilters, PolicyScanEntry


class _FakeInnerPolicy:
    __slots__ = ('data', 'options', 'name', 'session_factory')

    def __init__(self, data, options, session_factory):
        self.data = data
        self.options = options
        self.session_factory = session_factory
        self.name = data['name']

    def get_variables(self):
        return {}

    def expand_variables(self, variables=None):
        return None

    def validate(self):
        return None


def _policy_stub(work_dir: Path, name: str = 'bundle-rule') -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        data={
            'name': name,
            'resource': 'aws.ec2.instance',
            'query': [],
            'filters': [],
        },
        options=SimpleNamespace(output_dir=str(work_dir), region='us-east-1'),
        session_factory=None,
    )


@pytest.mark.parametrize(
    'cloud',
    (Cloud.AWS, Cloud.AZURE, Cloud.GOOGLE, Cloud.KUBERNETES),
)
def test_all_runners_merge_bundle_scans_for_same_rule(tmp_path: Path, cloud: Cloud) -> None:
    """Each scan overwrites ``resources.json``; merged output keeps all chunks."""
    stub = _policy_stub(tmp_path)
    bundle = BundleFilters.from_policy_map(
        {
            stub.name: [
                PolicyScanEntry(
                    query_merge=APPEND_TYPE,
                    query=[{'a': '1'}],
                    filters_merge=APPEND_TYPE,
                    filters=[],
                ),
                PolicyScanEntry(
                    query_merge=APPEND_TYPE,
                    query=[{'b': '2'}],
                    filters_merge=APPEND_TYPE,
                    filters=[],
                ),
            ],
        },
    )
    idx = {'i': 0}
    payloads = ([{'n': 1}], [{'n': 2}])

    def fake_handle_errors(self, pol) -> bool:
        i = idx['i']
        idx['i'] += 1
        out = Path(pol.options.output_dir) / pol.name
        out.mkdir(parents=True, exist_ok=True)
        (out / 'resources.json').write_bytes(msgspec.json.encode(payloads[i]))
        return True

    runner = Runner.factory(cloud, [stub], policy_bundle=bundle)
    runner_cls = type(runner)

    with (
        patch('executor.job.policies.runners.Policy', _FakeInnerPolicy),
        patch.object(runner_cls, '_handle_errors', fake_handle_errors),
        patch.object(
            PoliciesLoader,
            'get_policy_region',
            staticmethod(lambda _p: 'us-east-1'),
        ),
    ):
        runner.start()

    assert runner.n_successful == 1
    assert not runner.failed
    merged = msgspec.json.decode(
        (tmp_path / stub.name / 'resources.json').read_bytes(),
    )
    assert merged == [{'n': 1}, {'n': 2}]
