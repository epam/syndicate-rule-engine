import json
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

from helpers.constants import RuleSourceSyncingStatus
from lambdas.configuration_api_handler.handler import (
    STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN,
    ConfigurationApiHandler,
)


@pytest.fixture
def handler():
    return ConfigurationApiHandler(rule_source_service=Mock())


def test_invoke_rule_meta_updater_syncing_rule_source(handler):
    rule_source = Mock()
    rule_source.id = 'rs-123'
    rule_source.customer = 'CUSTODIAN_SYSTEM'
    rule_source.git_project_id = 'epam/ecc-aws-rulepack'
    rule_source.latest_sync.as_dict.return_value = {
        'current_status': RuleSourceSyncingStatus.SYNCING.value
    }

    handler.rule_source_service.get_nullable.return_value = rule_source
    handler.rule_source_service.is_allowed_to_sync.return_value = False

    event = Mock()
    event.customer = 'CUSTODIAN_SYSTEM'
    event.rule_source_id = 'rs-123'

    with patch(
        'lambdas.configuration_api_handler.handler.sync_rulesource'
    ) as mock_sync:
        response = handler.invoke_rule_meta_updater(event)

    mock_sync.delay.assert_not_called()
    body = json.loads(response['body'])
    assert response['statusCode'] == HTTPStatus.ACCEPTED
    assert len(body['items']) == 1
    assert body['items'][0]['status'] == STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN


def test_invoke_rule_meta_updater_no_rule_sources(handler):
    handler.rule_source_service.query.return_value = []

    event = Mock()
    event.customer = 'CUSTODIAN_SYSTEM'
    event.rule_source_id = None

    with patch(
        'lambdas.configuration_api_handler.handler.sync_rulesource'
    ) as mock_sync:
        with pytest.raises(Exception) as exc_info:
            handler.invoke_rule_meta_updater(event)

    mock_sync.delay.assert_not_called()
    assert 'No rule sources were found' in str(exc_info.value)
