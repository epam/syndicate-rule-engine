import base64
import json
from http import HTTPStatus
from unittest.mock import Mock, MagicMock, patch, PropertyMock

import pytest
import requests

from helpers import JWTToken, Version
from helpers.constants import (
    ALG_ATTR,
    AUTHORIZATION_PARAM,
    HTTPMethod,
    JobState,
    KID_ATTR,
    TOKEN_ATTR,
)
from services.clients.lm_client import (
    LMClient,
    LMClientAfter2p7,
    LMClientAfter3p0,
    LMClientAfter3p3,
    LMClientFactory,
    LMAccessData,
    LMEndpoint,
    LmTokenProducer,
    LMEmptyBalance,
    LMException,
    LMInvalidData,
    LMUnavailable,
)


@pytest.fixture
def mock_settings_service():
    """Mock SettingsService."""
    mock = Mock()
    mock.get_license_manager_client_key_data.return_value = {
        KID_ATTR: 'test_kid',
        ALG_ATTR: 'test_alg',
    }
    mock.get_license_manager_access_data.return_value = {
        'url': 'https://test-lm.example.com',
    }
    return mock


@pytest.fixture
def mock_ssm_client():
    """Mock AbstractSSMClient."""
    mock = Mock()
    mock.get_secret_value.return_value = {'value': 'test_pem_key'}
    mock.create_secret.return_value = None
    return mock


@pytest.fixture
def mock_token_producer(mock_settings_service, mock_ssm_client):
    """Create LmTokenProducer with mocked dependencies."""
    # Create a mock that behaves like LmTokenProducer
    # Since __slots__ prevents attribute assignment, we use a Mock
    producer = Mock(spec=LmTokenProducer)
    producer.produce = Mock(return_value='test_token')
    producer.get_kid = Mock(return_value='test_kid')
    producer.get_pem = Mock(return_value=b'test_pem')
    return producer


@pytest.fixture
def mock_requests_response():
    """Create a mock requests.Response."""
    def _create_response(
        status_code=200,
        json_data=None,
        text='',
        content=b'',
        headers=None,
        ok=True,
    ):
        response = Mock(spec=requests.Response)
        response.status_code = status_code
        response.ok = ok
        response.text = text
        response.content = content
        response.headers = headers or {}
        response.url = 'https://test-lm.example.com/test'
        
        if json_data is not None:
            response.json.return_value = json_data
        else:
            response.json.side_effect = ValueError('Not JSON')
        
        return response
    return _create_response


@pytest.fixture
def mock_session(mock_requests_response):
    """Mock requests.Session."""
    session = Mock(spec=requests.Session)
    session.request = Mock()
    return session


@pytest.fixture
def lm_client(mock_token_producer, mock_session):
    """Create LMClient instance with mocked session."""
    client = LMClient(
        baseurl='https://test-lm.example.com',
        token_producer=mock_token_producer,
    )
    client._session = mock_session
    return client


class TestLmTokenProducer:
    """Tests for LmTokenProducer class."""

    def test_derive_client_private_key_id(self):
        """Test derive_client_private_key_id static method."""
        result = LmTokenProducer.derive_client_private_key_id('test_kid')
        assert result == 'cs_lm_client_test_kid_prk'

    def test_get_kid_from_settings(self, mock_settings_service, mock_ssm_client):
        """Test get_kid retrieves KID from settings."""
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        kid = producer.get_kid()
        assert kid == 'test_kid'
        mock_settings_service.get_license_manager_client_key_data.assert_called_once()

    def test_get_kid_cached(self, mock_settings_service, mock_ssm_client):
        """Test get_kid uses cached value."""
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        kid1 = producer.get_kid()
        kid2 = producer.get_kid()
        assert kid1 == kid2 == 'test_kid'
        # Should only call once due to caching
        assert mock_settings_service.get_license_manager_client_key_data.call_count == 1

    def test_get_kid_none(self, mock_settings_service, mock_ssm_client):
        """Test get_kid returns None when no KID in settings."""
        mock_settings_service.get_license_manager_client_key_data.return_value = {}
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        kid = producer.get_kid()
        assert kid is None

    def test_get_pem_from_ssm(self, mock_settings_service, mock_ssm_client):
        """Test get_pem retrieves PEM from SSM."""
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        pem = producer.get_pem()
        assert pem == b'test_pem_key'
        mock_ssm_client.get_secret_value.assert_called_once()

    def test_get_pem_cached(self, mock_settings_service, mock_ssm_client):
        """Test get_pem uses cached value."""
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        pem1 = producer.get_pem()
        pem2 = producer.get_pem()
        assert pem1 == pem2 == b'test_pem_key'
        assert mock_ssm_client.get_secret_value.call_count == 1

    def test_get_pem_no_kid(self, mock_settings_service, mock_ssm_client):
        """Test get_pem returns None when no KID."""
        mock_settings_service.get_license_manager_client_key_data.return_value = {}
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        pem = producer.get_pem()
        assert pem is None
        mock_ssm_client.get_secret_value.assert_not_called()

    def test_get_ssm_auth_token_name(self):
        """Test get_ssm_auth_token_name static method."""
        result = LmTokenProducer.get_ssm_auth_token_name('Test Customer')
        assert result == 'caas_lm_auth_token_test_customer'
        
        result = LmTokenProducer.get_ssm_auth_token_name('test-customer')
        assert result == 'caas_lm_auth_token_test_customer'

    @patch('services.clients.lm_client.SystemCustomer')
    @patch('services.clients.lm_client.JWTToken')
    @patch('services.license_manager_token.LicenseManagerToken')
    def test_produce_token_cached(
        self,
        mock_license_token,
        mock_jwt_token,
        mock_system_customer,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test produce returns cached token when available and not expired."""
        mock_system_customer.get_name.return_value = 'test_customer'
        mock_token = 'cached_token'
        mock_jwt_instance = Mock()
        mock_jwt_instance.is_expired.return_value = False
        mock_jwt_token.return_value = mock_jwt_instance
        
        mock_ssm_client.get_secret_value.return_value = {TOKEN_ATTR: mock_token}
        
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        
        token = producer.produce(cached=True)
        assert token == mock_token
        mock_license_token.assert_not_called()

    @patch('services.clients.lm_client.SystemCustomer')
    @patch('services.clients.lm_client.JWTToken')
    @patch('services.license_manager_token.LicenseManagerToken')
    def test_produce_token_new(
        self,
        mock_license_token,
        mock_jwt_token,
        mock_system_customer,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test produce creates new token when cache is empty."""
        mock_system_customer.get_name.return_value = 'test_customer'
        mock_jwt_instance = Mock()
        mock_jwt_instance.is_expired.return_value = True
        mock_jwt_token.return_value = mock_jwt_instance
        
        mock_license_instance = Mock()
        mock_license_instance.produce.return_value = 'new_token'
        mock_license_token.return_value = mock_license_instance
        
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        
        token = producer.produce(cached=True)
        assert token == 'new_token'
        mock_license_token.assert_called_once()
        mock_ssm_client.create_secret.assert_called_once()

    @patch('services.clients.lm_client.SystemCustomer')
    @patch('services.license_manager_token.LicenseManagerToken')
    def test_produce_token_no_cache(
        self,
        mock_license_token,
        mock_system_customer,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test produce creates new token when cached=False."""
        mock_system_customer.get_name.return_value = 'test_customer'
        mock_license_instance = Mock()
        mock_license_instance.produce.return_value = 'new_token'
        mock_license_token.return_value = mock_license_instance
        
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        
        token = producer.produce(cached=False)
        assert token == 'new_token'
        mock_ssm_client.create_secret.assert_not_called()

    @patch('services.clients.lm_client.SystemCustomer')
    def test_produce_token_no_pem(
        self,
        mock_system_customer,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test produce returns None when no PEM available."""
        mock_system_customer.get_name.return_value = 'test_customer'
        mock_settings_service.get_license_manager_client_key_data.return_value = {}
        
        producer = LmTokenProducer(
            settings_service=mock_settings_service,
            ssm=mock_ssm_client,
        )
        
        token = producer.produce()
        assert token is None


class TestLMClient:
    """Tests for LMClient base class."""

    def test_init(self, mock_token_producer):
        """Test LMClient initialization."""
        client = LMClient(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        assert client._baseurl == 'https://test-lm.example.com'
        assert client._token_producer == mock_token_producer
        assert client._session is not None

    def test_context_manager(self, mock_token_producer, mock_session):
        """Test LMClient as context manager."""
        client = LMClient(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        with client as ctx_client:
            assert ctx_client == client
        mock_session.close.assert_called_once()

    def test_del(self, mock_token_producer, mock_session):
        """Test LMClient destructor."""
        client = LMClient(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        client.__del__()
        mock_session.close.assert_called_once()

    def test_safe_json_valid(self, lm_client, mock_requests_response):
        """Test _safe_json with valid JSON response."""
        json_data = {'key': 'value'}
        resp = mock_requests_response(json_data=json_data)
        result = lm_client._safe_json(resp)
        assert result == json_data

    def test_safe_json_invalid(self, lm_client, mock_requests_response):
        """Test _safe_json with invalid JSON response."""
        resp = mock_requests_response(text='not json')
        result = lm_client._safe_json(resp)
        assert result is None

    def test_get_error_message_from_json(self, lm_client, mock_requests_response):
        """Test _get_error_message extracts message from JSON."""
        json_data = {'message': 'Error occurred'}
        resp = mock_requests_response(json_data=json_data)
        result = lm_client._get_error_message(resp)
        assert result == 'Error occurred'

    def test_get_error_message_from_text(self, lm_client, mock_requests_response):
        """Test _get_error_message falls back to text."""
        resp = mock_requests_response(text='Error text', json_data=None)
        result = lm_client._get_error_message(resp)
        assert result == 'Error text'

    def test_get_items_from_response_valid(self, lm_client, mock_requests_response):
        """Test _get_items_from_response with valid items."""
        json_data = {'items': [{'id': 1}, {'id': 2}]}
        resp = mock_requests_response(json_data=json_data)
        result = lm_client._get_items_from_response(resp)
        assert result == [{'id': 1}, {'id': 2}]

    def test_get_items_from_response_no_items(self, lm_client, mock_requests_response):
        """Test _get_items_from_response with no items."""
        json_data = {'data': 'value'}
        resp = mock_requests_response(json_data=json_data)
        result = lm_client._get_items_from_response(resp)
        assert result is None

    def test_get_items_from_response_empty_items(self, lm_client, mock_requests_response):
        """Test _get_items_from_response with empty items."""
        json_data = {'items': []}
        resp = mock_requests_response(json_data=json_data)
        result = lm_client._get_items_from_response(resp)
        assert result is None

    def test_send_request_success(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test _send_request successful call."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(status_code=200)
        lm_client._session.request.return_value = resp
        
        result = lm_client._send_request(
            endpoint=LMEndpoint.LICENSE_SYNC,
            method=HTTPMethod.POST,
            data={'key': 'value'},
        )
        
        assert result == resp
        lm_client._session.request.assert_called_once()
        call_kwargs = lm_client._session.request.call_args[1]
        assert call_kwargs['method'] == HTTPMethod.POST.value
        assert call_kwargs['json'] == {'key': 'value'}

    def test_send_request_with_token(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test _send_request includes token in headers."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(status_code=200)
        lm_client._session.request.return_value = resp
        
        lm_client._send_request(
            endpoint=LMEndpoint.LICENSE_SYNC,
            method=HTTPMethod.GET,
            token='test_token',
        )
        
        call_kwargs = lm_client._session.request.call_args[1]
        assert call_kwargs['headers'][AUTHORIZATION_PARAM] == 'test_token'

    def test_send_request_exception(self, lm_client):
        """Test _send_request handles exceptions."""
        lm_client._session.request.side_effect = requests.RequestException('Error')
        
        result = lm_client._send_request(
            endpoint=LMEndpoint.LICENSE_SYNC,
            method=HTTPMethod.GET,
        )
        
        assert result is None

    def test_sync_license_success(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test sync_license successful call."""
        mock_token_producer.produce.return_value = 'test_token'
        license_data = {
            'license_key': 'test_key',
            'description': 'Test license',
        }
        resp = mock_requests_response(
            json_data={'items': [license_data]},
            status_code=200,
            ok=True,
        )
        lm_client._session.request.return_value = resp
        
        result, status = lm_client.sync_license('test_key')
        
        assert result == license_data
        assert status == 200

    def test_sync_license_failed_request(self, lm_client, mock_token_producer):
        """Test sync_license when request fails."""
        mock_token_producer.produce.return_value = 'test_token'
        lm_client._session.request.return_value = None
        
        result, status = lm_client.sync_license('test_key')
        
        assert result == 'Failed license sync'
        assert status == HTTPStatus.INTERNAL_SERVER_ERROR

    def test_sync_license_error_response(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test sync_license with error response."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            json_data={'message': 'Invalid license'},
            status_code=400,
            ok=False,
        )
        lm_client._session.request.return_value = resp
        
        result, status = lm_client.sync_license('test_key')
        
        assert result == 'Invalid license'
        assert status == 400

    def test_sync_license_invalid_response(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test sync_license with invalid response format."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            json_data={'data': 'value'},
            status_code=200,
            ok=True,
        )
        lm_client._session.request.return_value = resp
        
        result, status = lm_client.sync_license('test_key')
        
        assert result == 'Invalid response format'
        assert status == 200

    def test_check_permission_base(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test check_permission in base LMClient."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(status_code=200, ok=True)
        lm_client._session.request.return_value = resp
        
        result = lm_client.check_permission('customer', 'tenant', 'tlk')
        
        assert isinstance(result, bool)

    def test_activate_customer_success(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test activate_customer successful call."""
        mock_token_producer.produce.return_value = 'test_token'
        resp_data = {
            'license_key': 'lk',
            'tenant_license_key': 'tlk',
        }
        resp = mock_requests_response(
            json_data={'items': [resp_data]},
            status_code=200,
            ok=True,
        )
        lm_client._session.request.return_value = resp
        
        result = lm_client.activate_customer('customer', 'tlk')
        
        assert result == ('lk', 'tlk')

    def test_activate_customer_failed(self, lm_client, mock_token_producer):
        """Test activate_customer when request fails."""
        mock_token_producer.produce.return_value = 'test_token'
        lm_client._session.request.return_value = None
        
        result = lm_client.activate_customer('customer', 'tlk')
        
        assert result is None

    def test_post_job_success(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test post_job successful call."""
        mock_token_producer.produce.return_value = 'test_token'
        job_data = {'job_id': 'test_job', 'status': 'created'}
        resp = mock_requests_response(
            json_data={'items': [job_data]},
            status_code=200,
            ok=True,
        )
        lm_client._session.request.return_value = resp
        
        result = lm_client.post_job(
            job_id='test_job',
            customer='customer',
            tenant='tenant',
            ruleset_map={'ruleset1': ['rule1']},
        )
        
        assert result == job_data

    def test_post_job_unavailable(self, lm_client, mock_token_producer):
        """Test post_job raises LMUnavailable when request fails."""
        mock_token_producer.produce.return_value = 'test_token'
        lm_client._session.request.return_value = None
        
        with pytest.raises(LMUnavailable):
            lm_client.post_job(
                job_id='test_job',
                customer='customer',
                tenant='tenant',
                ruleset_map={'ruleset1': ['rule1']},
            )

    def test_post_job_empty_balance(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test post_job raises LMEmptyBalance on 403."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            json_data={'message': 'Balance exhausted'},
            status_code=403,
            ok=False,
        )
        lm_client._session.request.return_value = resp
        
        with pytest.raises(LMEmptyBalance):
            lm_client.post_job(
                job_id='test_job',
                customer='customer',
                tenant='tenant',
                ruleset_map={'ruleset1': ['rule1']},
            )

    def test_post_job_invalid_data(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test post_job raises LMInvalidData on 404."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            json_data={'message': 'Not found'},
            status_code=404,
            ok=False,
        )
        lm_client._session.request.return_value = resp
        
        with pytest.raises(LMInvalidData):
            lm_client.post_job(
                job_id='test_job',
                customer='customer',
                tenant='tenant',
                ruleset_map={'ruleset1': ['rule1']},
            )

    def test_post_job_unavailable_error(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test post_job raises LMUnavailable on other errors."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            json_data={'message': 'Server error'},
            status_code=500,
            ok=False,
        )
        lm_client._session.request.return_value = resp
        
        with pytest.raises(LMUnavailable):
            lm_client.post_job(
                job_id='test_job',
                customer='customer',
                tenant='tenant',
                ruleset_map={'ruleset1': ['rule1']},
            )

    def test_update_job_success(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test update_job successful call."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(status_code=200, ok=True)
        lm_client._session.request.return_value = resp
        
        result = lm_client.update_job(
            job_id='test_job',
            customer='customer',
            status=JobState.RUNNING,
        )
        
        assert result is True

    def test_update_job_failed(self, lm_client, mock_token_producer):
        """Test update_job when request fails."""
        mock_token_producer.produce.return_value = 'test_token'
        lm_client._session.request.return_value = None
        
        result = lm_client.update_job(
            job_id='test_job',
            customer='customer',
        )
        
        assert result is False

    def test_whoami_success(
        self, lm_client, mock_requests_response, mock_token_producer
    ):
        """Test whoami successful call."""
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            json_data={'client_id': 'test_client'},
            status_code=200,
            ok=True,
            headers={'Accept-Version': '3.3.0'},
        )
        lm_client._session.request.return_value = resp
        
        client_id, version = lm_client.whoami()
        
        assert client_id == 'test_client'
        assert version == '3.3.0'

    def test_whoami_failed(self, lm_client, mock_token_producer):
        """Test whoami when request fails."""
        mock_token_producer.produce.return_value = 'test_token'
        lm_client._session.request.return_value = None
        
        client_id, version = lm_client.whoami()
        
        assert client_id is None
        assert version is None

    def test_get_all_metadata_base(self, lm_client):
        """Test get_all_metadata in base class returns None."""
        result = lm_client.get_all_metadata('customer', 'tlk')
        assert result is None


class TestLMClientAfter2p7:
    """Tests for LMClientAfter2p7 class."""

    def test_check_permission_success(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test check_permission successful call."""
        client = LMClientAfter2p7(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        resp_data = {
            'tlk1': {'allowed': ['tenant1', 'tenant2']},
        }
        resp = mock_requests_response(
            json_data={'items': [resp_data]},
            status_code=200,
            ok=True,
        )
        mock_session.request.return_value = resp
        
        result = client.check_permission('customer', 'tenant1', 'tlk1')
        
        assert result is True

    def test_check_permission_tenant_not_allowed(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test check_permission when tenant is not allowed."""
        client = LMClientAfter2p7(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        resp_data = {
            'tlk1': {'allowed': ['tenant2']},
        }
        resp = mock_requests_response(
            json_data={'items': [resp_data]},
            status_code=200,
            ok=True,
        )
        mock_session.request.return_value = resp
        
        result = client.check_permission('customer', 'tenant1', 'tlk1')
        
        assert result is False

    def test_check_permission_tlk_not_found(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test check_permission when TLK not found in response."""
        client = LMClientAfter2p7(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        resp_data = {'other_tlk': {'allowed': ['tenant1']}}
        resp = mock_requests_response(
            json_data={'items': [resp_data]},
            status_code=200,
            ok=True,
        )
        mock_session.request.return_value = resp
        
        result = client.check_permission('customer', 'tenant1', 'tlk1')
        
        assert result is False

    def test_check_permission_failed_request(
        self, mock_token_producer, mock_session
    ):
        """Test check_permission when request fails."""
        client = LMClientAfter2p7(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        mock_session.request.return_value = None
        
        result = client.check_permission('customer', 'tenant1', 'tlk1')
        
        assert result is False


class TestLMClientAfter3p0:
    """Tests for LMClientAfter3p0 class."""

    def test_post_ruleset_success(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test post_ruleset successful call."""
        client = LMClientAfter3p0(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(
            status_code=201,
            ok=True,
            text='',
        )
        mock_session.request.return_value = resp
        
        result = client.post_ruleset(
            name='test_ruleset',
            version='1.0.0',
            cloud='aws',
            description='Test',
            display_name='Test Ruleset',
            download_url='https://example.com/ruleset',
            rules=['rule1', 'rule2'],
            overwrite=True,
        )
        
        assert result == (HTTPStatus.CREATED, '')

    def test_post_ruleset_failed(self, mock_token_producer, mock_session):
        """Test post_ruleset when request fails."""
        client = LMClientAfter3p0(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        mock_session.request.return_value = None
        
        result = client.post_ruleset(
            name='test_ruleset',
            version='1.0.0',
            cloud='aws',
            description='Test',
            display_name='Test Ruleset',
            download_url='https://example.com/ruleset',
            rules=['rule1'],
            overwrite=False,
        )
        
        assert result is None


class TestLMClientAfter3p3:
    """Tests for LMClientAfter3p3 class."""

    def test_get_all_metadata_success(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test get_all_metadata successful call."""
        client = LMClientAfter3p3(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        metadata_bytes = b'test metadata content'
        encoded = base64.b64encode(metadata_bytes)
        resp = mock_requests_response(
            status_code=200,
            ok=True,
            content=encoded,
        )
        mock_session.request.return_value = resp
        
        result = client.get_all_metadata('customer', 'tlk')
        
        assert result == metadata_bytes

    def test_get_all_metadata_with_version(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test get_all_metadata with installation_version."""
        client = LMClientAfter3p3(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        metadata_bytes = b'test metadata'
        encoded = base64.b64encode(metadata_bytes)
        resp = mock_requests_response(
            status_code=200,
            ok=True,
            content=encoded,
        )
        mock_session.request.return_value = resp
        
        result = client.get_all_metadata(
            'customer', 'tlk', installation_version='1.0.0'
        )
        
        assert result == metadata_bytes
        # Verify params were passed
        call_kwargs = mock_session.request.call_args[1]
        assert 'installation_version' in call_kwargs['params']

    def test_get_all_metadata_failed(
        self, mock_token_producer, mock_session, mock_requests_response
    ):
        """Test get_all_metadata when request fails."""
        client = LMClientAfter3p3(
            baseurl='https://test-lm.example.com',
            token_producer=mock_token_producer,
        )
        client._session = mock_session
        
        mock_token_producer.produce.return_value = 'test_token'
        resp = mock_requests_response(status_code=404, ok=False)
        mock_session.request.return_value = resp
        
        result = client.get_all_metadata('customer', 'tlk')
        
        assert result is None


class TestLMClientFactory:
    """Tests for LMClientFactory class."""

    @patch('services.clients.lm_client.LMAccessData')
    def test_create_no_version(
        self, mock_access_data, mock_settings_service, mock_ssm_client
    ):
        """Test create when whoami returns no version."""
        mock_access_data.from_dict.return_value = Mock(url='https://test.com')
        
        factory = LMClientFactory(mock_settings_service, mock_ssm_client)
        
        with patch('services.clients.lm_client.LMClientAfter3p3') as mock_client_class:
            mock_client = Mock()
            mock_client.whoami.return_value = (None, None)
            mock_client_class.return_value = mock_client
            
            result = factory.create()
            
            assert result == mock_client

    @patch('services.clients.lm_client.LMAccessData')
    def test_create_version_3p3(
        self,
        mock_access_data,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test create returns LMClientAfter3p3 for version >= 3.3.0."""
        mock_access_data.from_dict.return_value = Mock(url='https://test.com')
        
        factory = LMClientFactory(mock_settings_service, mock_ssm_client)
        
        with patch('services.clients.lm_client.LMClientAfter3p3') as mock_client_class:
            mock_client = Mock()
            mock_client.whoami.return_value = ('client_id', '3.3.0')
            mock_client_class.return_value = mock_client
            
            # Create a mock Version class that compares correctly
            class MockVersion:
                def __init__(self, version_str):
                    self.version_str = version_str
                    # Parse version to tuple for comparison
                    parts = version_str.split('.')
                    self.tuple = tuple(int(p) for p in parts)
                
                def __ge__(self, other):
                    if isinstance(other, MockVersion):
                        return self.tuple >= other.tuple
                    return True
            
            with patch('services.clients.lm_client.Version', MockVersion):
                result = factory.create()
                
                assert result == mock_client

    @patch('services.clients.lm_client.LMAccessData')
    def test_create_version_3p0(
        self,
        mock_access_data,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test create returns LMClientAfter3p0 for version >= 3.0.0."""
        mock_access_data.from_dict.return_value = Mock(url='https://test.com')
        
        factory = LMClientFactory(mock_settings_service, mock_ssm_client)
        
        with patch('services.clients.lm_client.LMClientAfter3p3') as mock_client_3p3:
            mock_client = Mock()
            mock_client.whoami.return_value = ('client_id', '3.0.0')
            mock_client_3p3.return_value = mock_client
            
            # Create a mock Version class that compares correctly
            class MockVersion:
                def __init__(self, version_str):
                    self.version_str = version_str
                    # Parse version to tuple for comparison
                    parts = version_str.split('.')
                    self.tuple = tuple(int(p) for p in parts)
                
                def __ge__(self, other):
                    if isinstance(other, MockVersion):
                        return self.tuple >= other.tuple
                    return True
            
            with patch('services.clients.lm_client.Version', MockVersion):
                with patch('services.clients.lm_client.LMClientAfter3p0') as mock_client_3p0:
                    mock_client_3p0_instance = Mock()
                    mock_client_3p0.return_value = mock_client_3p0_instance
                    
                    result = factory.create()
                    
                    assert result == mock_client_3p0_instance

    @patch('services.clients.lm_client.LMAccessData')
    def test_create_version_2p7(
        self,
        mock_access_data,
        mock_settings_service,
        mock_ssm_client,
    ):
        """Test create returns LMClientAfter2p7 for version >= 2.7.0."""
        mock_access_data.from_dict.return_value = Mock(url='https://test.com')
        
        factory = LMClientFactory(mock_settings_service, mock_ssm_client)
        
        with patch('services.clients.lm_client.LMClientAfter3p3') as mock_client_3p3:
            mock_client = Mock()
            mock_client.whoami.return_value = ('client_id', '2.7.0')
            mock_client_3p3.return_value = mock_client
            
            # Create a mock Version class that compares correctly
            class MockVersion:
                def __init__(self, version_str):
                    self.version_str = version_str
                    # Parse version to tuple for comparison
                    parts = version_str.split('.')
                    self.tuple = tuple(int(p) for p in parts)
                
                def __ge__(self, other):
                    if isinstance(other, MockVersion):
                        return self.tuple >= other.tuple
                    return True
            
            with patch('services.clients.lm_client.Version', MockVersion):
                with patch('services.clients.lm_client.LMClientAfter2p7') as mock_client_2p7:
                    mock_client_2p7_instance = Mock()
                    mock_client_2p7.return_value = mock_client_2p7_instance
                    
                    result = factory.create()
                    
                    assert result == mock_client_2p7_instance


class TestExceptions:
    """Tests for exception classes."""

    def test_lm_exception(self):
        """Test LMException base class."""
        exc = LMException('test error')
        assert str(exc) == 'test error'
        assert isinstance(exc, Exception)

    def test_lm_empty_balance(self):
        """Test LMEmptyBalance exception."""
        exc = LMEmptyBalance('balance exhausted')
        assert str(exc) == 'balance exhausted'
        assert isinstance(exc, LMException)

    def test_lm_invalid_data(self):
        """Test LMInvalidData exception."""
        exc = LMInvalidData('invalid data')
        assert str(exc) == 'invalid data'
        assert isinstance(exc, LMException)

    def test_lm_unavailable(self):
        """Test LMUnavailable exception."""
        exc = LMUnavailable('service unavailable')
        assert str(exc) == 'service unavailable'
        assert isinstance(exc, LMException)

