import time
import urllib.error
import urllib.request
from functools import cached_property

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

# Built based on https://pypi.org/project/ec2-metadata/, but simplified for our use cases


DEFAULT_TOKEN_TTL_SECONDS = 21600
TOKEN_HEADER = 'X-aws-ec2-metadata-token'
TOKEN_HEADER_TTL = 'X-aws-ec2-metadata-token-ttl-seconds'


class EC2Metadata:
    service_url = 'http://169.254.169.254/latest/'
    dynamic_url = f'{service_url}dynamic/'
    metadata_url = f'{service_url}meta-data/'
    userdata_url = f'{service_url}user-data/'

    def __init__(self, token_ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS):
        self._token = None
        self._token_ttl_seconds = token_ttl_seconds
        self._token_updated_at = 0.0

    def _ensure_token_is_fresh(self) -> str | None:
        now = time.time()
        if self._token is not None and (now - self._token_updated_at) <= (
            self._token_ttl_seconds - 60
        ):
            return self._token

        req = urllib.request.Request(
            f'{self.service_url}api/token',
            headers={TOKEN_HEADER_TTL: str(self._token_ttl_seconds)},
            method='PUT',
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                token = resp.read().decode('utf-8')
        except TimeoutError:
            _LOG.warning('Could not get imds token. Timeout')
            return None
        except urllib.error.URLError:
            _LOG.exception('Could not get imds token')
            return None

        self._token_updated_at = now
        self._token = token
        return self._token

    def _get_url(self, url: str, timeout: float = 1.0) -> str | None:
        token = self._ensure_token_is_fresh()

        req = urllib.request.Request(
            url, headers={TOKEN_HEADER: token} if token else {}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8')
        except TimeoutError:
            _LOG.warning(f'{url} timed out')
            return None
        except urllib.error.URLError:
            _LOG.warning(f'Invalid request to {url}')
            return None

    @cached_property
    def public_ipv4(self) -> str | None:
        return self._get_url(f'{self.metadata_url}public-ipv4')

    @cached_property
    def private_ipv4(self) -> str | None:
        return self._get_url(f'{self.metadata_url}local-ipv4')

    @property
    def local_ipv4(self) -> str | None:
        return self.private_ipv4


ec2_metadata = EC2Metadata()
