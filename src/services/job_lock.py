import time
import uuid
from typing import TYPE_CHECKING, Generator

from modular_sdk.models.tenant_settings import TenantSettings

from helpers.constants import TS_JOB_LOCK_KEY, Env
from services import SP
from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )

_LOG = get_logger(__name__)


class JobPayload:
    __slots__ = ('expiration', 'regions')

    def __init__(
        self, expiration: float | None = None, regions: set[str] | None = None
    ):
        if not expiration:
            expiration = time.time() + 3600 * 1.5

        self.expiration: float = expiration
        self.regions: set[str] = regions or set()

    def serialize(self) -> dict:
        return {'e': self.expiration, 'r': sorted(self.regions)}

    def is_expired(self) -> bool:
        return self.expiration < time.time()

    @classmethod
    def deserialize(cls, data: dict) -> 'JobPayload':
        return cls(expiration=data.get('e'), regions=set(data.get('r') or []))

    def intersected(self, regions: set[str]) -> set[str]:
        return self.regions & regions

    def is_locked(self, regions: set[str]) -> bool:
        return bool(self.intersected(regions)) and not self.is_expired()


class TenantSettingJobLock:
    """
    {
        "k": "CUSTODIAN_JOB_LOCK",
        "t": "EXAMPLE-TENANT",
        "v": {
            "job-id-1": {
                "e": 1231231,
                "r": []
            }
        }
    }
    """

    def __init__(self, tenant_name: str):
        """
        >>> lock = TenantSettingJobLock('MY_TENANT')
        >>> lock.locked({'eu-central-1'})
        False
        >>> lock.acquire('job-1')
        >>> lock.locked({'eu-central-1'})
        True
        >>> lock.locked({'eu-west-1'})
        False
        >>> lock.release()
        >>> lock.locked({'eu-central-1'})
        False
        :param tenant_name:
        """
        self._tenant_name = tenant_name

    @property
    def tss(self) -> 'TenantSettingsService':
        """
        Tenant settings service
        :return:
        """
        return SP.modular_client.tenant_settings_service()

    @staticmethod
    def _covert_regions(
        r: set[str] | tuple[str, ...] | list[str] | str, /
    ) -> set[str]:
        return {r} if isinstance(r, str) else set(r)

    def acquire(
        self,
        job_id: str,
        regions: set[str] | tuple[str, ...] | list[str] | str,
    ):
        """
        You must check whether the lock is locked before calling acquire().
        """
        regions = self._covert_regions(regions)

        payload = JobPayload(regions=regions).serialize()
        item = self.tss.get(tenant_name=self._tenant_name, key=TS_JOB_LOCK_KEY)
        if not item:
            # unfortunately we must create an item before we can update
            # nested attributes.
            # in other words, update fails in case you do it on the nested
            # attribute of not existing item.
            self.tss.create(
                tenant_name=self._tenant_name,
                key=TS_JOB_LOCK_KEY,
                value={job_id: payload},
            ).save()
            return
        # item found
        self.tss.update(
            item, actions=[TenantSettings.value[job_id].set(payload)]
        )

    def release(self, job_id: str, /):
        item = self.tss.create(
            tenant_name=self._tenant_name, key=TS_JOB_LOCK_KEY
        )
        try:
            self.tss.update(
                item, actions=[TenantSettings.value[job_id].remove()]
            )
        except Exception:  # noqa
            # update will fail in case item does not exist in db
            pass

    @staticmethod
    def _iter_payloads(
        data: dict[str, dict],
    ) -> Generator[tuple[str, JobPayload], None, None]:
        """
        Iterates over setting value and yields valid payloads, skipping
        invalid ones
        """
        for key, value in data.items():
            try:
                uuid.UUID(key)
                payload = JobPayload.deserialize(value)
            except Exception:  # noqa
                continue
            # key is a valid uuid thus a valid job id
            # payload is also valid
            yield key, payload

    def locked(
        self, r: set[str] | tuple[str, ...] | list[str] | str, /
    ) -> bool:
        return self.locked_for(r) is not None

    def locked_for(
        self, r: set[str] | tuple[str, ...] | list[str] | str, /
    ) -> str | None:
        """
        The same as above but allows to get the job that is running
        """
        if Env.ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT.as_bool():
            _LOG.debug(
                f'Ignoring job lock for {self._tenant_name} because env is set'
            )
            return
        regions = self._covert_regions(r)

        item = self.tss.get(self._tenant_name, TS_JOB_LOCK_KEY)
        if not item:
            return
        value = item.value.as_dict()
        for key, payload in self._iter_payloads(value):
            if payload.is_locked(regions):
                return key
