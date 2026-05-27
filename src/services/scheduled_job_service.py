import secrets
from datetime import timedelta
from typing import Any

from celery.beat import ScheduleEntry, maybe_schedule
from celery.schedules import BaseSchedule, crontab, schedule
from modular_sdk.models.pynamongo.convertors import instance_as_dict
from pynamodb.pagination import ResultIterator

from helpers.constants import COMPOUND_KEYS_SEPARATOR, ScheduledJobType
from helpers.time_helper import utc_iso
from models.scheduled_job import ScheduledJob
from onprem.scheduler import MongoScheduler
from services.base_data_service import BaseDataService


CELERY_CRON_FORMAT = 'cron({0._orig_minute} {0._orig_hour} {0._orig_day_of_month} {0._orig_month_of_year} {0._orig_day_of_week})'


class ScheduledJobService(BaseDataService[ScheduledJob]):
    def get_by_id(self, id: str) -> ScheduledJob | None:
        return super().get_nullable(hash_key=id)

    def get_by_name(
        self, customer_name: str, name: str
    ) -> ScheduledJob | None:
        return self.get_by_id(
            id=self.derive_standard_job_id(customer_name, name)
        )

    @staticmethod
    def generate_name(tenant_name: str) -> str:
        return f'sg-{tenant_name.lower()}-{secrets.token_hex(8)}'

    @staticmethod
    def derive_standard_job_id(customer_name: str, name: str) -> str:
        """
        Ids must be unique across the installation. No particular need for
        such id scheme, just bear with me
        """
        return COMPOUND_KEYS_SEPARATOR.join(
            (ScheduledJobType.STANDARD.value, customer_name, name)
        )

    def create(
        self,
        name: str | None,
        customer_name: str,
        tenant_name: str,
        typ: ScheduledJobType,
        description: str,
        meta: dict,
    ) -> ScheduledJob:
        if name is None:
            name = self.generate_name(tenant_name)
        return ScheduledJob(
            id=self.derive_standard_job_id(customer_name, name),
            name=name,
            customer_name=customer_name,
            tenant_name=tenant_name,
            typ=typ.value,
            description=description,
            meta=meta,
        )

    def get_by_customer(
        self,
        customer_name: str,
        typ: ScheduledJobType | None = None,
        tenant_name: str | None = None,
        limit: int | None = None,
        last_evaluated_key: int | dict | None = None,
    ) -> ResultIterator[ScheduledJob]:
        rkc = None
        if typ:
            rkc &= ScheduledJob.typ == typ.value
        fc = None
        if tenant_name:
            fc = ScheduledJob.tenant_name == tenant_name
        return ScheduledJob.customer_name_typ_index.query(
            hash_key=customer_name,
            range_key_condition=rkc,
            filter_condition=fc,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            scan_index_forward=False,
        )

    @staticmethod
    def set_celery_task(
        item: ScheduledJob,
        task: str,
        sch: int | timedelta | BaseSchedule,
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> None:
        item.celery = MongoScheduler.serialize_entry(
            ScheduleEntry(
                name=item.id,
                task=task,
                schedule=sch,
                args=args,
                kwargs=kwargs,
                app=None,
            )
        )

    def update(
        self,
        item: ScheduledJob,
        enabled: bool | None = None,
        description: str | None = None,
        sch: int | timedelta | BaseSchedule | None = None,
    ) -> None:
        actions = []
        if enabled is not None:
            actions.append(ScheduledJob.enabled.set(enabled))
        if description is not None:
            actions.append(ScheduledJob.description.set(description))
        if (
            sch is not None
            and item.celery
            and (entry := MongoScheduler.deserialize_entry(item.celery))
        ):
            entry.schedule = maybe_schedule(sch)
            actions.append(
                ScheduledJob.celery.set(MongoScheduler.serialize_entry(entry))
            )
        if actions:
            item.update(actions=actions)

    def dto(self, item: ScheduledJob) -> dict[str, Any]:
        dct = instance_as_dict(item)
        if cel := dct.pop('celery', None):
            entry = MongoScheduler.deserialize_entry(cel)
            if entry:
                dct['schedule'] = self.celery_schedule_to_str(entry.schedule)
                dct['total_run_count'] = entry.total_run_count
                dct['last_run_at'] = utc_iso(entry.last_run_at)
        dct.pop('typ', None)
        dct.pop('id', None)
        return dct

    @staticmethod
    def celery_schedule_to_str(sch: BaseSchedule) -> str:
        """
        Converts to the human-readable value that we use in dto. It mostly
        resembles AWS Event Bridge schedules
        """
        if isinstance(sch, schedule):
            val, unit = sch.human_seconds.split(maxsplit=1)
            return f'rate({int(float(val))} {unit})'
        if isinstance(sch, crontab):
            return CELERY_CRON_FORMAT.format(sch)
        raise NotImplementedError(f'cannot format {sch.__class__.__name__}')

