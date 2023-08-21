from typing import Optional
from models.scheduled_job import ScheduledJob
from services.clients.scheduler import AbstractJobScheduler


class SchedulerService:
    def __init__(self, client: AbstractJobScheduler):
        self._client = client

    @staticmethod
    def get(name: str,
            customer: Optional[str] = None) -> Optional[ScheduledJob]:
        item = ScheduledJob.get_nullable(hash_key=name)
        if not item or customer and item.customer_name != customer:
            return
        return item

    def update_job(self, item: ScheduledJob, is_enabled: bool):
        return self._client.update_job(item, is_enabled)
