import pickle

from celery.beat import ScheduleEntry, Scheduler
from pymongo import UpdateOne

from helpers.constants import ScheduledJobType
from helpers.log_helper import get_logger
from models.scheduled_job import ScheduledJob

_LOG = get_logger(__name__)


class MongoScheduler(Scheduler):
    schedule: dict[str, ScheduleEntry]
    Entry = ScheduleEntry

    def __init__(self, *args, **kwargs):
        self._dirty = set()
        super().__init__(*args, **kwargs)

    def reserve(self, entry):
        self._dirty.add(entry.name)
        return super().reserve(entry)

    def setup_schedule(self):
        # self.install_default_entries(self.data)  # default entries are not used

        system_items = {
            item.id: item
            for item in ScheduledJob.typ_index.query(
                hash_key=ScheduledJobType.SYSTEM.value,
                attributes_to_get=(ScheduledJob.id, ScheduledJob.celery),
            )
        }
        beat_schedule: dict = self.app.conf.beat_schedule.copy()
        while beat_schedule:
            name, data = beat_schedule.popitem()
            entry = self.Entry(**dict(data), name=name, app=None)

            if name not in system_items:
                # need to register new system item
                ScheduledJob(
                    id=name,
                    name=name,
                    typ=ScheduledJobType.SYSTEM.value,
                    celery=self.serialize_entry(entry),
                ).save()
            else:  # name in system_item
                # need to update attributes but keep last_run_at and total count
                si = system_items.pop(name)
                system_entry = self.to_entry(si)
                if not system_entry:
                    _LOG.warning(
                        'Could not load system entry. Removing form db'
                    )
                    beat_schedule[name] = data  # back to queue to be recreated
                    continue
                system_entry.update(entry)
                si.update(
                    actions=[
                        ScheduledJob.celery.set(
                            self.serialize_entry(system_entry)
                        )
                    ]
                )

        for item in system_items.values():
            item.delete()

        self.sync()

    @staticmethod
    def to_entry(job: ScheduledJob) -> ScheduleEntry | None:
        if not job.celery:
            return
        return MongoScheduler.deserialize_entry(job.celery)

    @staticmethod
    def deserialize_entry(data: bytes) -> ScheduleEntry | None:
        try:
            return pickle.loads(data)
        except pickle.UnpicklingError as e:
            _LOG.warning(f'Could not unpickle celery task: {e}')
            return

    @staticmethod
    def serialize_entry(entry: ScheduleEntry) -> bytes:
        return pickle.dumps(entry)

    def _to_bulk_update_one(self, entry: ScheduleEntry) -> UpdateOne:
        return UpdateOne(
            filter={ScheduledJob.id.attr_name: entry.name},
            update={
                '$set': {
                    ScheduledJob.celery.attr_name: self.serialize_entry(entry),
                }
            },
            upsert=False,
        )

    def sync(self):
        _LOG.info('Syncing Mongo Schedules')

        it = ScheduledJob.scan(
            filter_condition=ScheduledJob.celery.exists(),
            attributes_to_get=(ScheduledJob.id, ScheduledJob.celery),
        )
        disabled = set()  # keeps ids of disabled jobs
        new_schedule = {}
        for item in it:
            entry = self.to_entry(item)
            if not entry:
                _LOG.info(f'Invalid entry for task: {item.id}')
                continue
            new_schedule[item.id] = entry
            if not item.enabled:
                disabled.add(item.id)

        old_schedule = self.schedule
        ops = []
        while self._dirty:
            name = self._dirty.pop()
            if name not in new_schedule:
                continue  # don't need this entry anymore
            old_entry = old_schedule.get(name)
            if not old_entry:  # probably should not happen at all
                _LOG.warning(
                    'A dirty entry does not exist in schedule. Skipping'
                )
                continue
            # need to keep last_run_at and total count from the old entry
            # but "editable" fields from the new entry. See ScheduleEntry.update
            old_entry.update(new_schedule[name])
            new_schedule[name] = old_entry
            ops.append(self._to_bulk_update_one(old_entry))

        if ops:
            _LOG.debug(f'Updating {len(ops)} tasks')
            col = ScheduledJob.mongo_adapter().get_collection(ScheduledJob)
            # TODO: use bulk update only if number of ops > than
            #  a certain threshold
            col.bulk_write(ops)

        for name in disabled:
            new_schedule.pop(name)

        _LOG.debug(f'Current tasks: {new_schedule}')
        self.schedule = new_schedule
