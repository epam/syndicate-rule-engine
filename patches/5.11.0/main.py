import logging
import os
import sys

import pymongo
from pymongo.database import Database

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)


def _init_mongo() -> Database:
    host = os.environ.get('SRE_MONGO_URI')
    db = os.environ.get('SRE_MONGO_DB_NAME')
    assert host, 'Host is required'
    assert db, 'db name is required'

    client = pymongo.MongoClient(host=host)
    return client.get_database(db)


def _normalize_version(
    version: str, length: int = 6, parts: int | None = None
) -> str:
    if not version:
        version = '0.0.0'
    items = version.strip().split('.')
    if parts is not None:
        if len(items) > parts:
            items = items[:parts]
        elif len(items) < parts:
            items.extend(['0' for _ in range(parts - len(items))])

    return '.'.join([item.zfill(length) for item in items])


def patch_rule_versions():
    db = _init_mongo()

    collection = db.get_collection('CaaSRulesets')

    _LOG.info('Processing rulesets')
    for ruleset in collection.find({'id': {'$regex': '^.*#S#.*#.+$'}}):
        _LOG.info('Processing ruleset %s', ruleset['id'])
        items = ruleset['id'].split('#')
        items[3] = _normalize_version(items[3])
        collection.update_one(
            {'_id': ruleset['_id']}, {'$set': {'id': '#'.join(items)}}
        )
    _LOG.info('Finished processing rulesets')


def main() -> int:
    try:
        patch_rule_versions()
        return 0
    except Exception:
        _LOG.exception('Unexpected exception')
        return 1


if __name__ == '__main__':
    sys.exit(main())

