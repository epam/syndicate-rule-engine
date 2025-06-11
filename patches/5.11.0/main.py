import re
import os
import sys
import logging

import pymongo
from pymongo.database import Database

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)

LATEST_VERSION = '000005.000011.000000'

def _init_mongo() -> Database:
    host = os.environ.get('SRE_MONGO_URI')
    db = os.environ.get('SRE_MONGO_DB_NAME')
    assert host, 'Host is required'
    assert db, 'db name is required'

    client = pymongo.MongoClient(host=host)
    return client.get_database(db)

def _normalize_version(version:str) -> str:
    if not version or version == ':':
        return LATEST_VERSION
    
    pattern = r'\.'.join(r'\d+' for _ in range(3))
    if not re.fullmatch(pattern, version):
        _LOG.warning(f'Bad version string: {version}')
        return '000000.000000.000000'
    
    major, minor, patch = version.split('.')
    return f'{major:>06}.{minor:>06}.{patch:>06}'

def patch_rule_versions():
    db = _init_mongo()
    
    collection = db.get_collection('CaaSRules')

    _LOG.info('Processing rules')
    for rule in collection.find():
        items = rule['id'].split('#')
        items[3] = _normalize_version(items[3])
        collection.update_one(
            {'_id': rule['_id']}, 
            {'$set': {'id': '#'.join(items)}}
        )
    _LOG.info('Finished processing rules')

    collection = db.get_collection('CaaSRulesets')

    _LOG.info('Processing rulesets')
    for ruleset in collection.find():
        items = ruleset['id'].split('#')
        items[3] = _normalize_version(items[3])
        collection.update_one(
            {'_id': ruleset['_id']}, 
            {'$set': {'id': '#'.join(items)}}
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