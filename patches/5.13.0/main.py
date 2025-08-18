import logging
import os
import subprocess
import sys
import tempfile

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)

OLD_DB_NAME = 'custodian_as_a_service'
NEW_DB_NAME = 'syndicate_rule_engine'

def _init_mongo() -> tuple[MongoClient, Database]:
    host = os.environ.get('SRE_MONGO_URI')
    db = os.environ.get('SRE_MONGO_DB_NAME')
    assert host, 'Host is required'
    assert db, 'db name is required'

    client = MongoClient(host=host)
    return client, client.get_database(db)

def rename_collection(collection: Collection):
    new_name = collection.name.replace('CaaS', 'SRE')
    if new_name != collection.name:
        _LOG.info(f'Renaming collection {collection.name} to {new_name}')
        collection.rename(new_name)

def copy_database_with_archive(uri: str, old_db_name: str, new_db_name: str) -> bool:
    """Copy database using mongodump/mongorestore with archive and namespace mapping."""
    _LOG.info(f'Starting database copy from {old_db_name} to {new_db_name} using archive method')
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.archive', delete=False) as temp_archive:
            archive_path = temp_archive.name
        
        try:
            dump_cmd = [
                'mongodump',
                f'--uri={uri}',
                f'--db={old_db_name}',
                f'--archive={archive_path}',
            ]
            
            _LOG.info('Running dump command')
            _LOG.info(f'Command: {" ".join(dump_cmd)}')
            dump_result = subprocess.run(dump_cmd, capture_output=True, text=True, check=True)
            _LOG.info('Database dump to archive completed successfully')
            _LOG.info(f'mongodump output: {dump_result.stdout}')
            
            restore_cmd = [
                'mongorestore',
                f'--uri={uri}',
                f'--archive={archive_path}',
                f'--nsFrom={old_db_name}.*',
                f'--nsTo={new_db_name}.*'
            ]

            _LOG.info('Running restore command')
            _LOG.info(f'Command: {" ".join(restore_cmd)}')
            restore_result = subprocess.run(restore_cmd, capture_output=True, text=True, check=True)
            _LOG.info('Database restore from archive completed successfully')
            _LOG.info(f'mongorestore output: {restore_result.stdout}')
            
            return True
            
        finally:
            try:
                os.unlink(archive_path)
                _LOG.info(f'Cleaned up temporary archive: {archive_path}')
            except OSError as e:
                _LOG.warning(f'Failed to clean up archive {archive_path}: {e}')
        
    except subprocess.CalledProcessError as e:
        _LOG.error(f'Database copy command failed with return code {e.returncode}')
        _LOG.error(f'Error output: {e.stderr}')
        return False
    except Exception as e:
        _LOG.error(f'Unexpected error during database copy: {e}')
        return False

# NOTE: There is no easy way to rename MongoDB database.
# We have to create a new database and copy all collections and indexes.
# Therefore, we need enough disk space for the copy and archived dump.
def rename_database(client: MongoClient, old_db_name: str, new_db_name: str) -> bool:
    """Rename database by copying with archive and namespace mapping, then dropping the old database."""
    _LOG.info(f'Starting database rename from {old_db_name} to {new_db_name}')
    
    uri = os.environ.get('SRE_MONGO_URI')
    if not uri:
        _LOG.error('SRE_MONGO_URI environment variable is required')
        _LOG.error('For Docker MongoDB with root credentials, use: mongodb://mongouser:mongopassword@127.0.0.1:27017/?authSource=admin')
        return False
    
    if 'authSource=' not in uri and 'mongouser' in uri:
        if '?' in uri:
            uri += '&authSource=admin'
        else:
            uri += '?authSource=admin'
        _LOG.info('Added authSource=admin to URI for Docker MongoDB root authentication')
    
    if not copy_database_with_archive(uri, old_db_name, new_db_name):
        _LOG.error('Failed to copy database')
        return False
    
    # I think we shouldn't drop the old database just in case
    # try:
    #     client.drop_database(old_db_name)
    #     _LOG.info(f'Successfully dropped old database: {old_db_name}')
    # except Exception as e:
    #     _LOG.error(f'Failed to drop old database {old_db_name}: {e}')
    #     return False
    
    _LOG.info('Database rename completed successfully')
    return True

def main() -> int:
    try:
        client, db = _init_mongo()
        
        old_db_name = OLD_DB_NAME
        new_db_name = NEW_DB_NAME
        
        _LOG.info(f'Starting migration from {old_db_name} to {new_db_name}')
        
        collections = db.list_collection_names()
        for name in collections:
            if name.startswith('CaaS'):
                collection = db.get_collection(name)
                rename_collection(collection)
        
        if old_db_name != new_db_name:
            if not rename_database(client, old_db_name, new_db_name):
                _LOG.error('Database rename failed')
                return 1
            
        _LOG.info('Migration completed successfully')
        return 0
    except Exception as e:
        _LOG.exception(f'Unexpected exception: {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
