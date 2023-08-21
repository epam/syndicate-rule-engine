import os
import pathlib
import shutil
import json

from pathlib import Path
from helpers.constants import STEP_GENERATE_REPORT
from helpers.exception import ExecutorException
from helpers.log_helper import get_logger

MAPPING_FOLDER = 'resources_mapping'
MAPPING_PATH = Path(__file__).parent.parent / MAPPING_FOLDER

_LOG = get_logger(__name__)


class OSService:
    def __init__(self):
        self.c7n_workdir = str(pathlib.Path(__file__).parent.parent.absolute())

    def create_workdir(self, job_id):
        temp_dir = str(Path(self.c7n_workdir, job_id))
        os.chdir(str(pathlib.Path(temp_dir).parent))
        pathlib.Path(temp_dir).mkdir(exist_ok=True)
        return temp_dir

    @staticmethod
    def clean_workdir(work_dir):
        shutil.rmtree(work_dir, ignore_errors=True)
        _LOG.debug(f'Workdir for {work_dir} successfully cleaned')

    @staticmethod
    def list_policies_in_dir(cloud_policy_dir):
        yml_ = [file for file in os.listdir(cloud_policy_dir) if
                file.endswith('.yml')]
        yml_.sort()
        return yml_

    @staticmethod
    def read_file(file_path, json_content=True):
        if not os.path.exists(file_path):
            _LOG.error(f'File does not exist by path: {file_path}')
            raise ExecutorException(
                reason=f'Expected path does not exist: {file_path}',
                step_name=STEP_GENERATE_REPORT)
        with open(file_path) as file_desc:
            return json.loads(file_desc.read()) \
                if json_content else file_desc.read()

    @staticmethod
    def get_resource_mapping(cloud_name: str):
        resource_map_filename = \
            MAPPING_PATH / f'resources_map_{cloud_name.lower()}.json'
        if not os.path.exists(resource_map_filename):
            raise ExecutorException(
                reason=f'Invalid path: {resource_map_filename}',
                step_name='get resource mapping')
        _LOG.debug(f'Extracting resource map from file: '
                   f'\'{resource_map_filename}\'')
        with open(resource_map_filename, 'r') as f:
            return json.load(f)
