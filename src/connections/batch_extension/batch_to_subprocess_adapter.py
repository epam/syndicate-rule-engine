import os
import pathlib
import subprocess
import sys

import psutil

from helpers import generate_id, build_response
from helpers.constants import BATCH_ENV_SUBMITTED_AT, BATCH_ENV_JOB_ID
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.job import Job

_LOG = get_logger(__name__)
file_path = pathlib.Path(__file__).parent.resolve()

src = file_path.parent.parent
root = src.parent
docker_path = root / 'executor'
executor_path = docker_path / 'executor.py'

EXECUTABLE_VENV_FOLDER_PRIORITY = ['.executor_venv', 'venv']


class BatchToSubprocessAdapter:
    """
    Before using this class, please set up next env vars:
    VENV_PATH - path to python.exe in venv folder
    EXECUTOR_PATH - path to executor.py to run jobs
    set max_number_of_jobs in aliases
    job_id - will be unique id of the job
    pid - id of process that is now running job
    """

    def __init__(self, max_number_of_jobs):
        self._processes = {}
        self.max_jobs_count = max_number_of_jobs

    @property
    def processes(self):
        _temp = self._processes.copy()
        for job_id, pid in _temp.items():
            if not self.check_if_process_is_running(pid):
                self._processes.pop(job_id)
        return self._processes

    @staticmethod
    def resolve_executor_path() -> str:
        """
        Priorities:
        1. EXECUTOR_PATH env
        2. custodian-as-a-service/executor/executor.py
        """
        return os.environ.get('EXECUTOR_PATH') or str(executor_path)

    @staticmethod
    def resolve_executor_venv() -> str:
        """
        Priorities
        1. VENV_PATH env
        2. custodian-as-a-service/docker/.executor_venv/bin/python
        3. custodian-as-a-service/docker/venv/bin/python
        4. sys.executable
        """
        from_env = os.environ.get('VENV_PATH')
        if from_env:
            return from_env
        for folder in EXECUTABLE_VENV_FOLDER_PRIORITY:
            venv_path = docker_path / folder
            if venv_path.exists():
                return str(venv_path / 'bin/python')
        return sys.executable

    def submit_job(self, job_name: str = None, command: str = None,
                   environment_variables: dict = None):
        environment_variables = environment_variables or {}
        self.check_ability_to_start_job()

        job_id = generate_id()
        path_to_venv = self.resolve_executor_venv()
        path_to_executor = self.resolve_executor_path()

        # Popen raises TypeError in case there is an env where value is None
        env = {k: v for k, v in {
            **os.environ,
            **environment_variables,
            BATCH_ENV_JOB_ID: job_id,
            BATCH_ENV_SUBMITTED_AT: utc_iso()  # for scheduled jobs
        }.items() if v}

        _LOG.info('Executing sub-process')
        process = subprocess.Popen([
            path_to_venv, path_to_executor
        ], env=env, shell=False)

        self.processes[job_id] = process.pid
        return {'jobId': job_id}

    def terminate_job(self, job_id, reason="Terminating job."):
        if job_id in self.processes:
            pid = self.processes[job_id]
            p = psutil.Process(pid)
            p.terminate()

            self.processes.pop(job_id)

    @staticmethod
    def check_if_process_is_running(pid) -> bool:
        return psutil.pid_exists(pid)

    def check_ability_to_start_job(self):
        if len(self.processes) >= int(self.max_jobs_count):
            return build_response(
                content="The maximum number of jobs has already been started",
                code=400
            )

    @staticmethod
    def describe_jobs(jobs: list):
        result = []
        for job in jobs:
            job_item = Job.get_nullable(hash_key=job)
            if job_item:
                result.append(job_item.attribute_values)
        return result
