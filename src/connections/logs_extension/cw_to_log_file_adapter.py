import os
import pathlib


class CWToLogFileAdapter:
    def __init__(self):
        file_path = pathlib.Path(__file__).parent.resolve()

        src = file_path.parent.parent
        root = src.parent
        self.logs_path = f'{root}/docker/logs'

    def get_log_events(self, job_id):
        logs_path = f'{self.logs_path}/{job_id}/error.log'
        if not os.path.exists(logs_path):
            return []
        with open(logs_path, 'r') as file:
            full_content = file.read()
        result = [{'message': line} for line in full_content.split(' ;\n')]
        return result
