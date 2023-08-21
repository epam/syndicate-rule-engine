class ExecutorException(Exception):

    def __init__(self, step_name, reason):
        self.step_name = step_name
        self.reason = reason

    def __str__(self):
        return f'Error occurred on \'{self.step_name}\' step: {self.reason}'
