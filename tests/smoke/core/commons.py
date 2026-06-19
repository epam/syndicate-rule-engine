import datetime
import json
import logging
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import Any, cast

from smoke.core.settings import get_settings

_handler = logging.StreamHandler()
_handler.setLevel(logging.INFO)
_LOG = logging.getLogger()
_LOG.setLevel(logging.INFO)
_LOG.addHandler(_handler)

JSON_PATH_LIST_INDEXES = re.compile(r'\w*\[(-?\d+)\]')
JSON_PATH_PATTERN = re.compile(r'(\$\.[\w\d\[\]\.]+)')
OPTIONS_TO_HIDE = {
    '-sk',
    '--aws_secret_access_key',
    '-st',
    '--aws_session_token',
    '-secret',
    '--git_access_secret',
    '--password',
    '-gsecret',
    '-p',
}
SECRET_REPLACEMENT = '****'

COLOR_TEXT_MARKDOWN = '<span style="color:{color}">**{text}**</span>'
PASSED_MARKDOWN = COLOR_TEXT_MARKDOWN.format(color='green', text='passed')
FAILED_MARKDOWN = COLOR_TEXT_MARKDOWN.format(color='red', text='failed')
FAILED_EXPLANATION_TEMPLATE = """#### Output:
```json
{output}
```
#### Expected:
```json
{expected}
```
"""
CASE_TEMPLATE = """### {name}
{steps}
"""

LOG_NOT_ALLOWED_TO_EXECUTE_STEP = (
    'The steps if not allowed to be executed. '
    'Some previous steps have not succeeded'
)

SRC_FOLDER = 'src'


class TermColor:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @classmethod
    def blue(cls, st: str) -> str:
        return f'{cls.OKBLUE}{st}{cls.ENDC}'

    @classmethod
    def cyan(cls, st: str) -> str:
        return f'{cls.OKCYAN}{st}{cls.ENDC}'

    @classmethod
    def green(cls, st: str) -> str:
        return f'{cls.OKGREEN}{st}{cls.ENDC}'

    @classmethod
    def yellow(cls, st: str) -> str:
        return f'{cls.WARNING}{st}{cls.ENDC}'

    @classmethod
    def red(cls, st: str) -> str:
        return f'{cls.FAIL}{st}{cls.ENDC}'


def magic_get(d: dict | list, path: str) -> Any:
    """
    Simple json paths with only basic operations supported. This should be
    enough for the current smoke tests. If it becomes not enough you are
    welcome to use some external lib.
    If the path not found, None is returned.
    >>> magic_get({'a': 'b', 'c': [1,2,3, [{'b': 'c'}]]}, 'c[-1][0].b')
    'c'
    >>> magic_get([-1, {'one': 'two'}], 'c[-1][0].b') is None
    True
    >>> magic_get([-1, {'one': 'two'}], '[-1].one')
    'two'
    """
    if path.startswith('$'):
        path = path[1:]
    if path.startswith('.'):
        path = path[1:]
    parts = path.split('.')

    item: Any = d
    for part in parts:
        try:
            _key = part.split('[')[0]
            _indexes = re.findall(JSON_PATH_LIST_INDEXES, part)
            if _key:
                item = item.get(_key)
            for i in _indexes:
                item = item[int(i)]
        except (IndexError, TypeError, AttributeError):
            item = None
            break
    return item


def resolve_dynamic_params(string: str, data: Sequence) -> str:
    """
    >>> string = 'sre job describe -id $.[0].items[0].job_id'
    >>> data = [{'items': [{'job_id': 'uuid'}]}]
    >>> resolve_dynamic_params(string, data)
    'sre job describe -id uuid'
    """
    result = string
    paths = re.findall(JSON_PATH_PATTERN, string)
    for path in paths:
        resolved = magic_get(cast(dict | list, data), path)
        result = result.replace(path, resolved if resolved is not None else '')
    return result


def parse_cli_json(raw: str) -> dict[str, Any]:
    """Parse the first JSON object from CLI stdout.

    The SRE CLI may append a non-JSON hints block after ``--json`` output
    (e.g. presigned download curl commands for large reports).
    """
    stripped = raw.strip()
    if not stripped:
        return {}
    payload, _ = json.JSONDecoder().raw_decode(stripped)
    if isinstance(payload, dict):
        return payload
    return {'data': payload}


class Condition(ABC):
    def __init__(self) -> None:
        pass

    @staticmethod
    def assert_condition(maybe_condition: Any) -> None:
        if not isinstance(maybe_condition, Condition):
            raise TypeError(f'{maybe_condition} must be a condition')

    def __and__(self, other: 'Condition') -> 'And':
        self.assert_condition(other)
        return And(self, other)

    def __or__(self, other: 'Condition') -> 'Or':
        self.assert_condition(other)
        return Or(self, other)

    @abstractmethod
    def check(self, value: Any) -> bool: ...

    @abstractmethod
    def __repr__(self) -> str: ...


class Not(Condition):
    def __init__(self, condition: Condition):
        super().__init__()
        self._condition = condition

    def check(self, value: Any) -> bool:
        return not self._condition.check(value=value)

    def __repr__(self) -> str:
        return f'NOT {self._condition.__repr__()}'


class And(Condition):
    def __init__(self, left: Condition, right: Condition):
        super().__init__()
        self._left = left
        self._right = right

    def check(self, value: Any) -> bool:
        return self._left.check(value) and self._right.check(value)

    def __repr__(self) -> str:
        return f'({self._left.__repr__()} AND {self._right.__repr__()})'


class Or(Condition):
    def __init__(self, left: Condition, right: Condition):
        super().__init__()
        self._left = left
        self._right = right

    def check(self, value: Any) -> bool:
        return self._left.check(value) or self._right.check(value)

    def __repr__(self) -> str:
        return f'({self._left.__repr__()} OR {self._right.__repr__()})'


class In(Condition):
    def __init__(self, *values: Any):
        super().__init__()
        self._values = set(values)

    def check(self, value: Any) -> bool:
        return value in self._values

    def __repr__(self) -> str:
        return f'IN [{", ".join(map(str, self._values))}]'


class Contains(Condition):
    def __init__(self, value: str):
        super().__init__()
        self._value = value

    def check(self, value: Any) -> bool:
        return isinstance(value, str) and self._value in value

    def __repr__(self) -> str:
        return f'*{self._value}*'


class ListContains(Condition):
    def __init__(self, value: Any):
        super().__init__()
        self._value = value

    def check(self, value: list) -> bool:
        return self._value in (value or set())

    def __repr__(self) -> str:
        return f'HAS {self._value}'


class Equal(Condition):
    def __init__(self, value: Any):
        super().__init__()
        self._value = value

    def check(self, value: Any) -> bool:
        return bool(value == self._value)

    def __repr__(self) -> str:
        return str(self._value)


class False_(Condition):
    def check(self, value: bool) -> bool:
        return isinstance(value, bool) and not value

    def __repr__(self) -> str:
        return str(False).lower()


class True_(Condition):
    def check(self, value: Any) -> bool:
        return not False_().check(value)

    def __repr__(self) -> str:
        return str(True).lower()


class NotEmpty(Condition):
    def check(self, value: Any) -> bool:
        """
        int:0 and bool:False are not considered empty here
        """
        return bool(value) if not isinstance(value, (int, bool)) else True

    def __repr__(self) -> str:
        return 'EXISTS'


class Empty(Condition):
    def check(self, value: Any) -> bool:
        return not NotEmpty().check(value)

    def __repr__(self) -> str:
        return 'NOT EXIST'


class Len(Condition):
    def __init__(self, operator: Callable[[int, int], bool], _len: int):
        super().__init__()
        self._operator = operator
        self._len = _len

    def check(self, value: Sequence) -> bool:
        return isinstance(value, Sequence) and self._operator(
            len(value), self._len
        )

    def __repr__(self) -> str:
        return f'LEN {self._operator.__name__} {self._len}'


class IsInstance(Condition):
    def __init__(self, *types: type):
        super().__init__()
        self._types = types

    def check(self, value: Any) -> bool:
        return isinstance(value, tuple(self._types))

    def __repr__(self) -> str:
        type_names = ', '.join(t.__name__ for t in self._types)
        return f'IS INSTANCE ANY OF: {type_names}'


Expectations = dict[str, Condition]


class AbstractStep(ABC):
    """
    Abstract step class which handles expectations logic. Basically it
    executes some action which returns JSON and validates this JSON based
    on certain conditions. So-called `expectations` are these conditions.
    Expectations is a dict where a key is json-paths string and a value
    is instance of class __main__.Condition.
    Valid json-paths:
        - '$.key1.key2[0][2][-1].key3',
        - '.key1.key2[0][2][-1].key3',
        - 'key1.key2[0][2][-1].key3'

    expectations = {
        str: Condition
    }
    Each expectations' key will be retrieved from the output JSON and
    validated through a condition
    """

    def __init__(
        self,
        expectations: Expectations | None = None,
        depends_on: Sequence['AbstractStep'] | None = None,
        delay: int | None = None,
    ):
        self._expectations = expectations or {}
        self._output: dict = {}
        self._is_finished: bool = False
        self._is_succeeded: bool | None = None
        self._depends_on: tuple['AbstractStep', ...] = tuple(depends_on or [])
        settings = get_settings()
        default_delay = settings.step_delay
        self._delay = delay if delay is not None else default_delay

    def is_allowed(self) -> bool:
        for dependent_step in self._depends_on:
            if not dependent_step.succeeded:
                return False
        return True

    def _resolve_dynamic(self, target_string: str) -> str:
        return resolve_dynamic_params(
            target_string,
            tuple(step.output for step in self._depends_on),
        )

    @staticmethod
    def _check(data: dict, expectations: Expectations) -> bool:
        for path, condition in expectations.items():
            _to_check = magic_get(data, path)
            if not condition.check(_to_check):
                return False
        return True

    def _check_expectations(self, data: dict) -> bool:
        return self._check(data, self._expectations)

    def reset(self) -> None:
        self._output.clear()
        self._is_finished = False
        self._is_succeeded = None

    @property
    def succeeded(self) -> bool:
        if not self._is_finished:
            return False
        if not isinstance(self._is_succeeded, bool):
            self._is_succeeded = self._check_expectations(self._output)
        return self._is_succeeded

    @property
    def finished(self) -> bool:
        return self._is_finished

    @property
    def output(self) -> dict:
        return self._output

    def dump_expectations(self) -> str:
        almost_dumped = {
            path: str(condition)
            for path, condition in self._expectations.items()
        }
        return json.dumps(almost_dumped, indent=4)

    @abstractmethod
    def _execute(self, *args: Any, **kwargs: Any) -> str:
        """Returns json string. Makes one simple request based on class
        attributes and purpose"""

    def execute(self) -> None:
        if not self.is_allowed():
            _LOG.warning(LOG_NOT_ALLOWED_TO_EXECUTE_STEP)
            return
        self._output.clear()
        self._output.update(parse_cli_json(self._execute()))
        self._is_finished = True
        time.sleep(self._delay)

    @abstractmethod
    def report(self) -> str: ...


class Step(AbstractStep):
    """CLI step that runs the configured SRE CLI entry point."""

    def __init__(
        self,
        command: str,
        expectations: Expectations | None = None,
        depends_on: Sequence[AbstractStep] | None = None,
        json_flag: bool = True,
    ):
        super().__init__(expectations, depends_on)
        self._json_flag = json_flag
        self._command = self._adjust_command(command)

    def _adjust_command(self, command: str) -> str:
        if not self._json_flag:
            return command
        json_flag = '--json'
        if json_flag not in command:
            command += f' {json_flag}'
        return command

    def _execute(self, *args: Any, **kwargs: Any) -> str:
        process = subprocess.run(
            shlex.split(self._command),
            capture_output=True,
            check=False,
        )
        return process.stdout.decode()

    def execute(self) -> None:
        self._command = self._resolve_dynamic(self._command)
        _LOG.info(f'Executing in CLI: {self.command}')
        super().execute()

    @property
    def command(self) -> str:
        parts = shlex.split(self._command)
        result: list[str] = []
        i = 0
        while i < len(parts):
            part = parts[i]
            result.append(part)
            if part in OPTIONS_TO_HIDE:
                result.append(SECRET_REPLACEMENT)
                i += 2
            else:
                i += 1
        return ' '.join(result)

    def report(self) -> str:
        _succeeded = self.succeeded
        s = (
            f'`{self.command}` - '
            f'{PASSED_MARKDOWN if _succeeded else FAILED_MARKDOWN};'
        )
        if not _succeeded:
            s += '\n' + FAILED_EXPLANATION_TEMPLATE.format(
                output=json.dumps(self._output, indent=4),
                expected=self.dump_expectations(),
            )
        s += '\n\n'
        return s


class WaitUntil(Step):
    def __init__(
        self,
        command: str,
        expectations: Expectations | None = None,
        depends_on: Sequence[AbstractStep] | None = None,
        timeout: int = 900,
        sleep: int = 15,
        break_if: Expectations | None = None,
    ):
        super().__init__(command, expectations, depends_on)
        self._timeout = timeout
        self._sleep = sleep
        self._timed_out = False
        self._break_if = break_if or {}

    def execute(self) -> None:
        if not self.is_allowed():
            _LOG.warning(LOG_NOT_ALLOWED_TO_EXECUTE_STEP)
            return
        self._command = self._resolve_dynamic(self._command)

        def _request() -> dict[Any, Any]:
            return cast(dict[Any, Any], parse_cli_json(self._execute()))

        _LOG.info(f'Executing in CLI: {self.command}')

        _start = time.time()
        _result = _request()
        while not self._check_expectations(_result):
            if self._break_if and self._check(_result, self._break_if):
                _LOG.warning(
                    f"Command {self.command} has passes 'break if' "
                    f'condition. Going to the next step'
                )
                break
            if time.time() - _start > self._timeout:
                _LOG.warning(
                    f'Timeout exceeded. Command {self.command} was '
                    f'not in time. Going to the next step'
                )
                self._timed_out = True
                break
            time.sleep(self._sleep)
            _LOG.info(f'Executing in CLI: {self.command}')
            _result = _request()
        self._is_finished = True
        self._output.clear()
        self._output.update(_result)
        time.sleep(self._delay)

    def reset(self) -> None:
        super().reset()
        self._timed_out = False

    def report(self) -> str:
        report = super().report()
        if self._timed_out:
            report = (
                COLOR_TEXT_MARKDOWN.format(color='yellow', text='Timeout')
                + ' '
                + report
            )
        return report


class ApiStep(AbstractStep):
    def __init__(
        self,
        url: str,
        method: str = 'GET',
        data: dict | None = None,
        headers: dict | None = None,
        expectations: Expectations | None = None,
    ):
        super().__init__(expectations)
        self._url, self._method = url, method
        self._data, self._headers = data or {}, headers or {}

    def _execute(self, *args: Any, **kwargs: Any) -> str:
        _data = json.dumps(self._data).encode('utf8')
        _headers = {**self._headers, 'Content-Type': 'application/json'}
        request = urllib.request.Request(
            self._url, data=_data, headers=_headers, method=self._method
        )
        result = '{}'
        try:
            with urllib.request.urlopen(request) as response:
                result = response.read().decode()
        except urllib.error.HTTPError as error:
            result = error.fp.read().decode()
        except urllib.error.URLError as error:
            _LOG.error(
                f'Could not make the request: {error}. Returning an empty dict'
            )
        except TimeoutError:
            _LOG.warning(
                'Timeout occurred making the request. Returning an empty dict'
            )
        return result

    def execute(self) -> None:
        _LOG.info(f"Making '{self._method}' call to '{self._url}'")
        super().execute()

    def report(self) -> str:
        return 'in progress'


Steps = tuple[AbstractStep, ...]


class Case:
    def __init__(self, steps: Steps, name: str):
        self._steps = steps
        self._name = name
        self._done: list[AbstractStep] = []

    @property
    def finished(self) -> bool:
        return bool(self._done) and all(step.finished for step in self._done)

    @property
    def succeeded(self) -> bool:
        return bool(self._done) and all(step.succeeded for step in self._done)

    def reset(self) -> None:
        self._done.clear()
        for step in self._steps:
            step.reset()

    def execute(self) -> None:
        for step in self._steps:
            try:
                step.execute()
                self._done.append(step)
            except Exception as e:
                _LOG.warning(
                    f"Unexpected exception: '{e}' occurred while "
                    f'executing step: {vars(step)}'
                )

    def report(self) -> str:
        return CASE_TEMPLATE.format(
            name=self._name.title(),
            steps=''.join(s.report() for s in self._done),
        )


def write_cases(cases: list[Case], name: str | None = None) -> None:
    date = datetime.date.today().isoformat()
    report_name = name or f'smoke-report-{date}.md'
    with open(report_name, 'w') as file:
        file.write(f'## Syndicate Rule Engine smoke report {date}\n\n')
        for case in cases:
            file.write(case.report())
