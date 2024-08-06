from abc import ABC, abstractmethod
import argparse
import csv
import email
import email.message
import email.policy
import gzip
import importlib
import itertools
import json
import logging
import logging.config
import os
from pathlib import Path
import subprocess
import sys
from typing import Generator, List, Optional, TYPE_CHECKING, Tuple, Union, cast
import uuid

openpyxl = None
if TYPE_CHECKING:
    import openpyxl
    from openpyxl.worksheet.worksheet import Worksheet


__version__ = '1.0.0'


REPORT_FIELDS = {'id', 'name', 'arn', 'namespace'}


class TermColor:
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    FAIL = '\033[91m'
    DEBUG = '\033[90m'
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    UNDERLINE = '\033[4m'
    BOLD_RED = '\x1b[31;1m'

    _pattern = '{color}{string}' + ENDC

    @classmethod
    def blue(cls, st: str) -> str:
        return cls._pattern.format(color=cls.OKBLUE, string=st)

    @classmethod
    def cyan(cls, st: str) -> str:
        return cls._pattern.format(color=cls.OKCYAN, string=st)

    @classmethod
    def green(cls, st: str) -> str:
        return cls._pattern.format(color=cls.OKGREEN, string=st)

    @classmethod
    def yellow(cls, st: str) -> str:
        return cls._pattern.format(color=cls.WARNING, string=st)

    @classmethod
    def red(cls, st: str) -> str:
        return cls._pattern.format(color=cls.FAIL, string=st)

    @classmethod
    def gray(cls, st: str) -> str:
        return cls._pattern.format(color=cls.DEBUG, string=st)

    @classmethod
    def bold_red(cls, st: str) -> str:
        return cls._pattern.format(color=cls.BOLD_RED, string=st)


class ColorFormatter(logging.Formatter):
    formats = {
        logging.DEBUG: TermColor.gray,
        logging.INFO: TermColor.green,
        logging.WARNING: TermColor.yellow,
        logging.ERROR: TermColor.red,
        logging.CRITICAL: TermColor.bold_red
    }

    def format(self, record):
        res = super().format(record)
        return self.formats[record.levelno](res)


def get_logger(name: str, level=os.getenv('LOG_LEVEL', logging.DEBUG)):
    log = logging.getLogger(name)
    log.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter('%(levelname)s - %(message)s'))
    log.addHandler(handler)
    return log


_LOG = get_logger(__name__)


NoneType = type(None)
Leaf = Union[str, int, bool, NoneType]
JsonContainer = Union[list, dict]
Json = Union[Leaf, JsonContainer]


def iter_values(finding: Json) -> Generator[Leaf, Leaf, Json]:
    """
    Yields values from the given finding with an ability to send back
    the desired values. I proudly think this is cool, because we can put
    keys replacement login outside of this generator
    >>> gen = iter_values({'1':'q', '2': ['w', 'e'], '3': {'4': 'r'}})
    >>> next(gen)
    q
    >>> gen.send('instead of q')
    w
    >>> gen.send('instead of w')
    e
    >>> gen.send('instead of e')
    r
    >>> gen.send('instead of r')
    After the last command StopIteration will be raised, and it
    will contain the changed finding. The given finding will be changed
    in-place
    :param finding:
    :return:
    """
    if isinstance(finding, (str, int, bool, NoneType)):
        new = yield finding
        return new
    if isinstance(finding, dict):
        for k, v in finding.items():
            finding[k] = yield from iter_values(v)
        return finding
    if isinstance(finding, list):
        for i, v in enumerate(finding):
            finding[i] = yield from iter_values(v)
        return finding


def flip_dict(d: dict) -> None:
    """
    In place
    :param d:
    :return:
    """
    for k in tuple(d.keys()):
        d[d.pop(k)] = k


def keep_only(d: dict, keys: set) -> None:
    if not keys:
        return
    for k in tuple(d.keys()):
        if k not in keys:
            d.pop(k)


def query_yes_no(question: str, default: str = "yes") -> bool:
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write(
                "Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


def import_openpyxl() -> None:
    global openpyxl
    if openpyxl:
        return
    _LOG.info('Going to import openpyxl')
    try:
        openpyxl = importlib.import_module('openpyxl')
    except ImportError:
        if not query_yes_no('Required requirement openpyxl is not found. '
                            'Want to install?'):
            _LOG.error('Aborting...')
            sys.exit(1)
        subprocess.run(['pip', 'install', 'openpyxl'])
        openpyxl = importlib.import_module('openpyxl')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Obfuscation script cli enter-point'
    )
    # -- top level sub-parser
    sub_parsers = parser.add_subparsers(dest='action', required=True,
                                        help='Available actions')
    obfuscate_parser = sub_parsers.add_parser(
        'obfuscate', help='Obfuscates an existing dump'
    )
    obfuscate_parser.add_argument(
        '--dump-directory', default='custodian_dump', type=Path,
        help='Path to the folder where custodian dump is sited '
             '(default: %(default)s)'
    )
    obfuscate_parser.add_argument(
        '--to', required=False, type=Path,
        help='Path the the patched data must be placed. If not specified, '
             'pathed data will override an existing dump'
    )
    obfuscate_parser.add_argument(
        '--keep-all-fields', action='store_true',
        help='If specified, all the fields will be kept. By default, '
             'only ID fields are kept'
    )
    obfuscate_parser.add_argument(
        '--dictionary-out', default='dictionary_out.json', type=Path,
        help='Path where obfuscated keys and their IDs will be places '
             '(default: %(default)s)'
    )
    obfuscate_parser.add_argument(
        '--dictionary', dest='dictionary_path', required=False, type=Path,
        help='Optional dict, in case you want to give some specific '
             'aliases for concrete names in resources. '
             'Path to file which contains JSON with key-value pairs, '
             'where key is real value of some attribute in resources '
             f'and value is a name you want to replace the real value with. '
             f'(default: %(default)s)'
    )
    deobfuscate_parser = sub_parsers.add_parser(
        'deobfuscate', help='De-obfuscates an existing dump'
    )
    deobfuscate_parser.add_argument(
        '--dump-directory', default='custodian_dump', type=Path,
        help='Path to the folder where the obfuscated custodian dump is '
             'sited. Can be also a path to concrete file '
             '(default: %(default)s)'
    )
    deobfuscate_parser.add_argument(
        '--to', required=False, type=Path,
        help='Path where the de-obfuscated data must be placed. '
             'If not specified, pathed data will override an existing dump'
    )
    deobfuscate_parser.add_argument(
        '--dictionary', required=True, nargs='+', type=Path,
        help='Path to a file where obfuscated keys and their IDs are'
    )
    return parser


class ActionHandler(ABC):
    @abstractmethod
    def __call__(self, **kwargs):
        ...


class BaseHandler(ActionHandler):
    """
    Contains some common methods
    """

    def __call__(self, *args, **kwargs):
        raise NotImplementedError('BaseHandler must not be used directly')

    @staticmethod
    def is_gzipped_json(name: Path) -> bool:
        return name.suffixes == ['.json', '.gz']

    @staticmethod
    def is_json(name: Path) -> bool:
        return name.suffixes == ['.json']

    @staticmethod
    def is_xlsx(name: Path) -> bool:
        return name.suffixes == ['.xlsx']

    @staticmethod
    def is_csv(name: Path) -> bool:
        return name.suffixes == ['.csv']

    @staticmethod
    def load_json(path: Path, gzipped: Optional[bool] = None
                  ) -> Optional[Json]:
        if not isinstance(gzipped, bool):
            gzipped = BaseHandler.is_gzipped_json(path)
        try:
            with open(path, 'rb') as fp:
                data = fp.read()
                if gzipped:
                    data = gzip.decompress(data)
                return json.loads(data)
        except Exception as e:
            _LOG.warning(
                f'Unexpected error occurred trying to load {path}: {e}. '
                f'Skipping...'
            )
            return

    @staticmethod
    def dump_json(to: Path, data: Json, gzipped: Optional[bool] = None):
        if not isinstance(gzipped, bool):
            gzipped = BaseHandler.is_gzipped_json(to)
        to.parent.mkdir(parents=True, exist_ok=True)
        with open(to, 'wb') as fp:
            buf = json.dumps(data, separators=(',', ':')).encode()
            if gzipped:
                buf = gzip.compress(buf)
            fp.write(buf)

    @staticmethod
    def yield_files(root: Path) -> Generator[Tuple[Path, Path], None, None]:
        """
        Iterates over all the files in root and yield each file full
        path and path relative to root
        :param root: (full, relative)
        :return:
        """
        if root.is_file():
            yield root.resolve(), root.relative_to(root.parent)
            return
        for base, _, files in os.walk(root):
            for file in files:
                _path = Path(base, file)
                yield _path, _path.relative_to(root)


class ObfuscateDump(BaseHandler):

    @staticmethod
    def is_findings(dct) -> bool:
        """
        Is 'findings' format of report
        :param dct:
        :return:
        """
        if not isinstance(dct, dict):
            return False
        if not list(dct.keys())[0].startswith('ecc-'):
            return False
        return True

    @staticmethod
    def is_list_of_resources(lst) -> bool:
        """
        Is just list of resources
        :param lst:
        :return:
        """
        if not isinstance(lst, list):
            return False
        if len(lst) == 0:
            return True
        if not isinstance(lst[0], dict):
            return False
        return True

    @staticmethod
    def is_list_of_shard_parts(lst) -> bool:
        if not isinstance(lst, list):
            return False
        if len(lst) == 0:
            return True
        dct = lst[0]
        if not isinstance(dct, dict):
            return False
        return 'p' in dct and 'l' in dct and 't' in dct and 'r' in dct

    @staticmethod
    def obfuscate_finding(finding: dict, dictionary: dict,
                          dictionary_out: dict) -> None:
        """
        Main business logic
        :param finding:
        :param dictionary:
        :param dictionary_out:
        :return:
        """
        gen = iter_values(finding)
        try:
            real = next(gen)
            gen_id = uuid.uuid4
            while True:
                alias = dictionary_out.setdefault(
                    real, dictionary.get(real) or str(gen_id())
                )
                _LOG.debug(f'"{str(real)[:3]}***" will be '
                           f'replaced with {alias}')
                real = gen.send(alias)
        except StopIteration:
            pass

    def patch_findings(self, findings: dict, all_fields: bool,
                       dictionary: dict,
                       dictionary_out: dict):
        """
        In place
        :param findings:
        :param all_fields:
        :param dictionary:
        :param dictionary_out:
        :return:
        """
        for data in findings.values():
            for resources in data['resources'].values():
                for resource in resources:
                    if not all_fields:
                        keep_only(resource, REPORT_FIELDS)
                    self.obfuscate_finding(
                        resource,
                        dictionary,
                        dictionary_out
                    )

    def patch_list_of_resources(self, findings: list, all_fields: bool,
                                dictionary: dict, dictionary_out: dict):
        for resource in findings:
            if not all_fields:
                keep_only(resource, REPORT_FIELDS)
            self.obfuscate_finding(resource, dictionary, dictionary_out)

    def patch_list_of_shard_parts(self, findings: list, all_fields: bool,
                                  dictionary: dict, dictionary_out: dict):
        for part in findings:
            for resource in part.setdefault('r', []):
                if not all_fields:
                    keep_only(resource, REPORT_FIELDS)
                self.obfuscate_finding(resource, dictionary, dictionary_out)

    def yield_jsons(self, root: Path
                    ) -> Generator[Tuple[Path, Json], None, None]:
        """
        Yields tuples where the first element is file path relative to root,
        the second element - loaded file content.
        Loads only JSON and gzipped JSON. Skips the file if it cannot be
        loaded or not json
        :param root:
        :return:
        """
        for full, relative in self.yield_files(root):
            gz = self.is_gzipped_json(full)
            js = self.is_json(full)
            if not (gz or js):
                _LOG.info(f'Skipping: {relative} - not json')
                continue
            data = self.load_json(full)
            if not data:
                continue
            yield relative, data

    def __call__(self, dump_directory: Path, to: Optional[Path],
                 keep_all_fields: bool, dictionary_out: Path,
                 dictionary_path: Optional[Path]):
        # output dump directory validation
        if not to:
            if query_yes_no('Parameter --to was not provided. Patched files '
                            'will override the dump'):
                to = dump_directory
            else:
                _LOG.error('Aborting')
                sys.exit(1)
        _LOG.info(f'Obfuscated dump will be places to: "{to}"')

        # Loading desired values dictionary
        if dictionary_path:
            _LOG.info('Loading dictionary')
            try:
                with open(dictionary_path, 'r') as file:
                    dictionary = json.load(file)
                if not isinstance(dictionary, dict):
                    raise ValueError('The content must be a dict')
            except Exception as e:
                _LOG.error(f'Could not load {dictionary_path}: {e}')
                sys.exit(1)
        else:
            _LOG.info('Dictionary was not provided. All the '
                      'aliases will be randomly generated')
            dictionary = {}

        # Logging whether all the fields will be kept
        if keep_all_fields:
            _LOG.warning('All the fields will be kept')
        else:
            _LOG.info('Only id fields will be kept')

        # obfuscating
        out = {}  # here we will put real names to our randomly generated
        for path, content in self.yield_jsons(dump_directory):
            if self.is_findings(content):
                _LOG.info(f'Findings found by path: {path}. Pathing')
                content = cast(dict, content)  # is_findings ensures it's dict
                self.patch_findings(content, keep_all_fields, dictionary, out)
            elif self.is_list_of_shard_parts(content):
                _LOG.info(f'List of shard parts found by path: {path}. '
                          f'Pathing')
                content = cast(list, content)
                self.patch_list_of_shard_parts(content, keep_all_fields,
                                               dictionary, out)
            elif self.is_list_of_resources(content):
                _LOG.info(f'List of resources found by path: {path}. Pathing')
                content = cast(list, content)
                self.patch_list_of_resources(content, keep_all_fields,
                                             dictionary, out)
            else:
                _LOG.warning(f'Unknown file format: {path}. Skipping')
            self.dump_json(to / path, content)

        # dumping output dict
        _LOG.info(f'Output dictionary will be dumped to {dictionary_out}')
        dictionary_out.parent.mkdir(parents=True, exist_ok=True)
        flip_dict(out)
        with open(dictionary_out, 'w') as file:
            json.dump(out, file, indent=2)
        _LOG.info('Finished!')


class Deobfuscator(ABC):
    """
    >>> Deobfuscator().deobfuscate(Path('here')).to(Path('there')).using({})
    """
    _what: Union[Path, None] = None
    _to: Union[Path, None] = None

    def __init__(self, *args, **kwargs):
        pass

    def deobfuscate(self, what: Path) -> 'Deobfuscator':
        self._what = what
        return self

    def to(self, to: Path) -> 'Deobfuscator':
        self._to = to
        return self

    def using(self, dictionary: dict) -> None:
        assert self._what and self._to, 'Invalid calls chain'
        self._make_it(self._what, self._to, dictionary)

    @staticmethod
    def _deobfuscate_str(item: str, dictionary: dict) -> str:
        for k, v in dictionary.items():
            # todo think another more efficient way
            item = item.replace(k, str(v))
        return item

    @staticmethod
    def _deobfuscate_finding(finding: Json, dictionary: dict) -> None:
        """
        Deobfuscates one json item.
        :param finding:
        :param dictionary:
        :return:
        """
        gen = iter_values(finding)
        try:
            alias = next(gen)
            while True:
                if alias not in dictionary:
                    _LOG.warning(f'{alias} will not be replaced because '
                                 f'there is not corresponding value in '
                                 f'the dictionary')
                    alias = gen.send(alias)
                    continue

                real = dictionary[alias]
                _LOG.debug(f'{alias} will be '
                           f'replaced with "{str(real)[:3]}***"')
                alias = gen.send(real)
        except StopIteration:
            pass

    def _deobfuscate_maybe_json(self, item: str, dictionary: dict) -> str:
        try:
            data = json.loads(item)
            self._deobfuscate_finding(data, dictionary)
            return json.dumps(data, separators=(',', ':'))
        except json.JSONDecodeError:
            return self._deobfuscate_str(item, dictionary)

    def _deobfuscate_line(self, ln: List[str], dictionary: dict) -> list:
        """
        Returns the same object as received
        """
        for i, item in enumerate(ln):
            ln[i] = self._deobfuscate_maybe_json(item, dictionary)
        return ln

    @abstractmethod
    def _make_it(self, what: Path, to: Path, dictionary: dict):
        """
        Should create a deobfuscated file
        :param what:
        :param to:
        :param dictionary:
        :return:
        """


class JsonDeobfuscator(Deobfuscator):
    def _make_it(self, what: Path, to: Path, dictionary: dict):
        data = BaseHandler.load_json(what)
        self._deobfuscate_finding(data, dictionary)
        BaseHandler.dump_json(to, data)


class XlsxDeobfuscator(Deobfuscator):
    def _deobfuscate_worksheet(self, wsh: 'Worksheet', dictionary: dict):
        for row in wsh.rows:
            for cell in row:
                if cell.value is None:  # isinstance(cell, MergedCell)
                    continue  # MergedCell
                if not isinstance(cell.value, str):
                    continue
                cell.value = self._deobfuscate_maybe_json(cell.value,
                                                          dictionary)

    def _make_it(self, what: Path, to: Path, dictionary: dict):
        wb = openpyxl.load_workbook(what)
        for wsh in wb:
            _LOG.debug(f'Deobfuscating {wsh.title} worksheet')
            self._deobfuscate_worksheet(wsh, dictionary)
        wb.save(to)


class CsvDeobfuscator(Deobfuscator):
    def _make_it(self, what: Path, to: Path, dictionary: dict):
        f1 = open(what, 'r', newline='')
        f2 = open(to, 'w', newline='')
        reader = csv.reader(f1)
        writer = csv.writer(f2, dialect=reader.dialect)
        writer.writerows(
            map(self._deobfuscate_line, reader, itertools.repeat(dictionary))
        )
        f1.close()
        f2.close()


class EmlDeobfuscator(Deobfuscator):
    def _deobfuscate_text_part(self, part: email.message.EmailMessage,
                               dictionary: dict):
        charset = part.get_content_charset()
        content = self._deobfuscate_maybe_json(part.get_content(), dictionary)
        part.set_content(
            content.encode(),
            maintype=part.get_content_maintype(),
            subtype=part.get_content_subtype(),
            cte=part.get('Content-Transfer-Encoding'),
            disposition=part.get_content_disposition(),
            filename=part.get_filename(),
            cid=part.get('Content-ID'),
        )
        part.set_charset(charset)

    def _make_it(self, what: Path, to: Path, dictionary: dict):
        with open(what, 'rb') as file:
            msg = cast(
                email.message.EmailMessage,
                email.message_from_binary_file(file,
                                               policy=email.policy.default)
            )
        for part in msg.walk():
            # multipart/* are just containers
            if part.get_content_maintype() == 'multipart':
                continue
            ct = part.get_content_type()
            if ct in ('text/plain', 'text/html', 'text/csv', 'application/json'):
                self._deobfuscate_text_part(part, dictionary)

        with open(to, 'wb') as fp:
            fp.write(msg.as_bytes())


class DeobfuscatorFactory:
    def __init__(self, path: Path):
        self._path = path

    @classmethod
    def path(cls, path: Path):
        return cls(path)

    def build(self, *args, **kwargs) -> Union[Deobfuscator, None]:
        suffixes = self._path.suffixes
        if suffixes == ['.xlsx']:
            import_openpyxl()
            return XlsxDeobfuscator(*args, **kwargs)
        elif suffixes == ['.csv']:
            return CsvDeobfuscator(*args, **kwargs)
        elif suffixes == ['.json', '.gz'] or suffixes == ['.json']:
            return JsonDeobfuscator(*args, **kwargs)
        elif suffixes == ['.eml'] or suffixes == ['.emltpl']:
            return EmlDeobfuscator(*args, **kwargs)
        return


class DeObfuscateDump(BaseHandler):

    def __call__(self, dump_directory: Path, to: Optional[Path],
                 dictionary: List[Path]):
        if not to:
            if query_yes_no('Parameter --to was not provided. '
                            'De-obfuscated files will override the dump'):
                to = dump_directory
            else:
                _LOG.error('Aborting')
                sys.exit(1)
        if dump_directory.is_dir() and to.is_file():
            _LOG.error('If --dump-directory is a directory --to must '
                       'be a directory as well')
            sys.exit(1)
        if dump_directory.is_file():
            to.parent.mkdir(parents=True, exist_ok=True)
            to.touch()
        _LOG.info('Loading dictionary')
        dct = {}
        for i in dictionary:
            try:
                with open(i, 'r') as file:
                    dct.update(json.load(file))
            except Exception as e:
                _LOG.error(f'Could not load {i}: {e}')

        for full, relative in self.yield_files(dump_directory):
            _LOG.info(f'De-obfuscating {full}')
            _path = to if to.is_file() else to / relative
            _path.parent.mkdir(parents=True, exist_ok=True)
            deobfuscator = DeobfuscatorFactory(relative).build()
            if not deobfuscator:
                _LOG.warning(f'Not supported file type: {relative}')
                continue
            deobfuscator.deobfuscate(full).to(_path).using(dct)
        _LOG.info('Done!')


def main():
    arguments = build_parser().parse_args()
    mapping = {'obfuscate': ObfuscateDump(), 'deobfuscate': DeObfuscateDump()}
    func = mapping[arguments.action]
    delattr(arguments, 'action')
    func(**vars(arguments))


if __name__ == '__main__':
    main()
