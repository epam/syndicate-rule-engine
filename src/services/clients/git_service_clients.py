import shutil
import tempfile
from functools import cached_property
from pathlib import Path
from typing import TypedDict
from urllib.parse import urljoin

import requests
from urllib3.util import parse_url, Url

from helpers import deep_get
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

GIT_BLAME_QUERY = """
query($owner:String!, $repo:String!, $ref:String!, $filepath:String!) {
    repository(owner: $owner, name: $repo) {
        object(expression: $ref) {
            ... on Commit {
                blame(path: $filepath) {
                    ranges {
                        commit {
                            committedDate
                            oid
                        }
                        age
                    }
                }
            }
        }
    }
}
"""


class GitLabClientLib:
    """
    Our wrapper over GitLab wrapper
    """

    @staticmethod
    def extract_netloc(path: str) -> str:
        """
        https://git.epam.com/one/two -> https://git.epam.com
        Hoping that the provided path contains scheme and netloc
        :param path:
        :return:
        """
        parsed: Url = parse_url(path)
        scheme = parsed.scheme
        if not scheme:
            scheme = 'https'
        return scheme + '://' + parsed.netloc

    def __init__(self, url: str, private_token: str):
        self._url = self.extract_netloc(url)
        self._private_token = private_token

    @cached_property
    def client(self):
        """
        :return:
        :rtype: gitlab.Gitlab
        """
        import gitlab
        return gitlab.Gitlab(
            url=self._url,
            private_token=self._private_token
        )

    def get_project(self, pid: int):
        """
        :param pid:
        :return:
        :rtype: Optional[gitlab.base.RESTObject]
        """
        from gitlab.exceptions import GitlabError
        try:
            return self.client.projects.get(pid)
        except GitlabError as e:
            _LOG.warning('Error occurred trying to get project')
            return

    def clone_project(self, project: str | int, to: Path,
                      ref: str | None = None) -> Path | None:
        """
        In case the project was cloned, returns a path to its root
        >>> with tempfile.TemporaryDirectory() as folder:
        >>>     root = GitLabClient().clone_project(123, folder, 'master')
        :param project: gitlab project ID
        :param ref:
        :param to:
        :return:
        """
        pr = self.get_project(project)
        if not pr:
            _LOG.debug('Cannot clone project. It was not found')
            return
        _LOG.debug(f'Going to clone {project}:{ref}')
        with tempfile.NamedTemporaryFile(delete=False, dir=to) as file:
            pr.repository_archive(
                streamed=True,
                action=file.write,
                sha=ref,
            )
        extracted = to / 'extracted'
        shutil.unpack_archive(file.name, extracted, format='tar')
        _LOG.debug('Repository was cloned')
        return next(Path(extracted).iterdir())


class GitLabClient:
    """
    Our wrapper over GitLab api, without python-gitlab
    """

    class _GitLabFileMeta(TypedDict):
        """
        HEAD /api/v4/projects/:id/repository/files/:path?ref=master
        """
        blob_id: str
        commit_id: str
        content_sha256: str
        encoding: str
        file_name: str
        file_path: str
        last_commit_id: str
        ref: str
        size: int
        execute_filemode: bool

    _session: requests.Session = None

    def __init__(self, url: str | None = 'https://git.epam.com',
                 private_token: str | None = None):
        self._url = self.extract_netloc(url)
        self._private_token = private_token

    @staticmethod
    def extract_netloc(path: str) -> str:
        """
        https://git.epam.com/one/two -> https://git.epam.com
        Hoping that the provided path contains scheme and netloc
        :param path:
        :return:
        """
        parsed: Url = parse_url(path)
        scheme = parsed.scheme
        if not scheme:
            scheme = 'https'
        return scheme + '://' + parsed.netloc

    @classmethod
    def api_prefix(cls) -> str:
        return '/api/v4/'

    def _api_url(self, path: str) -> str:
        """
        "/projects/123/repository/archive" ->
        https://git.epam.com/api/v4/projects/123/repository/archive
        :param path:
        :return:
        """
        path = '/'.join(map(lambda x: x.strip('/'), (self.api_prefix(), path)))
        return urljoin(self._url, path)

    @classmethod
    def session(cls) -> requests.Session:
        if not isinstance(cls._session, requests.Session):
            cls._session = requests.Session()
        # todo maybe use the same requests session for all the clients
        return cls._session

    @classmethod
    def close(cls):
        if isinstance(cls._session, requests.Session):
            cls._session.close()

    def get_project(self, project: int | str) -> dict | None:
        resp = self.session().get(
            url=self._api_url(f'projects/{project}'),
            headers={
                'PRIVATE-TOKEN': self._private_token
            } if self._private_token else {},
        )
        if not resp.ok:
            return
        return resp.json()

    def clone_project(self, project: str | int, to: Path,
                      ref: str | None = None) -> Path | None:
        """
        In case the project was cloned, returns a path to its root
        >>> with tempfile.TemporaryDirectory() as folder:
        >>>     root = GitLabClient().clone_project(123, folder, 'master',)
        :param project: gitlab project ID
        :param ref:
        :param to:
        :return:
        """
        _LOG.debug(f'Going to clone {project} from GitLab')
        resp = self.session().get(
            url=self._api_url(f'projects/{project}/repository/archive'),
            headers={
                'PRIVATE-TOKEN': self._private_token
            } if self._private_token else {},
            params={'sha': ref} if ref else None,
            stream=True
        )  # throttling or errors not handled
        if not resp.ok:
            _LOG.debug('Cannot clone project. It was not found')
            return
        _LOG.debug(f'Going to stream project data to temp file')
        with tempfile.NamedTemporaryFile(delete=False, dir=to) as file:
            for chunk in resp.iter_content(1024):
                file.write(chunk)
        # in theory, we could've requested zip archive and unpack it on-fly by
        #  chunks without dumping
        extracted = to / 'extracted'
        shutil.unpack_archive(file.name, extracted, format='tar')
        _LOG.debug('Repository was cloned')
        return next(Path(extracted).iterdir())

    def get_file_meta(self, project: int | str, filepath: str,
                      ref: str | None = None) -> _GitLabFileMeta | None:
        """
        Makes just HEAD to /api/v4/projects/:id/repository/files/:path.
        Main purpose to retrieve commit_hash and updated_date for a file
        :param project:
        :param filepath:
        :param ref:
        :return:
        """
        filepath = filepath.strip('/').replace('/', '%2F').replace('.', '%2E')
        _LOG.debug(f'Going to get file meta: {filepath}')
        resp = self.session().head(
            url=self._api_url(
                f'projects/{project}/repository/files/{filepath}'),
            headers={
                'PRIVATE-TOKEN': self._private_token
            } if self._private_token else {},
            params={'ref': ref} if ref else None,
        )
        if not resp.ok:
            return
        return {
            'blob_id': resp.headers.get('X-Gitlab-Blob-Id'),
            'commit_id': resp.headers.get('X-Gitlab-Commit-Id'),
            'content_sha256': resp.headers.get('X-Gitlab-Content-Sha256'),
            'encoding': resp.headers.get('X-Gitlab-Encoding'),
            'file_name': resp.headers.get('X-Gitlab-File-Name'),
            'file_path': resp.headers.get('X-Gitlab-File-Path'),
            'last_commit_id': resp.headers.get('X-Gitlab-Last-Commit-Id'),
            'ref': resp.headers.get('X-Gitlab-Ref'),
            'size': int(resp.headers.get('X-Gitlab-Size')),
            'execute_filemode': resp.headers.get('X-Gitlab-Execute-Filemode')
        }


class _GitHubBlameRangeCommit(TypedDict):
    committedDate: str
    oid: str  # commit hash


class _GitHubBlameRange(TypedDict):
    commit: _GitHubBlameRangeCommit
    age: int


class GitHubClient:
    _session: requests.Session = None

    @staticmethod
    def extract_netloc(path: str) -> str:
        """
        https://git.epam.com/one/two -> https://git.epam.com
        Hoping that the provided path contains scheme and netloc
        :param path:
        :return:
        """
        parsed: Url = parse_url(path)
        scheme = parsed.scheme
        if not scheme:
            scheme = 'https'
        return scheme + '://' + parsed.netloc

    def __init__(self, url: str | None = 'https://api.github.com',
                 private_token: str | None = None):
        self._url = self.extract_netloc(url)
        self._private_token = private_token

    @property
    def has_token(self) -> bool:
        return bool(self._private_token)

    @classmethod
    def session(cls) -> requests.Session:
        if not isinstance(cls._session, requests.Session):
            cls._session = requests.Session()
            cls._session.headers = {
                'X-GitHub-Api-Version': '2022-11-28',
                'Accept': 'application/vnd.github+json',
            }
        return cls._session

    @classmethod
    def close(cls):
        if isinstance(cls._session, requests.Session):
            cls._session.close()

    def get_project(self, project: str) -> dict | None:
        project = project.strip('/')
        resp = self.session().get(url=urljoin(self._url, f'/repos/{project}'))
        if not resp.ok:
            return
        return resp.json()

    def clone_project(self, project: str, to: Path, ref: str | None = None,
                      ) -> Path | None:
        """
        :param project: GitHub project full name: '[owner]/[repo]'
        :param ref:
        :param to:
        :return:
        """
        project = project.strip('/')
        assert project.count('/') == 1, 'invalid project full name'
        path = f'/repos/{project}/tarball'
        if ref:
            path += f'/{ref}'
        _LOG.debug(f'Going to clone {project} from GitHub')
        resp = self.session().get(
            url=urljoin(self._url, path),
            stream=True
        )  # makes redirect
        if not resp.ok:
            _LOG.debug('Cannot clone project. It was not found')
            return
        _LOG.debug(f'Going to stream project data to temp file')
        with tempfile.NamedTemporaryFile(delete=False, dir=to) as file:
            for chunk in resp.iter_content(1024):
                file.write(chunk)
        # in theory, we could've requested zip archive and unpack it on-fly by
        #  chunks without dumping
        extracted = to / 'extracted'
        shutil.unpack_archive(file.name, extracted, format='tar')
        _LOG.debug('Repository was cloned')
        return next(Path(extracted).iterdir())

    def get_file_blame(self, project: str, filepath: str, ref: str
                       ) -> list[_GitHubBlameRange]:
        project = project.strip('/')
        owner, repo = project.split('/')
        variables = {
            "owner": owner,
            "repo": repo,
            "ref": ref,
            "filepath": filepath.lstrip('/')
        }
        resp = self.run_query(GIT_BLAME_QUERY, variables)
        if not resp:
            return []
        return deep_get(
            resp, ('data', 'repository', 'object', 'blame', 'ranges')
        ) or []

    @staticmethod
    def most_reset_blame(blames: list[_GitHubBlameRange]) -> _GitHubBlameRange:
        assert blames, 'At least one blame dict must be provided'
        # assuming if there is no age, it is old
        return min(blames, key=lambda x: x.get('age') or 10)

    def run_query(self, query: str, variables: dict) -> dict | None:
        """
        Runs the provided query and returns a raw response
        :param variables:
        :param query:
        :return:
        """
        if not self.has_token:
            raise AssertionError('Invalid usage. Graphql requires token')
        resp = self.session().post(
            url=urljoin(self._url, '/graphql'),
            json={'query': query, 'variables': variables},
            headers={'Authorization': f'Bearer {self._private_token}'}
        )
        if not resp.ok:
            _LOG.warning(f'Something went wrong making graphql '
                         f'query on Github: {resp.text}')
            return
        return resp.json()

    def get_file_meta(self, project: str, filepath: str,
                      ref: str | None = None):
        return
