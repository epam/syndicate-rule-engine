import json
from pathlib import Path
from re import finditer
from typing import Union, Optional, List, Dict

from helpers.constants import HTTPMethod

API_GATEWAY_RESOURCE_TYPE = 'api_gateway'
S3_BUCKET_RESOURCE_TYPE = 's3_bucket'


class DeploymentResourcesParser:
    def __init__(self, file_path: Union[str, Path]):
        self._filename = file_path
        self._content: Optional[dict] = None

    @property
    def content(self) -> dict:
        if not self._content:
            with open(self._filename, 'r') as file:
                self._content = json.load(file)
        return self._content

    def get_api_gateway(self) -> List[Dict]:
        api_gateway_dr = []
        for name, meta in self.content.items():
            if meta.get('resource_type') == API_GATEWAY_RESOURCE_TYPE:
                api_gateway_dr.append(self.content[name])
        return api_gateway_dr

    def generate_api_config(self) -> Dict[str, Dict]:
        config = {}
        ig_meta = self.get_api_gateway()[0]
        stage = ig_meta.get('deploy_stage')

        for url, resource_meta in ig_meta.get('resources', {}).items():
            endpoint_methods = self.get_endpoint_methods(resource_meta)
            lambda_name = self.get_endpoint_lambda_name(resource_meta,
                                                        endpoint_methods[0])

            request_path = '/' + stage + self.get_proxied_resource(url)
            config[request_path] = {
                'allowed_methods': endpoint_methods,
                'lambda_name': lambda_name
            }
        return config

    @staticmethod
    def get_endpoint_methods(ig_resource_meta: dict) -> List:
        methods = [key for key in ig_resource_meta.keys() if key.isupper()]
        if 'ANY' in methods:
            return list(HTTPMethod())
        return methods

    @staticmethod
    def get_endpoint_lambda_name(ig_resource_meta: dict,
                                 http_method: str) -> str:
        if http_method not in ig_resource_meta.keys():
            http_method = 'ANY'
        return ig_resource_meta.get(http_method, {}).get('lambda_name')

    def get_s3(self):
        s3_dr = {}
        for name, meta in self.content.items():
            if meta.get('resource_type') == S3_BUCKET_RESOURCE_TYPE:
                s3_dr[name] = meta
        return s3_dr

    @staticmethod
    def get_proxied_resource(resource: str) -> str:
        """
        Returns a proxied resource path, compatible with Bottle.
        :param resource: str, given '/path/{child_1}/{child_2+}'
        - returns `/path/<child_1>/<child_2>`
        :return: str
        """
        pattern = '([^{\/]+)(?=})'
        for match in finditer(pattern=pattern, string=resource):
            suffix = resource[match.end() + 1:]
            resource = resource[:match.start() - 1]
            path_input = match.group()
            path_input = path_input.strip('{+')
            resource += f'<{path_input}>' + suffix
        return resource
