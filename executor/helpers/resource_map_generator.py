import importlib
import json
import logging
import os
import sys
from pathlib import Path

MAPPING_FOLDER = 'resources_mapping'
MAPPING_PATH = Path(__file__).parent.parent / MAPPING_FOLDER
os.makedirs(MAPPING_PATH, exist_ok=True)

AWS = 'AWS'
AZURE = 'AZURE'
GOOGLE = 'GOOGLE'

AVAILABLE_CLOUDS = {AWS, AZURE, GOOGLE}

RESOURCE_MAP = 'ResourceMap'
RESOURCE_TYPE_ATTRIBUTE = 'resource_type'
RESOURCE_META_REQUIRED_ATTRIBUTES = ('id', 'name', 'date')

CLOUD_RESOURCE_MAP_PATH_MAPPING = {
    AWS: 'c7n.resources.resource_map',
    AZURE: 'c7n_azure.resources.resource_map',
    GOOGLE: 'c7n_gcp.resources.resource_map'
}

_LOG = logging.getLogger('custodian_resource_map_generator')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
_LOG.addHandler(handler)
_LOG.setLevel(logging.DEBUG)


def import_resource_map(cloud: str):
    resource_map_path = CLOUD_RESOURCE_MAP_PATH_MAPPING.get(cloud)
    _LOG.debug(f'Going to import module \'{resource_map_path}\'')
    if resource_map_path:
        module = importlib.import_module(resource_map_path)
        if hasattr(module, RESOURCE_MAP):
            _LOG.debug(f'Module {resource_map_path} '
                       f'has been imported successfully')
            return getattr(module, RESOURCE_MAP)
    _LOG.debug(f'Failed to import module: \'{resource_map_path}\'')


def build_resources_mapping(resource_map: dict):
    result_map = {}

    for resource_type, resource_path in resource_map.items():
        type_meta = get_resource_type_meta(resource_path=resource_path)
        if type_meta:
            result_map[resource_type] = type_meta
    return result_map


def get_resource_type_meta(resource_path: str):
    *module_path, cls_to_import = resource_path.split('.')
    module = importlib.import_module('.'.join(module_path))
    if not hasattr(module, cls_to_import):
        _LOG.error(f'Failed to import \'{cls_to_import}\' class '
                   f'from \'{module.__name__}\' module.')
        return
    cls = getattr(module, cls_to_import)
    if not hasattr(cls, RESOURCE_TYPE_ATTRIBUTE):
        _LOG.error(f'Missing \'{RESOURCE_TYPE_ATTRIBUTE}\' subclass'
                   f'from {resource_path}')
        return
    meta = getattr(cls, RESOURCE_TYPE_ATTRIBUTE)
    if meta:
        type_meta = {}
        for attr in RESOURCE_META_REQUIRED_ATTRIBUTES:
            attr_value = getattr(meta, attr) if hasattr(meta, attr) else None
            if attr_value:
                type_meta[attr] = attr_value
        return type_meta


def save_resources_map(resources_map: dict, cloud: str):
    with open(MAPPING_PATH / f'resources_map_{cloud.lower()}.json', 'w') as f:
        json.dump(resources_map, f)


def main(target_cloud: str):
    _LOG.debug(f'Importing resource map for cloud: {target_cloud}')
    cloud_resource_mapping = import_resource_map(cloud=target_cloud)
    _LOG.debug(f'ResourceMap: {cloud_resource_mapping}')
    _LOG.debug('Building resources_mapping')
    resources_mapping = build_resources_mapping(
        resource_map=cloud_resource_mapping)
    _LOG.debug(f'Resources Mapping: {resources_mapping}')
    _LOG.debug('Reformatting detailed report.')

    save_resources_map(resources_mapping, target_cloud)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        target_cloud = sys.argv[1]
        target_cloud = target_cloud.upper()
        if target_cloud not in AVAILABLE_CLOUDS:
            _LOG.error(f'Specified cloud must be one of the following: '
                       f'{AVAILABLE_CLOUDS}, got \'{target_cloud}\'')
            exit(1)
        main(target_cloud=target_cloud)
    else:
        _LOG.warning(f'{sys.argv[0]} did not receive any cloud argument. '
                     f'Generating for all available clouds')
        for cloud in AVAILABLE_CLOUDS:
            main(target_cloud=cloud)
