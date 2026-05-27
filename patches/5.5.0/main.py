#!/usr/local/bin/python
import logging
import sys

from modular_sdk.commons.constants import ParentType
from modular_sdk.models.parent import Parent
from modular_sdk.modular import Modular

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.DEBUG
)
_LOG = logging.getLogger(__name__)

SIEM_DEFECT_DOJO = 'SIEM_DEFECT_DOJO'


def patch():
    """
    This patch reads all the parent with type "SIEM_DEFECT_DOJO" and replaces
    the type to "CUSTODIAN_SIEM_DEFECT_DOJO" for mongo
    """
    # todo better use modular-sdk services for patches, but parent service
    #  lacks functionality for this patch
    ps = Modular().parent_service()
    _LOG.info('Starting patch')
    it = Parent.scan(filter_condition=(Parent.type == SIEM_DEFECT_DOJO))
    for parent in it:
        _LOG.info(f'Found parent {SIEM_DEFECT_DOJO}: {parent.parent_id}')
        _LOG.info('Updating parent')
        parent.update(actions=[
            Parent.type.set(ParentType.CUSTODIAN_SIEM_DEFECT_DOJO.value),
            Parent.type_scope.set(ps.build_type_scope(
                type_=ParentType.CUSTODIAN_SIEM_DEFECT_DOJO.value,
                scope=parent.scope,
                tenant_name=parent.tenant_name,
                cloud=parent.cloud
            ))
        ])
    _LOG.info('Patch has finished')


def main() -> int:
    try:
        patch()
        return 0
    except Exception:  # noqa
        _LOG.exception('Unexpected exception occurred')
        return 1


if __name__ == '__main__':
    sys.exit(main())
