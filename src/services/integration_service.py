from typing import Generator

from modular_sdk.commons.constants import ParentType
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.parent_service import ParentService

from services.ambiguous_job_service import AmbiguousJob
from services.defect_dojo_service import DefectDojoConfiguration, \
    DefectDojoParentMeta, DefectDojoService


class IntegrationService:
    def __init__(self, parent_service: ParentService,
                 defect_dojo_service: DefectDojoService):
        self._ps = parent_service
        self._dds = defect_dojo_service

    def get_dojo_adapters(self, tenant: Tenant,
                          send_after_job: bool | None = None
                          ) -> Generator[tuple[DefectDojoConfiguration, DefectDojoParentMeta], None, None]:
        """
        Yields dojo configurations for the given tenant. Currently, can yield
        only one, but maybe in future need multiple
        :param tenant:
        :param send_after_job:
        :return:
        """
        parent = self._ps.get_linked_parent_by_tenant(
            tenant=tenant, type_=ParentType.SIEM_DEFECT_DOJO
        )
        if not parent:
            return
        configuration = DefectDojoParentMeta.from_parent(parent)
        if (isinstance(send_after_job, bool) and
                configuration.send_after_job != send_after_job):
            return

        dojo = self._dds.get_nullable(parent.application_id)
        if not dojo:
            return
        yield dojo, configuration

    @staticmethod
    def job_tags_dojo(job: AmbiguousJob) -> list[str]:
        return list(filter(None, [
            job.owner,
            job.type.value,
            job.scheduled_rule_name,
            *(job.rulesets or []),
        ]))
