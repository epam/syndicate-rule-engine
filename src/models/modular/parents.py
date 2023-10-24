import dataclasses
from typing import List, Dict

from modular_sdk.commons import DataclassBase
from modular_sdk.models.parent import Parent

Parent = Parent


@dataclasses.dataclass()
class ParentMeta(DataclassBase):
    """
    Parent with type CUSTODIAN_LICENSES meta
    """
    rules_to_exclude: List[str]


@dataclasses.dataclass()
class DefectDojoParentMeta(DataclassBase):
    entities_mapping: Dict[str, str] = dataclasses.field(default_factory=dict)
    display_all_fields: bool = False
    upload_files: bool = False
    resource_per_finding: bool = False
