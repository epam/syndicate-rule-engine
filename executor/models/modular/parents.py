import dataclasses
from typing import List, Dict

from modular_sdk.commons import DataclassBase
from modular_sdk.models.parent import Parent

Parent = Parent


@dataclasses.dataclass(frozen=True)
class ParentMeta(DataclassBase):
    """
    Common meta
    """
    rules_to_exclude: List[str]


@dataclasses.dataclass(frozen=True)
class DefectDojoParentMeta(DataclassBase):
    entities_mapping: Dict[str, str] = dataclasses.field(default_factory=dict)
    display_all_fields: bool = False
    upload_files: bool = False
    resource_per_finding: bool = False
