from dataclasses import dataclass, field
from typing import Optional
from datetime import date


@dataclass
class EOLPart:
    part_number: str
    supplier: str
    description: str = ""
    eol_date: Optional[date] = None
    last_buy_date: Optional[date] = None
    replacement_pn: str = ""
    product_family: str = ""
    package: str = ""
    source_url: str = ""
    source_doc: str = ""


@dataclass
class TIAlternative:
    ti_part_number: str
    ti_description: str = ""
    match_type: str = ""
    params: dict = field(default_factory=dict)
    ti_product_url: str = ""
    lifecycle_status: str = ""


@dataclass
class CrossRefResult:
    eol_part: EOLPart
    ti_alternatives: list = field(default_factory=list)
