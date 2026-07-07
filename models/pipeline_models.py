from dataclasses import dataclass, asdict
from typing import Any

import ahocorasick


@dataclass
class Dictionaries:
    ac_automaton:          ahocorasick.Automaton
    typo_list:             list[dict]
    typo_regex_list:       list[dict]
    garbage_config:        dict
    product_name_norm_list: list[dict]



@dataclass
class ErrorRecord:
    product_id:              str
    category:                str | None
    main_category:           str | None
    sub_category:            str | None
    product_brand:           str
    product_name_raw:        str
    product_name:            str
    product_ingredients_raw: str
    product_url:             str
    crawled_at:              Any   # pd.Timestamp | pd.NaT
    error_type:              str
    residual_text:           str
    goods_no:                str = ""   # 올리브영 상품번호(raw 통과)

    def to_dict(self) -> dict:
        return asdict(self)