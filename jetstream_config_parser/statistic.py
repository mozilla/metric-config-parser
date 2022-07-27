from typing import Any, Dict

import attr


@attr.s(auto_attribs=True)
class Statistic:
    slug: str
    params: Dict[str, Any]
