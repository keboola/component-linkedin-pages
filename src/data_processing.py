from typing import MutableMapping
from copy import deepcopy
from linkedin.models import URN, TimeRange


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '_') -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def process_stat_element(page_stat: dict, organization_urn: URN):
    assert (page_stat.get("organization") or page_stat.get("organizationalEntity")) == str(organization_urn)
    page_stat_processed = deepcopy(page_stat)
    if page_stat_processed.get("timeRange"):
        page_stat_processed["timeRange"] = TimeRange.from_api_dict(
            page_stat_processed["timeRange"]).to_serializable_dict()
    page_stat_processed = flatten_dict(page_stat_processed)
    return page_stat_processed
