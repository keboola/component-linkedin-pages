from abc import ABC, abstractmethod
from typing import Iterator, MutableMapping
from copy import deepcopy    # noqa

from data_output import Table
from linkedin.models import URN, TimeIntervals, TimeRange    # noqa


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '_') -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class OrganizationStatisticsProcessor(ABC):
    def __init__(self, page_statistics_iterator: Iterator[dict], time_intervals: TimeIntervals | None):
        self.page_statistics_iterator = page_statistics_iterator
        self.time_intervals = time_intervals
        super().__init__()

    @abstractmethod
    def get_result_tables(self) -> list[Table]:
        pass


class ShareStatisticsProcessor(OrganizationStatisticsProcessor):
    def get_result_tables(self) -> list[Table]:
        pass
        if self.time_intervals is None:
            raise NotImplementedError()
        else:
            return [self.get_time_bound_share_statistics()]

    def get_time_bound_share_statistics(self) -> Table:
        records = [self.process_element(element) for element in self.page_statistics_iterator]
        record = records[0]
        columns = list(record.keys())
        return Table(name="time_bound_share_statistics", columns=columns, primary_key=[], records=records)

    def process_element(self, element: dict):
        processed_element = deepcopy(element)
        if processed_element.get("timeRange"):
            processed_element["timeRange"] = TimeRange.from_api_dict(
                processed_element["timeRange"]).to_serializable_dict()
        processed_element = flatten_dict(processed_element)
        return processed_element
