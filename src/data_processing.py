from abc import ABC, abstractmethod
from typing import Iterable, Iterator, MutableMapping
from copy import deepcopy
from itertools import chain

from data_output import Table
from linkedin.models import TimeIntervals, TimeRange


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
    def get_result_tables(self) -> Iterable[Table]:
        pass


class ShareStatisticsProcessor(OrganizationStatisticsProcessor):
    def get_result_tables(self) -> list[Table]:
        if self.time_intervals is None:
            table = self.get_share_statistics(table_name="total_share_statistics", primary_key=["organizationalEntity"])
        else:
            table = self.get_share_statistics(table_name="time_bound_share_statistics",
                                              primary_key=["organizationalEntity", "timeRange_start", "timeRange_end"])
        return [table] if table else []

    def get_share_statistics(self, table_name: str, primary_key: list[str]) -> Table | None:
        records = (self.process_element(element) for element in self.page_statistics_iterator)
        record = next(records, None)
        if record is None:
            return
        columns = list(record.keys())
        records = chain((record,), records)
        return Table(name=table_name, columns=columns, primary_key=primary_key, records=records)

    def process_element(self, element: dict):
        processed_element = deepcopy(element)
        total_share_statistics: dict = processed_element.pop("totalShareStatistics")
        processed_element.update(total_share_statistics)
        if processed_element.get("timeRange"):
            processed_element["timeRange"] = TimeRange.from_api_dict(
                processed_element["timeRange"]).to_serializable_dict()
        processed_element = flatten_dict(processed_element)
        return processed_element
