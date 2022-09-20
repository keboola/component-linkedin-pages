from abc import ABC, abstractmethod
import logging
from typing import Iterable, Iterator, MutableMapping
from copy import deepcopy
from itertools import chain

from csv_table import Table
from linkedin.models import URN, StandardizedDataType, TimeIntervals, TimeRange


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '_') -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def create_table(records: Iterable[dict], table_name: str, primary_key: list[str], flatten_records: bool = True):
    records = iter(records)
    if flatten_records:
        records_processed = (flatten_dict(d) for d in records)
    else:
        records_processed = records
    record_processed: dict = next(records_processed, None)
    if record_processed is None:
        logging.warning(f"API returned no records for output table '{table_name}'.")
        return Table(name=table_name, columns=None, primary_key=primary_key, records=records_processed)
    columns = list(record_processed.keys())
    for pk in primary_key:
        if pk not in columns:
            raise ValueError(f"Invalid primary key. Primary key element '{pk}' not found in columns: {columns}.")
    records_processed = chain((record_processed,), records_processed)
    return Table(name=table_name, columns=columns, primary_key=primary_key, records=records_processed)


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
        records_processed = (self.process_element(element) for element in self.page_statistics_iterator)
        return create_table(records=records_processed, table_name=table_name, primary_key=primary_key)

    def process_element(self, element: dict):
        processed_element = deepcopy(element)
        total_share_statistics: dict = processed_element.pop("totalShareStatistics")
        processed_element.update(total_share_statistics)
        if processed_element.get("timeRange"):
            processed_element["timeRange"] = TimeRange.from_api_dict(
                processed_element["timeRange"]).to_serializable_dict()
        return processed_element


def create_standardized_data_enum_table(standardized_data_type: StandardizedDataType,
                                        records: Iterable[dict]) -> Table | None:
    def process_enum_element(el: dict) -> dict:
        if standardized_data_type is StandardizedDataType.SKILLS:
            processed_element = {"standardizedName": el["standardizedName"], "id": el["id"]}
        elif standardized_data_type is StandardizedDataType.IAB_CATEGORIES:
            processed_element = {"displayName": el["displayName"], "iabName": el["iabName"], "id": el["id"]}
        elif standardized_data_type is StandardizedDataType.COUNTRIES:
            processed_element = {
                "name": el["name"]["value"],
                "id": el["countryCode"],
                "urn": el["$URN"],
                "countryCode": el["countryCode"]
            }
        elif standardized_data_type is StandardizedDataType.STATES:
            processed_element = {
                "name": el["name"]["value"],
                "id": el["stateCode"],
                "urn": el["$URN"],
                "stateCode": el["stateCode"],
                "country": el["country"]
            }
        elif standardized_data_type is StandardizedDataType.REGIONS:
            processed_element = {
                "name": el["name"]["value"],
                "id": el["id"],
                "urn": el["$URN"],
                "country": el["country"]
            }
        else:
            processed_element = {"name": el["name"]["localized"]["en_US"], "id": el["id"], "urn": el["$URN"]}
            for field_name in ("rollup", "rollupIds", "parentId"):
                if el.get(field_name):
                    processed_element[field_name] = el[field_name]
        return processed_element

    records_processed = (process_enum_element(d) for d in records)
    return create_table(records=records_processed,
                        table_name=standardized_data_type.normalized_name,
                        primary_key=["id"],
                        flatten_records=False)


def create_posts_subobject_table(urn_to_records_dict: dict[URN, Iterable[dict]], table_name: str,
                                 primary_key: list[str]):
    def process_record(record: dict, post_urn: URN):
        processed_rec = record.copy()
        processed_rec["post_urn"] = str(post_urn)
        return processed_rec

    records_processed = chain.from_iterable(
        (process_record(d, post_urn=urn) for d in records) for urn, records in urn_to_records_dict.items())
    return create_table(records=records_processed, table_name=table_name, primary_key=primary_key)
