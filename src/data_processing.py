from abc import ABC, abstractmethod
import logging
from typing import Iterable, Mapping, MutableMapping
from copy import deepcopy
from itertools import chain
import re

from inflection import underscore, camelize
from keboola.utils.header_normalizer import DefaultHeaderNormalizer

from csv_table import Table
from linkedin.models import URN, StandardizedDataType, TimeIntervals, TimeRange

HEADER_NORMALIZER = DefaultHeaderNormalizer()

X_BY_Y_RE = re.compile(r"(\w+)By(\w+)")


def flatten_dict(d: Mapping, parent_key: str = '', sep: str = '_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def rename_dict_keys(d: MutableMapping, key_name_mapping: Mapping):
    return {key_name_mapping.get(key, key): value for key, value in d.items()}


def create_table(records: Iterable[dict],
                 table_name: str,
                 primary_key: list[str],
                 flatten_records: bool = True,
                 normalize_header: bool = True):
    records = iter(records)
    if flatten_records:
        records_processed = (flatten_dict(d) for d in records)
    else:
        records_processed = records
    record_processed: dict = next(records_processed, None)
    if record_processed is None:
        logging.warning(f"API returned no records for output table '{table_name}'.")
        return Table(name=table_name, columns=None, primary_key=primary_key, records=records_processed)
    records_processed = chain((record_processed,), records_processed)
    columns = list(record_processed.keys())
    if normalize_header:
        denormalized_columns = columns
        columns = HEADER_NORMALIZER.normalize_header(columns)
        column_name_mapping = {
            denorm_name: norm_name
            for denorm_name, norm_name in zip(denormalized_columns, columns)
            if denorm_name != norm_name
        }
        records_processed = (rename_dict_keys(d, column_name_mapping) for d in records_processed)
    for pk in primary_key:
        if pk not in columns:
            raise ValueError(f"Invalid primary key. Primary key element '{pk}' not found in columns: {columns}.")
    return Table(name=table_name, columns=columns, primary_key=primary_key, records=records_processed)


class OrganizationStatisticsProcessor(ABC):
    def __init__(self, records: Iterable[dict], time_intervals: TimeIntervals | None):
        self.records_iterator = iter(records)
        self.time_intervals = time_intervals
        self.set_processing_parameters()
        super().__init__()

    @abstractmethod
    def set_processing_parameters(self,
                                  statistics_type_name: str,
                                  organization_urn_fieldname: str,
                                  total_statistics_fieldname: str | None = None):
        self.time_bound_statistics_table_name: str = f"time_bound_{statistics_type_name}_statistics"
        self.organization_urn_fieldname: str = organization_urn_fieldname
        self.time_bound_statistics_table_name_primary_key: list[str] = [
            self.organization_urn_fieldname, "timeRange_start", "timeRange_end"
        ]
        self.total_statistics_fieldname: str = (total_statistics_fieldname if total_statistics_fieldname else camelize(
            f"total_{statistics_type_name}_statistics", uppercase_first_letter=False))

    def get_result_tables(self) -> list[Table]:
        if self.time_intervals is None:
            return self.get_total_statistics_tables()
        else:
            return [self.get_time_bound_statistics_table()]

    def get_time_bound_statistics_table(self) -> Table:
        records_processed = (self.process_time_bound_element(element) for element in self.records_iterator)
        return create_table(records=records_processed,
                            table_name=self.time_bound_statistics_table_name,
                            primary_key=self.time_bound_statistics_table_name_primary_key)

    def process_time_bound_element(self, element: dict):
        processed_element = deepcopy(element)
        processed_element.update(processed_element.pop(self.total_statistics_fieldname))
        if processed_element.get("timeRange"):
            processed_element["timeRange"] = TimeRange.from_api_dict(
                processed_element["timeRange"]).to_serializable_dict()
        return processed_element

    def get_total_statistics_tables(self) -> list[Table]:
        records_processed = [self.process_organization_record(record) for record in self.records_iterator]
        record_processed = records_processed[0]
        table_name_to_table_records_dict = {
            table_name: list(chain.from_iterable(record[table_name] for record in records_processed))
            for table_name in record_processed.keys()
        }
        return [
            create_table(records=records,
                         table_name=underscore(table_name),
                         primary_key=[
                             self.organization_urn_fieldname,
                             camelize(X_BY_Y_RE.match(table_name).group(2), uppercase_first_letter=False)
                         ] if X_BY_Y_RE.match(table_name) else [self.organization_urn_fieldname],
                         flatten_records=True) for table_name, records in table_name_to_table_records_dict.items()
        ]

    def process_organization_record(self, record: dict) -> dict:
        record_processed = deepcopy(record)
        organization_urn: str = record_processed.pop(self.organization_urn_fieldname)
        for table_name, table_records in record_processed.items():
            if isinstance(table_records, list):
                data_key = X_BY_Y_RE.match(table_name).group(1)
                for table_record in table_records:
                    table_record: dict
                    table_record[self.organization_urn_fieldname] = organization_urn
                    table_record.update(table_record.pop(data_key))
            elif isinstance(table_records, dict):
                table_records[self.organization_urn_fieldname] = organization_urn
                record_processed[table_name] = [table_records]
            else:
                raise ValueError("Organization records cannot contain values that are neither lists or dicts.")
        return record_processed


class ShareStatisticsProcessor(OrganizationStatisticsProcessor):
    def set_processing_parameters(self):
        super().set_processing_parameters(statistics_type_name="share",
                                          organization_urn_fieldname="organizationalEntity")


class FollowerStatisticsProcessor(OrganizationStatisticsProcessor):
    def set_processing_parameters(self):
        super().set_processing_parameters(statistics_type_name="follower",
                                          organization_urn_fieldname="organizationalEntity",
                                          total_statistics_fieldname="followerGains")


class PageStatisticsProcessor(OrganizationStatisticsProcessor):
    def set_processing_parameters(self):
        super().set_processing_parameters(statistics_type_name="page", organization_urn_fieldname="organization")


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
