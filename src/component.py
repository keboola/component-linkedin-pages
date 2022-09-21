from datetime import datetime, timezone
from enum import Enum, unique
from itertools import chain
import logging
from typing import Iterable, Iterator

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from linkedin import (LinkedInClient, LinkedInClientException, URN, TimeIntervals, TimeGranularityType, TimeRange,
                      StandardizedDataType)

from data_processing import (FollowerStatisticsProcessor, PageStatisticsProcessor, ShareStatisticsProcessor,
                             create_standardized_data_enum_table, create_posts_subobject_table, create_table)
from csv_table import Table

# Global config keys:
KEY_DEBUG = "debug"
KEY_ORGANIZATION_IDS = "organizations"

# Row config keys:
KEY_EXTRACTION_TARGET = "endpoints"
KEY_SYNC_OPTIONS = "sync_options"
KEY_DESTINATION = "destination"
KEY_LOAD_TYPE = "load_type"

REQUIRED_PARAMETERS = [KEY_EXTRACTION_TARGET, KEY_DESTINATION]
REQUIRED_IMAGE_PARS = []

# State keys:
KEY_LAST_RUN_DATETIME = "last_run_downloaded_data_up_to_datetime"


# Row config enums:
@unique
class LoadType(Enum):
    FULL = "full_load"
    INCREMENTAL = "incremental_load"


@unique
class ExtractionTarget(Enum):
    PAGE_STATS_TIME_BOUND = "page_statistics_time_bound"
    PAGE_STATS_LIFETIME = "page_statistics_lifetime"
    FOLLOWER_STATS_TIME_BOUND = "follower_statistics_time_bound"
    FOLLOWER_STATS_LIFETIME = "follower_statistics_lifetime"
    SHARE_STATS_TIME_BOUND = "share_statistics_time_bound"
    SHARE_STATS_LIFETIME = "share_statistics_lifetime"
    POSTS = "posts"
    ENUMS = "enumerated_types"
    ORGANIZATIONS = "organizations"


PAGE_STATS_EXTRACTION_TARGETS = (ExtractionTarget.PAGE_STATS_TIME_BOUND, ExtractionTarget.PAGE_STATS_LIFETIME)
FOLLOWER_STATS_EXTRACTION_TARGETS = (ExtractionTarget.FOLLOWER_STATS_TIME_BOUND,
                                     ExtractionTarget.FOLLOWER_STATS_LIFETIME)
SHARE_STATS_EXTRACTION_TARGETS = (ExtractionTarget.SHARE_STATS_TIME_BOUND, ExtractionTarget.SHARE_STATS_LIFETIME)

STATS_EXTRACTION_TARGETS = (PAGE_STATS_EXTRACTION_TARGETS + FOLLOWER_STATS_EXTRACTION_TARGETS +
                            SHARE_STATS_EXTRACTION_TARGETS)

TIME_BOUND_STATS_EXTRACTION_TARGETS = (ExtractionTarget.PAGE_STATS_TIME_BOUND,
                                       ExtractionTarget.FOLLOWER_STATS_TIME_BOUND,
                                       ExtractionTarget.SHARE_STATS_TIME_BOUND)
LIFETIME_STATS_EXTRACTION_TARGETS = (ExtractionTarget.PAGE_STATS_LIFETIME, ExtractionTarget.FOLLOWER_STATS_LIFETIME,
                                     ExtractionTarget.SHARE_STATS_LIFETIME)

# Other hardcoded constants:
STATISTICS_REPORT_GRANULARITY = TimeGranularityType.DAY


class LinkedInPagesExtractor(ComponentBase):
    def run(self) -> None:
        self.tmp_state = self.get_state_file()
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params: dict = self.configuration.parameters
        self.extraction_target = ExtractionTarget(params[KEY_EXTRACTION_TARGET])
        organization_ids_str: str | None = params.get(KEY_ORGANIZATION_IDS)
        if organization_ids_str:
            try:
                organization_ids: list[int] = [int(id_str) for id_str in organization_ids_str.split(",")]
            except ValueError as ve:
                raise UserException(ve)
        else:
            organization_ids = None
        time_range: dict | None = params.get(KEY_SYNC_OPTIONS)
        self.incremental = LoadType(params[KEY_DESTINATION][KEY_LOAD_TYPE]) is LoadType.INCREMENTAL
        self.debug = bool(params.get(KEY_DEBUG))

        access_token = self.get_access_token()
        self.client = LinkedInClient(access_token)

        organization_urns = self.get_organization_urns(organization_ids)

        if self.extraction_target in STATS_EXTRACTION_TARGETS:
            self.set_time_intervals(time_range=time_range)
            if self.time_intervals and self.time_intervals.time_range.length_in_days == 0:
                logging.warning("Empty resultant time range for time bound statistics (start is the same as end),"
                                " exiting without data output.")
                return

            if self.extraction_target in PAGE_STATS_EXTRACTION_TARGETS:
                self.linked_in_client_method = self.client.get_organization_page_statistics
                self.statistics_processor_class = PageStatisticsProcessor
            elif self.extraction_target in FOLLOWER_STATS_EXTRACTION_TARGETS:
                self.linked_in_client_method = self.client.get_organization_follower_statistics
                self.statistics_processor_class = FollowerStatisticsProcessor
            elif self.extraction_target in SHARE_STATS_EXTRACTION_TARGETS:
                self.linked_in_client_method = self.client.get_organization_share_statistics
                self.statistics_processor_class = ShareStatisticsProcessor
            else:
                raise ValueError(f"Invalid extraction target: {self.extraction_target}")
            output_tables = self.get_all_statistics_tables(organization_urns=organization_urns)

        elif self.extraction_target is ExtractionTarget.POSTS:
            output_tables = self.get_all_posts_based_tables(organization_urns=organization_urns)
        elif self.extraction_target is ExtractionTarget.ENUMS:
            output_tables = self.get_all_standardized_data_enum_tables()
        elif self.extraction_target is ExtractionTarget.ORGANIZATIONS:
            output_tables = self.get_organizations_table(organization_urns=organization_urns)
        else:
            raise ValueError(f"Invalid extraction target: {self.extraction_target}")

        for table in output_tables:
            table.save_as_csv_with_manifest(component=self, incremental=self.incremental, include_csv_header=self.debug)
        if self.tmp_state:
            self.write_state_file(self.tmp_state)

    def get_organization_urns(self, organization_ids: list[int]):
        if organization_ids:
            organization_urns = [URN(entity_type="organization", id=id) for id in organization_ids]
        else:
            try:
                organization_acls = list(self.client.get_organization_acls("roleAssignee"))
            except LinkedInClientException as client_exc:
                raise UserException(client_exc) from client_exc
            organization_urns = [URN.from_str(org_acl["organization"]) for org_acl in organization_acls]
        return organization_urns

    def set_time_intervals(self, time_range: dict):
        if self.extraction_target in TIME_BOUND_STATS_EXTRACTION_TARGETS and not time_range:
            raise UserException("When downloading time bound statistics, Sync Options must be properly specified.")
        if self.extraction_target in LIFETIME_STATS_EXTRACTION_TARGETS:
            self.time_intervals = None
            return
        try:
            self.time_intervals = TimeIntervals(time_granularity_type=STATISTICS_REPORT_GRANULARITY,
                                                time_range=TimeRange.from_config_dict(
                                                    time_range,
                                                    last_run_datetime_str=self.tmp_state.get(KEY_LAST_RUN_DATETIME)))
        except ValueError as ve:
            raise UserException(f"Invalid time range provided. {ve.args[0]}") from ve
        logging.info(f"Specified time range parsed to {self.time_intervals.time_range}")
        self.tmp_state[KEY_LAST_RUN_DATETIME] = min(
            datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, fold=0),
            self.time_intervals.time_range.end).isoformat(timespec="seconds")

    def get_all_statistics_tables(self, organization_urns: Iterable[URN]) -> list[Table]:
        assert hasattr(self, "statistics_processor_class") and hasattr(self, "time_intervals")
        records = chain.from_iterable(
            self.get_statistics_data_for_organization(organization_urn=organization_urn)
            for organization_urn in organization_urns)
        return self.statistics_processor_class(records=records, time_intervals=self.time_intervals).get_result_tables()

    def get_statistics_data_for_organization(self, organization_urn: URN) -> Iterator[dict]:
        assert hasattr(self, "linked_in_client_method") and hasattr(self, "time_intervals")
        try:
            return self.linked_in_client_method(organization_urn, time_intervals=self.time_intervals)
        except LinkedInClientException as client_exc:
            raise UserException(client_exc) from client_exc

    def get_all_standardized_data_enum_tables(self):
        return [
            create_standardized_data_enum_table(standardized_data_type,
                                                records=self.client.get_all_standardized_data_type_enum_values(
                                                    standardized_data_type=standardized_data_type))
            for standardized_data_type in StandardizedDataType
        ]

    def get_all_posts_based_tables(self, organization_urns: Iterable[URN]) -> list[Table]:
        posts_records = (chain.from_iterable(
            chain(self.client.get_posts_by_author(urn, is_dsc=True), self.client.get_posts_by_author(urn, is_dsc=False))
            for urn in organization_urns))
        posts_table = create_table(records=posts_records, table_name="posts", primary_key=["id"])
        if posts_table.is_empty:
            logging.warning("No posts found for any available/specified organization.")
        posts_table.save_as_csv_with_manifest(self, incremental=self.incremental, include_csv_header=self.debug)
        posts_urns = list(    # Keeping the posts URNs in memory here - may cause problems if number of posts is high
            URN.from_str(processed_record["id"]) for processed_record in posts_table.get_refreshed_records_iterator())

        comments_urn_to_records = {urn: self.client.get_comments_on_post(urn) for urn in posts_urns}
        comments_table = create_posts_subobject_table(urn_to_records_dict=comments_urn_to_records,
                                                      table_name="comments",
                                                      primary_key=["id"])

        likes_urn_to_records = {urn: self.client.get_likes_on_post(urn) for urn in posts_urns}
        likes_table = create_posts_subobject_table(urn_to_records_dict=likes_urn_to_records,
                                                   table_name="likes",
                                                   primary_key=["$URN"])

        return [posts_table, comments_table, likes_table]

    def get_organizations_table(self, organization_urns: Iterable[URN]) -> list[Table]:
        organization_records = (
            self.client.get_administered_organization(organization_urn) for organization_urn in organization_urns)
        return [create_table(records=organization_records, table_name="organizations", primary_key=["id"])]

    def get_access_token(self) -> str:
        if "access_token" not in self.configuration.oauth_credentials["data"]:
            raise UserException("Access token not available. Retry Authorization process")
        return self.configuration.oauth_credentials["data"]["access_token"]


if __name__ == "__main__":
    try:
        comp = LinkedInPagesExtractor()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
