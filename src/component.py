from enum import Enum, unique
from itertools import chain
import logging
from typing import Iterable, Iterator

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from linkedin import (LinkedInClient, LinkedInClientException, URN, TimeIntervals, TimeGranularityType, TimeRange)

from data_processing import ShareStatisticsProcessor
from data_output import Table

# Global config keys:
KEY_DEBUG = "debug"
KEY_ORGANIZATION_IDS = "organization_ids"

# Row config keys:
KEY_INCREMENTAL = "incremental"
KEY_EXTRACTION_TARGET = "extraction_target"
KEY_TIME_RANGE = "time_range"

REQUIRED_PARAMETERS = [KEY_EXTRACTION_TARGET, KEY_INCREMENTAL]
REQUIRED_IMAGE_PARS = []


# Row config enums:
@unique
class ExtractionTarget(Enum):
    PAGE_STATS = "Page statistics"
    FOLLOWER_STATS = "Follower statistics"
    SHARE_STATS = "Share statistics"
    POSTS = "Posts"
    ENUMS = "Enumerated types"


STATS_EXTRACTION_TARGETS = (ExtractionTarget.PAGE_STATS, ExtractionTarget.FOLLOWER_STATS, ExtractionTarget.SHARE_STATS)

# Other hardcoded constants:
STATISTICS_REPORT_GRANULARITY = TimeGranularityType.DAY


class LinkedInPagesExtractor(ComponentBase):
    def run(self) -> None:
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params: dict = self.configuration.parameters
        self.extraction_target = ExtractionTarget(params[KEY_EXTRACTION_TARGET])
        organization_ids: list[int] | None = params.get(KEY_ORGANIZATION_IDS)
        time_range: dict | None = params.get(KEY_TIME_RANGE)
        incremental = bool(params.get(KEY_INCREMENTAL))
        debug = bool(params.get(KEY_DEBUG))

        access_token = self.get_access_token()
        self.client = LinkedInClient(access_token)

        organization_urns = self.get_organization_urns(organization_ids)

        if time_range:
            try:
                self.time_intervals = TimeIntervals(time_granularity_type=STATISTICS_REPORT_GRANULARITY,
                                                    time_range=TimeRange.from_config_dict(time_range))
                logging.info(f"Specified time range parsed to {self.time_intervals.time_range}")
            except ValueError as ve:
                raise UserException(f"Invalid time range provided. {ve.args[0]}") from ve
        else:
            self.time_intervals = None

        if self.extraction_target in STATS_EXTRACTION_TARGETS:
            if self.extraction_target is ExtractionTarget.PAGE_STATS:
                self.linked_in_client_method = self.client.get_organization_page_statistics
            elif self.extraction_target is ExtractionTarget.FOLLOWER_STATS:
                self.linked_in_client_method = self.client.get_organization_follower_statistics
            elif self.extraction_target is ExtractionTarget.SHARE_STATS:
                self.linked_in_client_method = self.client.get_organization_share_statistics
                self.statistics_processor_class = ShareStatisticsProcessor
            else:
                raise ValueError(f"Invalid extraction target: {self.extraction_target}")
            output_tables = self.get_all_processed_statistics_data(organization_urns=organization_urns)
        else:
            raise NotImplementedError("Only organization statistics extraction targets are implemented.")

        for table in output_tables:
            table.save_as_csv_with_manifest(component=self, incremental=incremental, include_csv_header=debug)

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

    def get_all_processed_statistics_data(self, organization_urns: list[URN]) -> Iterable[Table]:
        assert hasattr(self, "statistics_processor_class") and hasattr(self, "time_intervals")
        all_stats_data = chain.from_iterable(
            self.get_statistics_data_for_organization(organization_urn=organization_urn)
            for organization_urn in organization_urns)
        return self.statistics_processor_class(page_statistics_iterator=all_stats_data,
                                               time_intervals=self.time_intervals).get_result_tables()

    def get_statistics_data_for_organization(self, organization_urn: URN) -> Iterator[dict]:
        assert hasattr(self, "linked_in_client_method") and hasattr(self, "time_intervals")
        try:
            return self.linked_in_client_method(organization_urn, time_intervals=self.time_intervals)
        except LinkedInClientException as client_exc:
            raise UserException(client_exc) from client_exc

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
