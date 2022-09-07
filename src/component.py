from datetime import datetime
import logging
from itertools import chain    # noqa
from copy import deepcopy

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from linkedin import (    # noqa
    LinkedInClient, LinkedInClientException, URN, TimeIntervals, TimeGranularityType, TimeRange)

KEY_ORGANIZATION_IDS = "organization_ids"
# KEY_ORGANIZATION_VANITY_NAME = "organization_vanity_name"
KEY_TIME_RANGE = "time_range"

REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []


def process_stat_element(page_stat: dict):
    page_stat = deepcopy(page_stat)
    page_stat["timeRange"] = TimeRange.from_api_dict(page_stat["timeRange"]).to_serializable_dict()
    return page_stat


class LinkedInPagesExtractor(ComponentBase):
    # def __init__(self):
    #     super().__init__()

    def run(self) -> None:
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params: dict = self.configuration.parameters
        organization_ids: list[int] | None = params.get(KEY_ORGANIZATION_IDS)
        time_range: dict | None = params.get(KEY_TIME_RANGE)
        # if not organization_ids:
        #     organization_vanity_name = params.get(KEY_ORGANIZATION_VANITY_NAME)
        #     if not organization_vanity_name:
        #         raise UserException("Either organization ID or organization vanity name has to be specified.")

        access_token = self.get_access_token()
        self.client = LinkedInClient(access_token)

        if organization_ids:
            organization_urns = [URN(entity_type="organization", id=id) for id in organization_ids]
        else:
            try:
                organization_acls = list(self.client.get_organization_acls("roleAssignee"))
            except LinkedInClientException as client_exc:
                raise UserException(client_exc) from client_exc
            organization_urns = [URN.from_str(org_acl["organization"]) for org_acl in organization_acls]

        if time_range:
            time_intervals = TimeIntervals(time_granularity_type=TimeGranularityType.DAY,
                                           time_range=TimeRange.from_config_dict(time_range))
        else:
            time_intervals = None
        org_page_stats = {    #  Maybe use generator of tuples instead # noqa
            organization_urn: self.fetch_organization_page_stats(organization_urn=organization_urn,
                                                                 time_intervals=time_intervals)
            for organization_urn in organization_urns
        }
        pass

    def fetch_organization_page_stats(self,
                                      organization_urn: URN,
                                      time_intervals: TimeIntervals | None = None) -> list[dict]:
        try:
            return [
                process_stat_element(page_stat)
                for page_stat in self.client.get_organization_page_statistics(organization_urn,
                                                                              time_intervals=time_intervals)
            ]
        except LinkedInClientException as client_exc:
            raise UserException(client_exc) from client_exc
        # try:
        #     page_stats = [    # noqa
        #         process_stat_element(page_stat)
        #         for page_stat in client.get_organization_page_statistics(organization_ids,
        #                                                                  time_intervals=time_intervals)
        #     ]
        #     follower_stats = [    # noqa
        #         process_stat_element(page_stat)
        #         for page_stat in client.get_organization_follower_statistics(organization_ids,
        #                                                                      time_intervals=time_intervals)
        #     ]
        #     share_stats = [    # noqa
        #         process_stat_element(page_stat)
        #         for page_stat in client.get_organization_share_statistics(organization_ids,
        #                                                                   time_intervals=time_intervals)
        #     ]
        # except LinkedInClientException as client_exc:
        #     raise UserException(client_exc) from client_exc

        # posts = chain(client.get_posts_by_author(author_urn=organization_urn(organization_id), is_dsc=False),
        #               client.get_posts_by_author(author_urn=organization_urn(organization_id), is_dsc=True))
        # # posts_list = list(posts)
        # for post in posts:
        #     post_urn = post["id"]
        #     post_social_actions_summary = client.get_social_action_summary_on_post(post_urn)
        #     total_comments = post_social_actions_summary["commentsSummary"]["aggregatedTotalComments"]
        #     total_likes = post_social_actions_summary["likesSummary"]["totalLikes"]
        #     if total_comments == 0 or total_likes == 0:
        #         continue
        #     post_comments = list(client.get_comments_on_post(post_urn))
        #     post_likes = list(client.get_likes_on_post(post_urn))
        #     if len(post_comments) > 0 and len(post_likes) > 0:
        #         break

        # if not organization_ids:
        #     organization_infos = list(client.get_organization_by_vanity_name(vanity_name=organization_vanity_name))
        #     if len(organization_infos) != 1:
        #         raise UserException("Organization with the specified vanity name was not found.")
        #     organization_info = organization_infos[0]
        #     organization_ids = organization_info["id"]

        # org_info = client.get_administered_organization(organization_id)    # noqa
        # try:
        #     org_acls = client.get_organization_acls("roleAssignee")    # noqa
        #     # print(data2)
        # except LinkedInClientException as client_exc:
        #     raise UserException(client_exc) from client_exc

        # page_stats = list(client.get_organization_page_statistics(organization_id))    # noqa

        # follower_stats = list(client.get_organization_follower_statistics(organization_id))    # noqa

        # share_stats = list(client.get_organization_share_statistics(organization_id))    # noqa

        # post_raw_page = client.get_posts_by_author(    # noqa
        #     author_urn=organization_urn(organization_id), is_dsc=True, start=10, count=4)

        # post_with_likes_urn = 'urn:li:share:6367102219933806592'
        # post_with_comments_and_likes_urn = 'urn:li:share:6861468431473025024'
        # post = client.get_post_by_urn(post_with_comments_and_likes_urn)
        # post_comments = list(client.get_comments_on_post(post_with_comments_and_likes_urn))
        # post_likes = list(client.get_likes_on_post(post_with_comments_and_likes_urn))
        # post_reactions = list(client.get_reactions_on_post(post_with_comments_and_likes_urn))
        # pass

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
