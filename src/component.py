import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from linkedin import LinkedInClient, LinkedInClientException, organization_urn    # noqa

KEY_ORGANIZATION_ID = "organization_id"

REQUIRED_PARAMETERS = [KEY_ORGANIZATION_ID]
REQUIRED_IMAGE_PARS = []


class LinkedInPagesExtractor(ComponentBase):
    # def __init__(self):
    #     super().__init__()

    def run(self) -> None:
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params: dict = self.configuration.parameters
        organisation_id: str | int = params[KEY_ORGANIZATION_ID]

        access_token = self.get_access_token()
        client = LinkedInClient(access_token)

        # org_info = client.get_administered_organization(organisation_id)    # noqa
        # try:
        #     org_acls = client.get_organization_acls("roleAssignee")    # noqa
        #     # print(data2)
        # except LinkedInClientException as client_exc:
        #     raise UserException(client_exc) from client_exc

        # page_stats = client.get_organization_page_statistics(organisation_id)    # noqa

        # follower_stats = client.get_organization_follower_statistics(organisation_id)    # noqa

        # post_raw_page = client.get_posts_by_author(    # noqa
        #     author_urn=organization_urn(organisation_id), is_dsc=True, start=10, count=4)
        posts = client.get_posts_by_author(author_urn=organization_urn(organisation_id), is_dsc=True)    # noqa
        # posts_list = list(posts)
        for post in posts:
            post_urn = post["id"]
            post_social_actions_summary = client.get_social_action_summary_on_post(post_urn)
            total_comments = post_social_actions_summary["commentsSummary"]["aggregatedTotalComments"]
            total_likes = post_social_actions_summary["likesSummary"]["totalLikes"]
            if total_comments == 0:
                continue
            post_comments = list(client.get_comments_on_post(post_urn))
            # post_likes = list(client.get_likes_on_post(post_urn))
            if len(post_comments) > 0:
                break

        post_with_likes_urn = 'urn:li:share:6367102219933806592'
        pass

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
