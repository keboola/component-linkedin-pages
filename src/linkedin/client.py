import functools
from typing import Callable, ParamSpec

import requests
from urllib.parse import quote

from keboola.http_client import HttpClient

BASE_URL = "https://api.linkedin.com"
API_VERSION = "v2"

DEFAULT_HTTP_HEADER = {"X-Restli-Protocol-Version": "2.0.0", "LinkedIn-Version": "202208"}

ENDPOINT_ORG = "organizations"
ENDPOINT_ORG_ACL = "organizationAcls"
ENDPOINT_ORG_PAGE_STATS = "organizationPageStatistics"
ENDPOINT_ORG_FOLLOWER_STATS = "organizationalEntityFollowerStatistics"

ENDPOINT_POSTS = "posts"
ENDPOINT_SOCIAL_ACTIONS = "socialActions"


def auth_header(access_token: str):
    return {'Authorization': 'Bearer ' + access_token}


def organization_urn(id: str | int):
    return f"urn:li:organization:{id}"


def bool_param_string(val: bool):
    return "true" if val else "false"


class LinkedInClientException(Exception):
    pass


P = ParamSpec('P')


def response_error_handling(func: Callable[P, requests.Response]) -> Callable[P, dict]:
    """Function, that handles response handling of HTTP requests.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            r = func(*args, **kwargs)
            r.raise_for_status()
        except requests.HTTPError as e:
            response: requests.Response = e.response
            orig_message: str = e.args[0]
            if response.status_code in (401, 403):
                raise LinkedInClientException(orig_message +
                                              (f"\nResponse content: {response.text}" if response.text else "")) from e
            else:
                raise
        else:
            return r.json()

    return wrapper


class LinkedInClient(HttpClient):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        base_url = "/".join([BASE_URL, API_VERSION])
        super().__init__(base_url, auth_header=auth_header(self.access_token), default_http_header=DEFAULT_HTTP_HEADER)

    @response_error_handling
    def get_administered_organization(self, organization_id: str | int):
        url = f"{ENDPOINT_ORG}/{organization_id}"
        return self.get_raw(endpoint_path=url)

    @response_error_handling
    def get_organization_acls(self, role: str | None = None):
        params = {}
        if role:
            params["q"] = role
        return self.get_raw(endpoint_path=ENDPOINT_ORG_ACL, params=params)

    @response_error_handling
    def get_organization_page_statistics(self, organization_id: str | int):
        params = {"q": "organization", "organization": organization_urn(organization_id)}
        return self.get_raw(endpoint_path=ENDPOINT_ORG_PAGE_STATS, params=params)

    @response_error_handling
    def get_organization_follower_statistics(self, organization_id: str | int):
        params = {"q": "organizationalEntity", "organizationalEntity": organization_urn(organization_id)}
        return self.get_raw(endpoint_path=ENDPOINT_ORG_FOLLOWER_STATS, params=params)

    @response_error_handling
    def get_posts_by_author(self, author_urn: str, is_dsc: bool = False):    # TODO: Handle pagination
        params = {"q": "author", "author": author_urn, "isDsc": bool_param_string(is_dsc)}
        return self.get_raw(endpoint_path=ENDPOINT_POSTS, params=params)

    @response_error_handling
    def get_comments_on_post(self, post_urn: str):
        url = f"{ENDPOINT_SOCIAL_ACTIONS}/{quote(post_urn)}/comments"
        return self.get_raw(endpoint_path=url)
