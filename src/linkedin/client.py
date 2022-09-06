from functools import wraps
from typing import Callable, ParamSpec

import requests
from urllib.parse import quote

from keboola.http_client.http import HttpClient, Cookie

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


def response_error_handling(api_call: Callable[P, requests.Response]) -> Callable[P, dict]:
    """Function, that handles response handling of HTTP requests. The one from the library doesn't output all info.
    """
    @wraps(api_call)
    def wrapper(*args, **kwargs):
        try:
            r = api_call(*args, **kwargs)
            r.raise_for_status()
        except requests.HTTPError as e:
            response: requests.Response = e.response
            orig_message: str = e.args[0]
            if response.status_code in (401, 402, 403):
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
        base_url = f"{BASE_URL}/{API_VERSION}"
        super().__init__(base_url, auth_header=auth_header(self.access_token), default_http_header=DEFAULT_HTTP_HEADER)

    @response_error_handling
    def get(self,
            endpoint_path: str | None = None,
            params: dict = None,
            headers: dict = None,
            is_absolute_path: bool = False,
            cookies: Cookie = None,
            ignore_auth: bool = False,
            **kwargs):
        return self.get_raw(endpoint_path,
                            params=params,
                            headers=headers,
                            cookies=cookies,
                            is_absolute_path=is_absolute_path,
                            ignore_auth=ignore_auth,
                            **kwargs)

    def _get_all_elements_from_paginated_responses(self,
                                                   count: int,
                                                   start: int | None = None,
                                                   params: dict = None,
                                                   **kwargs):
        if params is None:
            params = dict()
        assert count > 0
        params["count"] = count
        if isinstance(start, int):
            assert start >= 0
            params["start"] = start
            return self.get(params=params, **kwargs)
        params["start"] = 0
        all_elements = []
        all_pages_handled = False
        while not all_pages_handled:
            next_page = self.get(params=params, **kwargs)
            elements: list = next_page["elements"]
            all_elements.extend(elements)
            if len(elements) < count:
                all_pages_handled = True
            else:
                params["start"] += count

        return all_elements

    def get_administered_organization(self, organization_id: str | int):
        url = f"{ENDPOINT_ORG}/{organization_id}"
        return self.get(endpoint_path=url)

    def get_organization_acls(self, role: str | None = None, start: int | None = None, count: int | None = 10):
        params = {}
        if role:
            params["q"] = role
        return self._get_all_elements_from_paginated_responses(endpoint_path=ENDPOINT_ORG_ACL,
                                                               count=count,
                                                               start=start,
                                                               params=params)

    def get_organization_page_statistics(self,
                                         organization_id: str | int,
                                         start: int | None = None,
                                         count: int | None = 10):
        params = {"q": "organization", "organization": organization_urn(organization_id)}
        return self._get_all_elements_from_paginated_responses(endpoint_path=ENDPOINT_ORG_PAGE_STATS,
                                                               count=count,
                                                               start=start,
                                                               params=params)

    def get_organization_follower_statistics(self,
                                             organization_id: str | int,
                                             start: int | None = None,
                                             count: int | None = 10):
        params = {"q": "organizationalEntity", "organizationalEntity": organization_urn(organization_id)}
        return self._get_all_elements_from_paginated_responses(endpoint_path=ENDPOINT_ORG_FOLLOWER_STATS,
                                                               count=count,
                                                               start=start,
                                                               params=params)

    def get_posts_by_author(self,
                            author_urn: str,
                            is_dsc: bool = False,
                            start: int | None = None,
                            count: int | None = 10):
        params = {"q": "author", "author": author_urn, "isDsc": bool_param_string(is_dsc)}
        return self._get_all_elements_from_paginated_responses(endpoint_path=ENDPOINT_POSTS,
                                                               count=count,
                                                               start=start,
                                                               params=params)

    def get_comments_on_post(self, post_urn: str, start: int | None = None, count: int | None = 10):
        url = f"{ENDPOINT_SOCIAL_ACTIONS}/{quote(post_urn)}/comments"
        return self._get_all_elements_from_paginated_responses(endpoint_path=url, count=count, start=start)
