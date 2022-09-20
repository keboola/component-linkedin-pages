from functools import wraps
from typing import Callable, Dict, Iterator, List, ParamSpec

import requests
from urllib.parse import quote

from keboola.http_client.http import HttpClient, Cookie

from .models import URN, TimeIntervals, StandardizedDataType

BASE_URL = "https://api.linkedin.com"
API_VERSION = "v2"

DEFAULT_HTTP_HEADER = {"X-Restli-Protocol-Version": "2.0.0", "LinkedIn-Version": "202208"}

ENDPOINT_ORG = "organizations"
ENDPOINT_ORG_ACL = "organizationAcls"
ENDPOINT_ORG_PAGE_STATS = "organizationPageStatistics"
ENDPOINT_ORG_FOLLOWER_STATS = "organizationalEntityFollowerStatistics"
ENDPOINT_ORG_SHARE_STATS = "organizationalEntityShareStatistics"

ENDPOINT_POSTS = "posts"
ENDPOINT_SOCIAL_ACTIONS = "socialActions"
ENDPOINT_REACTIONS = "reactions"

# Enum endpoints:
ENDPOINT_DEGREES = "degrees"

# Other constants:
DEFAULT_PAGE_SIZE = 1000


def auth_header(access_token: str):
    return {'Authorization': 'Bearer ' + access_token}


def bool_to_param_string(val: bool):
    return "true" if val else "false"


class LinkedInClientException(Exception):
    pass


P = ParamSpec('P')


def response_error_handling(api_call: Callable[P, requests.Response]) -> Callable[P, dict]:
    """
    Function, that handles response handling of HTTP requests. The one from the library doesn't output all info.
    """
    @wraps(api_call)
    def wrapper(*args, **kwargs):
        try:
            r = api_call(*args, **kwargs)
            r.raise_for_status()
        except requests.HTTPError as e:
            response: requests.Response = e.response
            orig_message: str = e.args[0]
            if response.status_code in (400, 401, 402, 403):
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

    def _handle_pagination(self,
                           count: int,
                           start: int | None = None,
                           params: dict = None,
                           **kwargs) -> Iterator[Dict] | Dict:
        """
        If start is set (not None), returns the raw response JSON as Dict,
        otherwise returns iterator over all elements.
        """
        if params is None:
            params = dict()
        assert count > 0
        params["count"] = count
        if isinstance(start, int):
            assert start >= 0
            params["start"] = start
            return self.get(params=params, **kwargs)
        params["start"] = 0

        def generator():
            total_elements_downloaded = 0
            all_pages_handled = False
            while not all_pages_handled:
                next_page = self.get(params=params, **kwargs)
                elements: List[Dict] = next_page["elements"]
                paging_info: Dict[str, int | List[Dict[str, str]]] = next_page["paging"]
                total_elements = paging_info.get("total")
                yield from elements
                actual_page_size = len(elements)
                total_elements_downloaded += actual_page_size
                all_pages_handled = bool((total_elements and total_elements_downloaded >= total_elements) or
                                         (not total_elements and actual_page_size < count))
                params["start"] += count

        return generator()

    def get_administered_organization(self, organization_id: str | int | URN):
        if isinstance(organization_id, URN):
            assert organization_id.entity_type == "organization"
            organization_id = organization_id.id
        url = f"{ENDPOINT_ORG}/{organization_id}"
        return self.get(endpoint_path=url)

    def get_organization_by_vanity_name(self,
                                        vanity_name: str,
                                        start: int | None = None,
                                        count: int = DEFAULT_PAGE_SIZE):
        params = {"q": "vanityName", "vanityName": vanity_name}
        return self._handle_pagination(endpoint_path=ENDPOINT_ORG, params=params, count=count, start=start)

    def get_organization_acls(self, role: str | None = None, start: int | None = None, count: int = DEFAULT_PAGE_SIZE):
        params = {}
        if role:
            params["q"] = role
        return self._handle_pagination(endpoint_path=ENDPOINT_ORG_ACL, count=count, start=start, params=params)

    def get_organization_page_statistics(self,
                                         organization_urn: URN,
                                         time_intervals: TimeIntervals | None = None,
                                         start: int | None = None,
                                         count: int = DEFAULT_PAGE_SIZE):
        assert organization_urn.entity_type == "organization"
        params = {"q": "organization", "organization": str(organization_urn)}
        # Cannot do this commented out sensible thing:
        # if time_intervals is not None:
        #     params["timeIntervals"] = time_intervals.to_url_string()
        # I must do this to prevent URL encoding instead:
        if time_intervals:
            url = f"{ENDPOINT_ORG_PAGE_STATS}?timeIntervals={time_intervals.to_url_string()}"
        else:
            url = ENDPOINT_ORG_PAGE_STATS
        return self._handle_pagination(endpoint_path=url, count=count, start=start, params=params)

    def get_organization_follower_statistics(self,
                                             organization_urn: URN,
                                             time_intervals: TimeIntervals | None = None,
                                             start: int | None = None,
                                             count: int = DEFAULT_PAGE_SIZE):
        assert organization_urn.entity_type == "organization"
        params = {"q": "organizationalEntity", "organizationalEntity": str(organization_urn)}
        # Cannot do this commented out sensible thing:
        # if time_intervals is not None:
        #     params["timeIntervals"] = time_intervals.to_url_string()
        # I must do this to prevent URL encoding instead:
        if time_intervals:
            url = f"{ENDPOINT_ORG_FOLLOWER_STATS}?timeIntervals={time_intervals.to_url_string()}"
        else:
            url = ENDPOINT_ORG_FOLLOWER_STATS
        return self._handle_pagination(endpoint_path=url, count=count, start=start, params=params)

    def get_organization_share_statistics(self,
                                          organization_urn: URN,
                                          time_intervals: TimeIntervals | None = None,
                                          start: int | None = None,
                                          count: int = DEFAULT_PAGE_SIZE):
        assert organization_urn.entity_type == "organization"
        params = {"q": "organizationalEntity", "organizationalEntity": str(organization_urn)}
        # Cannot do this commented out sensible thing:
        # if time_intervals is not None:
        #     params["timeIntervals"] = time_intervals.to_url_string()
        # I must do this to prevent URL encoding instead:
        if time_intervals:
            url = f"{ENDPOINT_ORG_SHARE_STATS}?timeIntervals={time_intervals.to_url_string()}"
        else:
            url = ENDPOINT_ORG_SHARE_STATS
        return self._handle_pagination(endpoint_path=url, count=count, start=start, params=params)

    def get_post_by_urn(self, post_urn: URN):
        url = f"{ENDPOINT_POSTS}/{quote(str(post_urn))}"
        return self.get(endpoint_path=url)

    def get_posts_by_author(self,
                            author_urn: URN,
                            is_dsc: bool,
                            start: int | None = None,
                            count: int = DEFAULT_PAGE_SIZE):
        params = {"q": "author", "author": author_urn, "isDsc": bool_to_param_string(is_dsc)}
        return self._handle_pagination(endpoint_path=ENDPOINT_POSTS, count=count, start=start, params=params)

    def get_comments_on_post(self, post_urn: URN, start: int | None = None, count: int = DEFAULT_PAGE_SIZE):
        url = f"{ENDPOINT_SOCIAL_ACTIONS}/{quote(str(post_urn))}/comments"
        return self._handle_pagination(endpoint_path=url, count=count, start=start)

    def get_likes_on_post(self, post_urn: URN, start: int | None = None, count: int = DEFAULT_PAGE_SIZE):
        url = f"{ENDPOINT_SOCIAL_ACTIONS}/{quote(str(post_urn))}/likes"
        return self._handle_pagination(endpoint_path=url, count=count, start=start)

    def get_social_action_summary_on_post(self, post_urn: URN):
        url = f"{ENDPOINT_SOCIAL_ACTIONS}/{quote(str(post_urn))}"
        return self.get(endpoint_path=url)

    def get_all_standardized_data_type_enum_values(self,
                                                   standardized_data_type: StandardizedDataType,
                                                   start: int | None = None,
                                                   count: int = DEFAULT_PAGE_SIZE):
        url = ("skills?locale=(language:en,country:US)"
               if standardized_data_type is StandardizedDataType.SKILLS else standardized_data_type.value)
        return self._handle_pagination(endpoint_path=url, count=count, start=start)
